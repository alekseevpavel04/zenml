#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#  implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Copy-and-compare service for execution archive payloads."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, PositiveInt
from sqlalchemy.engine import Engine
from sqlmodel import Session, col, or_, select

from zenml.config.execution_archive import ExecutionArchivePolicy
from zenml.enums import ExecutionStatus
from zenml.utils.time_utils import to_utc_timezone, utc_now
from zenml.zen_stores.execution_archive.codec import (
    ChecksumMismatchError,
    decode_manifest,
    encode_manifest,
    sha256_digest,
)
from zenml.zen_stores.execution_archive.models import (
    ExecutionArchiveManifest,
    ExecutionArchiveObject,
    ExecutionArchiveState,
)
from zenml.zen_stores.execution_archive.payload import (
    ArchivedPipelineRunPayload,
    ArchivedPipelineSnapshotPayload,
    ArchivedStepConfigurationPayload,
    ArchivedStepRunPayload,
    ExecutionArchivePayload,
    compress_payload,
    decode_payload,
    decompress_payload,
    encode_payload,
)
from zenml.zen_stores.execution_archive.storage import (
    ExecutionArchiveObjectStore,
)
from zenml.zen_stores.schemas import (
    ExecutionArchiveSchema,
    PipelineRunSchema,
    PipelineSnapshotSchema,
    StepConfigurationSchema,
    StepRunSchema,
)


class ExecutionArchiveError(RuntimeError):
    """Base error for execution archive copy-and-compare operations."""


class ExecutionArchiveNotEligibleError(ExecutionArchiveError):
    """Raised when a root execution family is not safe to export."""


class ExecutionArchiveParityError(ExecutionArchiveError):
    """Raised when cold payload differs from the current SQL source."""


class ExecutionArchiveStateError(ExecutionArchiveError):
    """Raised when a catalog transition violates archive authority rules."""


class ExecutionArchiveRequest(BaseModel):
    """Explicit request to copy one root execution family."""

    workspace_id: UUID
    project_id: UUID
    root_run_id: UUID
    generation: PositiveInt
    policy: ExecutionArchivePolicy
    writer_version: str = Field(min_length=1, max_length=64)
    writer_alembic_revision: str = Field(min_length=1, max_length=64)

    model_config = ConfigDict(frozen=True)


@dataclass(frozen=True)
class ExecutionArchiveResult:
    """Verified outcome of one copy-and-compare operation."""

    archive_id: UUID
    manifest: ExecutionArchiveObject
    canonical_bytes: int
    stored_bytes: int


@dataclass(frozen=True)
class _Capture:
    payload: ExecutionArchivePayload
    latest_mutation: datetime
    completed: bool


@dataclass(frozen=True)
class _CatalogEntry:
    id: UUID
    created: datetime
    state: ExecutionArchiveState
    source_fingerprint: str
    bucket: Optional[str]
    manifest: Optional[ExecutionArchiveObject]


class ExecutionArchiveService:
    """Copies SQL payload to immutable storage and verifies exact parity."""

    def __init__(
        self,
        *,
        engine: Engine,
        object_store: ExecutionArchiveObjectStore,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        """Initialize the archive service.

        Args:
            engine: ZenML SQL store engine.
            object_store: Immutable archive object store.
            clock: Current-time provider.
        """
        self._engine = engine
        self._object_store = object_store
        self._clock = clock

    def export_and_compare(
        self, request: ExecutionArchiveRequest
    ) -> ExecutionArchiveResult:
        """Copy and verify one family without changing hot SQL payload.

        Args:
            request: Explicit root-family export request.

        Returns:
            Exact manifest identity and byte counts.

        Raises:
            Exception: If eligibility, SQL, storage, or parity validation
                fails. The original typed exception is preserved.
        """
        now = _as_naive_utc(self._clock())
        capture = self._capture(request)
        eligible_at = self._eligible_at(request, capture, now)
        canonical_payload = encode_payload(capture.payload)
        entry = self._begin_export(
            request=request,
            eligible_at=eligible_at,
            source_fingerprint=sha256_digest(canonical_payload),
        )

        try:
            if entry.state == ExecutionArchiveState.VERIFIED:
                return self._compare(request, entry)

            compressed_payload = compress_payload(canonical_payload)
            retention_until = to_utc_timezone(now) + timedelta(
                days=self._object_store.object_lock_days
            )
            payload_object = self._object_store.put_immutable(
                key=self._object_key(request, entry.id, "payload.json.gz"),
                payload=compressed_payload,
                retain_until=retention_until,
            )
            manifest = self._manifest(
                request=request,
                entry=entry,
                payload=capture.payload,
                payload_object=payload_object,
                canonical_bytes=len(canonical_payload),
                retention_until=retention_until,
            )
            manifest_payload = encode_manifest(manifest)
            manifest_object = self._object_store.put_immutable(
                key=self._object_key(request, entry.id, "manifest.json"),
                payload=manifest_payload,
                retain_until=retention_until,
            )
            entry = self._mark_exported(
                archive_id=entry.id,
                manifest=manifest_object,
                canonical_bytes=len(canonical_payload),
                stored_bytes=len(compressed_payload),
            )
            result = self._compare(request, entry)
            self._mark_verified(entry.id)
            return result
        except Exception as exc:
            state = (
                ExecutionArchiveState.CORRUPT
                if isinstance(exc, ChecksumMismatchError)
                else ExecutionArchiveState.FAILED_RETRYABLE
            )
            self._mark_failed(entry.id, state, str(exc))
            raise

    def _capture(self, request: ExecutionArchiveRequest) -> _Capture:
        """Read one root execution family in a short SQL transaction.

        Args:
            request: Project-scoped root-family request.

        Returns:
            Deterministic payload and eligibility data.

        Raises:
            ExecutionArchiveError: If the root or its referenced snapshots are
                missing from the project.
        """
        with Session(self._engine) as session:
            root = session.exec(
                select(PipelineRunSchema)
                .where(col(PipelineRunSchema.id) == request.root_run_id)
                .where(col(PipelineRunSchema.project_id) == request.project_id)
            ).one_or_none()
            if root is None:
                raise ExecutionArchiveError(
                    "The requested root run does not exist in this project."
                )
            if root.parent_run_id is not None or root.root_run_id is not None:
                raise ExecutionArchiveError(
                    "Execution archives require a root run ID."
                )

            runs = list(
                session.exec(
                    select(PipelineRunSchema)
                    .where(
                        col(PipelineRunSchema.project_id) == request.project_id
                    )
                    .where(
                        or_(
                            col(PipelineRunSchema.id) == request.root_run_id,
                            col(PipelineRunSchema.root_run_id)
                            == request.root_run_id,
                        )
                    )
                ).all()
            )
            run_ids = {run.id for run in runs}
            steps = list(
                session.exec(
                    select(StepRunSchema)
                    .where(col(StepRunSchema.project_id) == request.project_id)
                    .where(col(StepRunSchema.pipeline_run_id).in_(run_ids))
                ).all()
            )
            step_ids = {step.id for step in steps}
            snapshot_ids = {
                snapshot_id
                for snapshot_id in (
                    [run.snapshot_id for run in runs]
                    + [step.snapshot_id for step in steps]
                )
                if snapshot_id is not None
            }
            snapshots = (
                list(
                    session.exec(
                        select(PipelineSnapshotSchema)
                        .where(
                            col(PipelineSnapshotSchema.project_id)
                            == request.project_id
                        )
                        .where(
                            col(PipelineSnapshotSchema.id).in_(snapshot_ids)
                        )
                    ).all()
                )
                if snapshot_ids
                else []
            )
            if {snapshot.id for snapshot in snapshots} != snapshot_ids:
                raise ExecutionArchiveError(
                    "The execution family has an incomplete snapshot closure."
                )

            predicates = []
            if snapshot_ids:
                predicates.append(
                    col(StepConfigurationSchema.snapshot_id).in_(snapshot_ids)
                )
            if step_ids:
                predicates.append(
                    col(StepConfigurationSchema.step_run_id).in_(step_ids)
                )
            configurations = (
                list(
                    session.exec(
                        select(StepConfigurationSchema).where(or_(*predicates))
                    ).all()
                )
                if predicates
                else []
            )
            records = [*runs, *snapshots, *steps, *configurations]
            payload = ExecutionArchivePayload(
                root_run_id=request.root_run_id,
                runs=[
                    ArchivedPipelineRunPayload(
                        id=run.id,
                        orchestrator_environment=run.orchestrator_environment,
                        exception_info=run.exception_info,
                        pipeline_configuration=run.pipeline_configuration,
                        client_environment=run.client_environment,
                    )
                    for run in sorted(runs, key=lambda item: item.id.hex)
                ],
                snapshots=[
                    ArchivedPipelineSnapshotPayload(
                        id=snapshot.id,
                        pipeline_configuration=snapshot.pipeline_configuration,
                        client_environment=snapshot.client_environment,
                        pipeline_spec=snapshot.pipeline_spec,
                        source_code=snapshot.source_code,
                    )
                    for snapshot in sorted(
                        snapshots, key=lambda item: item.id.hex
                    )
                ],
                steps=[
                    ArchivedStepRunPayload(
                        id=step.id,
                        source_code=step.source_code,
                        docstring=step.docstring,
                        exception_info=step.exception_info,
                        step_configuration=step.step_configuration,
                    )
                    for step in sorted(steps, key=lambda item: item.id.hex)
                ],
                step_configurations=[
                    ArchivedStepConfigurationPayload(
                        id=config.id,
                        snapshot_id=config.snapshot_id,
                        step_run_id=config.step_run_id,
                        index=config.index,
                        name=config.name,
                        config=config.config,
                    )
                    for config in sorted(
                        configurations, key=lambda item: item.id.hex
                    )
                ],
            )
            return _Capture(
                payload=payload,
                latest_mutation=max(record.updated for record in records),
                completed=all(
                    run.status == ExecutionStatus.COMPLETED.value
                    for run in runs
                )
                and all(_is_finished(step.status) for step in steps),
            )

    def _begin_export(
        self,
        *,
        request: ExecutionArchiveRequest,
        eligible_at: datetime,
        source_fingerprint: str,
    ) -> _CatalogEntry:
        """Create or resume one immutable archive generation.

        Args:
            request: Explicit export request.
            eligible_at: Time the payload became eligible.
            source_fingerprint: Canonical SQL payload fingerprint.

        Returns:
            Detached catalog entry.
        """
        with Session(self._engine) as session:
            schema = session.exec(
                select(ExecutionArchiveSchema)
                .where(
                    col(ExecutionArchiveSchema.root_run_id)
                    == request.root_run_id
                )
                .where(
                    col(ExecutionArchiveSchema.generation)
                    == request.generation
                )
                .with_for_update()
            ).one_or_none()
            if schema is None:
                schema = ExecutionArchiveSchema(
                    project_id=request.project_id,
                    root_run_id=request.root_run_id,
                    generation=request.generation,
                    state=ExecutionArchiveState.EXPORTING.value,
                    policy_version=request.policy.policy_version,
                    eligible_at=eligible_at,
                    source_fingerprint=source_fingerprint,
                )
                session.add(schema)
            else:
                self._validate_retry(schema, request, source_fingerprint)
                if (
                    schema.state
                    == ExecutionArchiveState.FAILED_RETRYABLE.value
                ):
                    schema.state = ExecutionArchiveState.EXPORTING.value
                    schema.last_error = None
                    schema.updated = utc_now()
            session.commit()
            session.refresh(schema)
            return _entry(schema)

    def _mark_exported(
        self,
        *,
        archive_id: UUID,
        manifest: ExecutionArchiveObject,
        canonical_bytes: int,
        stored_bytes: int,
    ) -> _CatalogEntry:
        """Persist the exact manifest pointer after uploads complete.

        Args:
            archive_id: Archive catalog identifier.
            manifest: Exact manifest object version.
            canonical_bytes: Uncompressed payload bytes.
            stored_bytes: Compressed payload bytes.

        Returns:
            Updated detached catalog entry.

        Raises:
            ExecutionArchiveStateError: If the transition is invalid.
        """
        with Session(self._engine) as session:
            schema = _locked_schema(session, archive_id)
            if schema.state not in {
                ExecutionArchiveState.EXPORTING.value,
                ExecutionArchiveState.EXPORTED.value,
            }:
                raise ExecutionArchiveStateError(
                    f"Cannot record an export from state '{schema.state}'."
                )
            existing = _manifest_object(schema)
            if existing is not None and (
                existing != manifest
                or schema.bucket != self._object_store.bucket
            ):
                raise ExecutionArchiveStateError(
                    "An exported generation cannot change its manifest."
                )
            schema.state = ExecutionArchiveState.EXPORTED.value
            schema.bucket = self._object_store.bucket
            schema.manifest_key = manifest.key
            schema.manifest_version_id = manifest.version_id
            schema.manifest_sha256 = manifest.sha256
            schema.manifest_stored_bytes = manifest.stored_bytes
            schema.canonical_bytes = canonical_bytes
            schema.stored_bytes = stored_bytes
            schema.updated = utc_now()
            session.commit()
            session.refresh(schema)
            return _entry(schema)

    def _mark_verified(self, archive_id: UUID) -> None:
        """Mark one exported generation as verified.

        Args:
            archive_id: Archive catalog identifier.

        Raises:
            ExecutionArchiveStateError: If no exported manifest exists.
        """
        with Session(self._engine) as session:
            schema = _locked_schema(session, archive_id)
            if (
                schema.state
                not in {
                    ExecutionArchiveState.EXPORTED.value,
                    ExecutionArchiveState.VERIFIED.value,
                }
                or _manifest_object(schema) is None
            ):
                raise ExecutionArchiveStateError(
                    f"Cannot verify an archive from state '{schema.state}'."
                )
            schema.state = ExecutionArchiveState.VERIFIED.value
            schema.last_error = None
            schema.updated = utc_now()
            session.commit()

    def _mark_failed(
        self,
        archive_id: UUID,
        state: ExecutionArchiveState,
        error: str,
    ) -> None:
        """Record a pre-commit failure without changing hot payload.

        Args:
            archive_id: Archive catalog identifier.
            state: Retryable or corrupt failure state.
            error: Failure description.

        Raises:
            ExecutionArchiveStateError: If the archive is already authoritative.
        """
        with Session(self._engine) as session:
            schema = _locked_schema(session, archive_id)
            if schema.state in {
                ExecutionArchiveState.COMMITTED.value,
                ExecutionArchiveState.COMPACTING.value,
                ExecutionArchiveState.COLD.value,
            }:
                raise ExecutionArchiveStateError(
                    "The exporter cannot downgrade a committed archive."
                )
            schema.state = state.value
            schema.last_error = error
            schema.updated = utc_now()
            session.commit()

    def _compare(
        self, request: ExecutionArchiveRequest, entry: _CatalogEntry
    ) -> ExecutionArchiveResult:
        """Compare an exact archive version with a fresh SQL capture.

        Args:
            request: Original export request.
            entry: Catalog entry containing the manifest pointer.

        Returns:
            Verified archive result.

        Raises:
            ExecutionArchiveStateError: If the catalog pointer is unusable.
            ExecutionArchiveParityError: If cold and hot payloads differ.
        """
        if entry.manifest is None or entry.bucket != self._object_store.bucket:
            raise ExecutionArchiveStateError(
                "The archive catalog has no usable manifest pointer."
            )
        manifest = decode_manifest(
            self._object_store.get_exact(entry.manifest)
        )
        expected_identity = (
            entry.id,
            request.workspace_id,
            request.project_id,
            request.root_run_id,
        )
        actual_identity = (
            manifest.archive_id,
            manifest.workspace_id,
            manifest.project_id,
            manifest.root_run_id,
        )
        if actual_identity != expected_identity or len(manifest.objects) != 1:
            raise ExecutionArchiveParityError(
                "The archive manifest identity or payload count is invalid."
            )

        canonical = decompress_payload(
            self._object_store.get_exact(manifest.objects[0])
        )
        if (
            len(canonical) != manifest.canonical_bytes
            or sha256_digest(canonical) != manifest.source_fingerprint
        ):
            raise ExecutionArchiveParityError(
                "The archive payload does not match its manifest."
            )
        cold = decode_payload(canonical)
        if (
            cold.table_counts() != manifest.table_counts
            or cold.table_hashes() != manifest.table_hashes
        ):
            raise ExecutionArchiveParityError(
                "The archive table evidence does not match its payload."
            )
        if self._capture(request).payload != cold:
            raise ExecutionArchiveParityError(
                "The archive differs from authoritative SQL; use a new generation."
            )
        return ExecutionArchiveResult(
            archive_id=entry.id,
            manifest=entry.manifest,
            canonical_bytes=manifest.canonical_bytes,
            stored_bytes=manifest.objects[0].stored_bytes,
        )

    @staticmethod
    def _eligible_at(
        request: ExecutionArchiveRequest,
        capture: _Capture,
        now: datetime,
    ) -> datetime:
        """Validate policy, execution state, and payload age.

        Args:
            request: Explicit export request.
            capture: Current SQL capture.
            now: Current time.

        Returns:
            Exact eligibility time.

        Raises:
            ExecutionArchiveNotEligibleError: If any gate fails.
        """
        if not request.policy.enabled:
            raise ExecutionArchiveNotEligibleError(
                "Execution archival is disabled for this workspace."
            )
        if _as_naive_utc(request.policy.effective_at) > now:
            raise ExecutionArchiveNotEligibleError(
                "The execution archive policy is not effective yet."
            )
        if not capture.completed:
            raise ExecutionArchiveNotEligibleError(
                "Only completed execution families can be exported."
            )
        eligible_at = _as_naive_utc(capture.latest_mutation) + timedelta(
            days=request.policy.hot_retention_days
        )
        if eligible_at > now:
            raise ExecutionArchiveNotEligibleError(
                "The execution payload is still inside its hot retention window."
            )
        return eligible_at

    def _object_key(
        self,
        request: ExecutionArchiveRequest,
        archive_id: UUID,
        filename: str,
    ) -> str:
        """Build a workspace- and project-isolated object key.

        Args:
            request: Explicit export request.
            archive_id: Archive catalog identifier.
            filename: Object filename.

        Returns:
            Deterministic object key.
        """
        return "/".join(
            part
            for part in (
                self._object_store.key_prefix,
                "workspaces",
                str(request.workspace_id),
                "projects",
                str(request.project_id),
                "executions",
                str(request.root_run_id),
                str(archive_id),
                filename,
            )
            if part
        )

    def _manifest(
        self,
        *,
        request: ExecutionArchiveRequest,
        entry: _CatalogEntry,
        payload: ExecutionArchivePayload,
        payload_object: ExecutionArchiveObject,
        canonical_bytes: int,
        retention_until: datetime,
    ) -> ExecutionArchiveManifest:
        """Build the manifest uploaded after its payload object.

        Args:
            request: Explicit export request.
            entry: Archive catalog entry.
            payload: Canonical execution payload.
            payload_object: Exact payload object version.
            canonical_bytes: Uncompressed payload bytes.
            retention_until: Object Lock retention time.

        Returns:
            Version 1 execution archive manifest.
        """
        return ExecutionArchiveManifest(
            archive_id=entry.id,
            workspace_id=request.workspace_id,
            project_id=request.project_id,
            root_run_id=request.root_run_id,
            generation=request.generation,
            writer_version=request.writer_version,
            writer_alembic_revision=request.writer_alembic_revision,
            policy_version=request.policy.policy_version,
            source_fingerprint=entry.source_fingerprint,
            canonical_bytes=canonical_bytes,
            run_ids=[run.id for run in payload.runs],
            step_run_ids=[step.id for step in payload.steps],
            snapshot_ids=[snapshot.id for snapshot in payload.snapshots],
            table_counts=payload.table_counts(),
            table_hashes=payload.table_hashes(),
            objects=[payload_object],
            kms_key_id=self._object_store.kms_key_id,
            retention_until=retention_until,
            created_at=entry.created,
        )

    @staticmethod
    def _validate_retry(
        schema: ExecutionArchiveSchema,
        request: ExecutionArchiveRequest,
        source_fingerprint: str,
    ) -> None:
        """Validate immutable inputs before resuming one generation.

        Args:
            schema: Existing archive generation.
            request: Current export request.
            source_fingerprint: Current canonical payload fingerprint.

        Raises:
            ExecutionArchiveStateError: If retry inputs differ or state is final.
        """
        if (
            schema.project_id != request.project_id
            or schema.policy_version != request.policy.policy_version
            or schema.source_fingerprint != source_fingerprint
        ):
            raise ExecutionArchiveStateError(
                "Changed project, policy, or payload requires a new generation."
            )
        if schema.state not in {
            ExecutionArchiveState.EXPORTING.value,
            ExecutionArchiveState.EXPORTED.value,
            ExecutionArchiveState.VERIFIED.value,
            ExecutionArchiveState.FAILED_RETRYABLE.value,
        }:
            raise ExecutionArchiveStateError(
                f"Archive state '{schema.state}' cannot resume export."
            )


def _locked_schema(
    session: Session, archive_id: UUID
) -> ExecutionArchiveSchema:
    """Load one archive row with a write lock.

    Args:
        session: Active SQL session.
        archive_id: Archive catalog identifier.

    Returns:
        Locked archive schema.

    Raises:
        ExecutionArchiveStateError: If the catalog row does not exist.
    """
    schema = session.exec(
        select(ExecutionArchiveSchema)
        .where(col(ExecutionArchiveSchema.id) == archive_id)
        .with_for_update()
    ).one_or_none()
    if schema is None:
        raise ExecutionArchiveStateError(
            f"Execution archive '{archive_id}' does not exist."
        )
    return schema


def _manifest_object(
    schema: ExecutionArchiveSchema,
) -> Optional[ExecutionArchiveObject]:
    """Build an exact manifest reference from a catalog row.

    Args:
        schema: Archive catalog schema.

    Returns:
        Exact manifest object, if complete.

    Raises:
        ExecutionArchiveStateError: If the persisted pointer is partial.
    """
    values = (
        schema.manifest_key,
        schema.manifest_version_id,
        schema.manifest_sha256,
        schema.manifest_stored_bytes,
    )
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise ExecutionArchiveStateError(
            "The execution archive manifest pointer is incomplete."
        )
    assert schema.manifest_key is not None
    assert schema.manifest_version_id is not None
    assert schema.manifest_sha256 is not None
    assert schema.manifest_stored_bytes is not None
    return ExecutionArchiveObject(
        key=schema.manifest_key,
        version_id=schema.manifest_version_id,
        sha256=schema.manifest_sha256,
        stored_bytes=schema.manifest_stored_bytes,
    )


def _entry(schema: ExecutionArchiveSchema) -> _CatalogEntry:
    """Detach a catalog schema from its SQL session.

    Args:
        schema: Archive catalog schema.

    Returns:
        Detached catalog entry.
    """
    return _CatalogEntry(
        id=schema.id,
        created=schema.created,
        state=ExecutionArchiveState(schema.state),
        source_fingerprint=schema.source_fingerprint,
        bucket=schema.bucket,
        manifest=_manifest_object(schema),
    )


def _is_finished(status: str) -> bool:
    """Return whether a stored execution status is terminal.

    Args:
        status: Stored execution status.

    Returns:
        Whether the status is known and terminal.
    """
    try:
        return ExecutionStatus(status).is_finished
    except ValueError:
        return False


def _as_naive_utc(value: datetime) -> datetime:
    """Normalize a timestamp for comparison with SQL ``DATETIME`` values.

    Args:
        value: Timezone-aware or timezone-naive UTC timestamp.

    Returns:
        Timezone-naive UTC timestamp.
    """
    return to_utc_timezone(value).replace(tzinfo=None)
