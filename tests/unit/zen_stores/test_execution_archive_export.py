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
"""End-to-end tests for execution archive copy-and-compare."""

import base64
import hashlib
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Callable, Dict, Iterator, Optional, Tuple
from uuid import UUID, uuid4

import pytest
from sqlmodel import Session, select

from zenml.config.execution_archive import (
    ExecutionArchivePolicy,
    ExecutionArchiveTarget,
)
from zenml.enums import ExecutionStatus
from zenml.models import ProjectFilter, UserFilter
from zenml.zen_stores.execution_archive import (
    ExecutionArchiveObject,
    ExecutionArchiveState,
    sha256_digest,
    verify_sha256,
)
from zenml.zen_stores.execution_archive.service import (
    ExecutionArchiveParityError,
    ExecutionArchiveRequest,
    ExecutionArchiveService,
)
from zenml.zen_stores.execution_archive.storage import (
    S3ExecutionArchiveObjectStore,
)
from zenml.zen_stores.schemas import (
    ExecutionArchiveSchema,
    PipelineRunSchema,
    PipelineSchema,
    PipelineSnapshotSchema,
    StepConfigurationSchema,
    StepRunSchema,
)
from zenml.zen_stores.sql_zen_store import (
    SqlZenStore,
    SqlZenStoreConfiguration,
)


class _MemoryArchiveStore:
    """Immutable in-memory object store for service tests."""

    bucket = "archive-bucket"
    key_prefix = "execution-archive"
    kms_key_id = "kms-key"
    object_lock_days = 365

    def __init__(
        self, after_manifest: Optional[Callable[[], None]] = None
    ) -> None:
        """Initialize the store.

        Args:
            after_manifest: Callback after the first manifest write.
        """
        self.objects: Dict[str, Tuple[ExecutionArchiveObject, bytes]] = {}
        self._after_manifest = after_manifest

    def put_immutable(
        self, *, key: str, payload: bytes, retain_until: datetime
    ) -> ExecutionArchiveObject:
        """Create or verify one immutable object.

        Args:
            key: Object key.
            payload: Object bytes.
            retain_until: Required retention time.

        Returns:
            Exact object reference.
        """
        del retain_until
        existing = self.objects.get(key)
        if existing is not None:
            verify_sha256(payload, existing[0].sha256)
            assert payload == existing[1]
            return existing[0]

        object_ = ExecutionArchiveObject(
            key=key,
            version_id="version-1",
            sha256=sha256_digest(payload),
            stored_bytes=len(payload),
        )
        self.objects[key] = (object_, payload)
        if key.endswith("manifest.json") and self._after_manifest is not None:
            callback, self._after_manifest = self._after_manifest, None
            callback()
        return object_

    def get_exact(self, object_: ExecutionArchiveObject) -> bytes:
        """Read one exact object version.

        Args:
            object_: Exact object reference.

        Returns:
            Stored object bytes.
        """
        stored_object, payload = self.objects[object_.key]
        assert stored_object == object_
        verify_sha256(payload, object_.sha256)
        return payload


class _FakeS3Client:
    """Small boto3-compatible client for S3 contract testing."""

    def __init__(self) -> None:
        self.payload = b""
        self.put_request: Dict[str, object] = {}
        self.retain_until: Optional[datetime] = None

    def put_object(self, **kwargs: object) -> Dict[str, str]:
        """Record one put request.

        Args:
            **kwargs: Boto3 PutObject arguments.

        Returns:
            Minimal PutObject response.
        """
        self.put_request = kwargs
        self.payload = kwargs["Body"]  # type: ignore[assignment]
        self.retain_until = kwargs["ObjectLockRetainUntilDate"]  # type: ignore[assignment]
        return {"VersionId": "version-1"}

    def head_object(self, **kwargs: object) -> Dict[str, object]:
        """Return protection metadata for the stored object.

        Args:
            **kwargs: Boto3 HeadObject arguments.

        Returns:
            Version, checksum, encryption, and retention metadata.
        """
        del kwargs
        return {
            "VersionId": "version-1",
            "ChecksumSHA256": base64.b64encode(
                hashlib.sha256(self.payload).digest()
            ).decode(),
            "ContentLength": len(self.payload),
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": "kms-key",
            "ObjectLockMode": "GOVERNANCE",
            "ObjectLockRetainUntilDate": self.retain_until,
        }

    def get_object(self, **kwargs: object) -> Dict[str, object]:
        """Return one exact stored object.

        Args:
            **kwargs: Boto3 GetObject arguments.

        Returns:
            Exact version and streaming body.
        """
        assert kwargs["VersionId"] == "version-1"
        return {"VersionId": "version-1", "Body": BytesIO(self.payload)}


@pytest.fixture
def sql_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[SqlZenStore]:
    """Create a fresh SQLite-backed ZenML store.

    Args:
        tmp_path: Temporary test directory.
        monkeypatch: Pytest environment patcher.

    Yields:
        A fresh SQL Zen store.
    """
    db_dir = tmp_path / "zenml-cfg"
    db_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("ZENML_CONFIG_PATH", str(db_dir))
    config = SqlZenStoreConfiguration(url=f"sqlite:///{db_dir / 'test.db'}")
    yield SqlZenStore(config=config, skip_default_registrations=False)


def _populate_execution_family(
    store: SqlZenStore, now: datetime
) -> Tuple[UUID, UUID, UUID]:
    """Populate one old completed execution family.

    Args:
        store: Test SQL store.
        now: Current test time.

    Returns:
        Project, root-run, and snapshot identifiers.
    """
    project_id = store.list_projects(ProjectFilter()).items[0].id
    user_id = store.list_users(UserFilter()).items[0].id
    old = now - timedelta(days=200)
    pipeline = PipelineSchema(
        name="archive-pipeline",
        project_id=project_id,
        user_id=user_id,
        run_count=1,
        created=old,
        updated=old,
    )
    snapshot = PipelineSnapshotSchema(
        project_id=project_id,
        user_id=user_id,
        pipeline_id=pipeline.id,
        name=None,
        description=None,
        is_dynamic=False,
        pipeline_configuration='{"name":"archive-pipeline"}',
        client_environment='{"python":"3.11"}',
        run_name_template="archive-{date}",
        client_version="0.96.3",
        server_version="0.96.3",
        pipeline_spec='{"steps":["train"]}',
        source_code='print("pipeline")',
        step_count=1,
        created=old,
        updated=old,
    )
    run = PipelineRunSchema(
        project_id=project_id,
        user_id=user_id,
        pipeline_id=pipeline.id,
        snapshot_id=snapshot.id,
        name="archive-run",
        orchestrator_run_id=None,
        start_time=old,
        end_time=old + timedelta(minutes=1),
        in_progress=False,
        status=ExecutionStatus.COMPLETED.value,
        orchestrator_environment='{"worker":"test"}',
        exception_info=None,
        index=1,
        enable_heartbeat=False,
        created=old,
        updated=old,
    )
    step = StepRunSchema(
        project_id=project_id,
        user_id=user_id,
        pipeline_run_id=run.id,
        snapshot_id=snapshot.id,
        name="train",
        start_time=old,
        end_time=old + timedelta(seconds=30),
        status=ExecutionStatus.COMPLETED.value,
        source_code='print("step")',
        docstring="Train the model.",
        version=1,
        is_retriable=False,
        created=old,
        updated=old,
    )
    configuration = StepConfigurationSchema(
        snapshot_id=snapshot.id,
        step_run_id=None,
        index=0,
        name="train",
        config='{"spec":{"source":"train"}}',
        created=old,
        updated=old,
    )
    run_id = run.id
    snapshot_id = snapshot.id
    with Session(store.engine) as session:
        session.add(pipeline)
        session.add(snapshot)
        session.add(run)
        session.add(step)
        session.add(configuration)
        session.commit()
    return project_id, run_id, snapshot_id


def _request(
    *, project_id: UUID, root_run_id: UUID, now: datetime
) -> ExecutionArchiveRequest:
    """Build an enabled export request.

    Args:
        project_id: Owning project identifier.
        root_run_id: Root pipeline run identifier.
        now: Current test time.

    Returns:
        Explicit copy-and-compare request.
    """
    return ExecutionArchiveRequest(
        workspace_id=uuid4(),
        project_id=project_id,
        root_run_id=root_run_id,
        generation=1,
        policy=ExecutionArchivePolicy(
            enabled=True,
            hot_retention_days=180,
            policy_version=1,
            effective_at=now.replace(tzinfo=timezone.utc) - timedelta(days=1),
        ),
        writer_version="0.96.3",
        writer_alembic_revision="1dfbad4fc7c1",
    )


def test_copy_and_compare_is_idempotent_and_keeps_hot_sql(
    sql_store: SqlZenStore,
) -> None:
    """A verified retry reuses two immutable objects and changes no payload.

    Args:
        sql_store: Fresh SQL Zen store.
    """
    now = datetime(2026, 8, 23, 12, 0, 0)
    project_id, run_id, snapshot_id = _populate_execution_family(
        sql_store, now
    )
    object_store = _MemoryArchiveStore()
    service = ExecutionArchiveService(
        engine=sql_store.engine,
        object_store=object_store,
        clock=lambda: now,
    )
    request = _request(project_id=project_id, root_run_id=run_id, now=now)

    first = service.export_and_compare(request)
    second = service.export_and_compare(request)

    assert first.archive_id == second.archive_id
    assert len(object_store.objects) == 2
    with Session(sql_store.engine) as session:
        snapshot = session.get(PipelineSnapshotSchema, snapshot_id)
        archive = session.exec(select(ExecutionArchiveSchema)).one()
        assert snapshot is not None
        assert snapshot.pipeline_spec == '{"steps":["train"]}'
        assert archive.state == ExecutionArchiveState.VERIFIED.value
        assert archive.compacted_at is None


def test_concurrent_payload_change_fails_parity_without_compaction(
    sql_store: SqlZenStore,
) -> None:
    """A source change after upload fails closed and preserves the hot value.

    Args:
        sql_store: Fresh SQL Zen store.
    """
    now = datetime(2026, 8, 23, 12, 0, 0)
    project_id, run_id, snapshot_id = _populate_execution_family(
        sql_store, now
    )

    def mutate_source() -> None:
        with Session(sql_store.engine) as session:
            snapshot = session.get(PipelineSnapshotSchema, snapshot_id)
            assert snapshot is not None
            snapshot.pipeline_spec = '{"steps":["train","evaluate"]}'
            snapshot.updated = now
            session.add(snapshot)
            session.commit()

    service = ExecutionArchiveService(
        engine=sql_store.engine,
        object_store=_MemoryArchiveStore(after_manifest=mutate_source),
        clock=lambda: now,
    )

    with pytest.raises(ExecutionArchiveParityError):
        service.export_and_compare(
            _request(project_id=project_id, root_run_id=run_id, now=now)
        )

    with Session(sql_store.engine) as session:
        snapshot = session.get(PipelineSnapshotSchema, snapshot_id)
        archive = session.exec(select(ExecutionArchiveSchema)).one()
        assert snapshot is not None
        assert snapshot.pipeline_spec == '{"steps":["train","evaluate"]}'
        assert archive.state == ExecutionArchiveState.FAILED_RETRYABLE.value
        assert archive.compacted_at is None


def test_s3_store_enforces_immutable_versioned_protection() -> None:
    """The S3 adapter writes once and reads the pinned protected version."""
    client = _FakeS3Client()
    target = ExecutionArchiveTarget(
        bucket="archive-bucket",
        key_prefix="archives",
        kms_key_id="kms-key",
        object_lock_days=30,
    )
    store = S3ExecutionArchiveObjectStore(client=client, target=target)
    retain_until = datetime(2026, 9, 22, 12, 0, 0)

    object_ = store.put_immutable(
        key="archives/object.json",
        payload=b"archive",
        retain_until=retain_until,
    )

    assert object_.version_id == "version-1"
    assert store.get_exact(object_) == b"archive"
    assert client.put_request["IfNoneMatch"] == "*"
    assert client.put_request["ServerSideEncryption"] == "aws:kms"
    assert client.put_request["ObjectLockMode"] == "GOVERNANCE"
    assert client.retain_until is not None
    assert client.retain_until.tzinfo is timezone.utc
