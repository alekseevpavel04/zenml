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
"""Typed payload moved by the execution archive."""

import gzip
import json
from typing import Dict, List, Literal, Optional, Sequence, Tuple
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from zenml.zen_stores.execution_archive.codec import sha256_digest
from zenml.zen_stores.execution_archive.models import Sha256Digest


class ArchivedPipelineRunPayload(BaseModel):
    """Large mutable columns retained for one pipeline run."""

    id: UUID
    orchestrator_environment: Optional[str] = None
    exception_info: Optional[str] = None
    pipeline_configuration: Optional[str] = None
    client_environment: Optional[str] = None

    model_config = ConfigDict(frozen=True)


class ArchivedPipelineSnapshotPayload(BaseModel):
    """Large immutable columns retained for one pipeline snapshot."""

    id: UUID
    pipeline_configuration: str
    client_environment: str
    pipeline_spec: Optional[str] = None
    source_code: Optional[str] = None

    model_config = ConfigDict(frozen=True)


class ArchivedStepRunPayload(BaseModel):
    """Large columns retained for one step run."""

    id: UUID
    source_code: Optional[str] = None
    docstring: Optional[str] = None
    exception_info: Optional[str] = None
    step_configuration: Optional[str] = None

    model_config = ConfigDict(frozen=True)


class ArchivedStepConfigurationPayload(BaseModel):
    """Exact configuration row retained for archive and restore."""

    id: UUID
    snapshot_id: Optional[UUID] = None
    step_run_id: Optional[UUID] = None
    index: int
    name: str
    config: str

    model_config = ConfigDict(frozen=True)

    @model_validator(mode="after")
    def _validate_single_owner(self) -> "ArchivedStepConfigurationPayload":
        """Require exactly one snapshot or step-run owner.

        Returns:
            The validated configuration payload.

        Raises:
            ValueError: If ownership is missing or ambiguous.
        """
        if (self.snapshot_id is None) == (self.step_run_id is None):
            raise ValueError(
                "An archived step configuration needs exactly one owner."
            )
        return self


class ExecutionArchivePayload(BaseModel):
    """Canonical payload for one root execution family."""

    schema_version: Literal[1] = 1
    root_run_id: UUID
    runs: List[ArchivedPipelineRunPayload] = Field(min_length=1)
    snapshots: List[ArchivedPipelineSnapshotPayload] = Field(
        default_factory=list
    )
    steps: List[ArchivedStepRunPayload] = Field(default_factory=list)
    step_configurations: List[ArchivedStepConfigurationPayload] = Field(
        default_factory=list
    )

    model_config = ConfigDict(frozen=True)

    @model_validator(mode="after")
    def _validate_references(self) -> "ExecutionArchivePayload":
        """Reject duplicate records and a missing root run.

        Returns:
            The validated payload.

        Raises:
            ValueError: If an entity ID is repeated or the root is absent.
        """
        for name, identifiers in (
            ("pipeline_run", [record.id for record in self.runs]),
            ("pipeline_snapshot", [record.id for record in self.snapshots]),
            ("step_run", [record.id for record in self.steps]),
            (
                "step_configuration",
                [record.id for record in self.step_configurations],
            ),
        ):
            if len(identifiers) != len(set(identifiers)):
                raise ValueError(f"Archive payload {name} IDs must be unique.")

        if self.root_run_id not in {run.id for run in self.runs}:
            raise ValueError("The archive payload must contain its root run.")
        return self

    def table_counts(self) -> Dict[str, int]:
        """Return the number of archived records by source table.

        Returns:
            Source-table record counts.
        """
        return {name: len(records) for name, records in self._tables()}

    def table_hashes(self) -> Dict[str, Sha256Digest]:
        """Return canonical record hashes by source table.

        Returns:
            Source-table SHA-256 hashes.
        """
        return {
            name: sha256_digest(_canonical_json(records))
            for name, records in self._tables()
        }

    def _tables(self) -> Tuple[Tuple[str, Sequence[BaseModel]], ...]:
        """Return archive records grouped by their source table.

        Returns:
            Ordered source-table records.
        """
        return (
            ("pipeline_run", tuple(self.runs)),
            ("pipeline_snapshot", tuple(self.snapshots)),
            ("step_run", tuple(self.steps)),
            ("step_configuration", tuple(self.step_configurations)),
        )


def _canonical_json(value: object) -> bytes:
    """Encode a JSON-compatible value deterministically.

    Args:
        value: Value or Pydantic models to encode.

    Returns:
        Canonical UTF-8 JSON bytes.
    """
    if isinstance(value, (list, tuple)):
        value = [
            item.model_dump(mode="json")
            if isinstance(item, BaseModel)
            else item
            for item in value
        ]
    elif isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def encode_payload(payload: ExecutionArchivePayload) -> bytes:
    """Encode an execution payload as canonical UTF-8 JSON.

    Args:
        payload: Payload to encode.

    Returns:
        Canonical payload bytes.
    """
    return _canonical_json(payload)


def decode_payload(payload: bytes) -> ExecutionArchivePayload:
    """Decode and validate canonical execution payload bytes.

    Args:
        payload: Canonical payload bytes.

    Returns:
        The validated execution payload.
    """
    return ExecutionArchivePayload.model_validate_json(payload)


def compress_payload(payload: bytes) -> bytes:
    """Compress canonical payload bytes deterministically.

    Args:
        payload: Canonical payload bytes.

    Returns:
        Deterministic gzip bytes.
    """
    return gzip.compress(payload, compresslevel=6, mtime=0)


def decompress_payload(payload: bytes) -> bytes:
    """Decompress archived payload bytes.

    Args:
        payload: Gzip-compressed payload bytes.

    Returns:
        Canonical payload bytes.
    """
    return gzip.decompress(payload)
