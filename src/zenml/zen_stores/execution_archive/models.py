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
"""Typed execution archive records."""

from datetime import datetime
from typing import Annotated, Dict, List, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PositiveInt,
    StringConstraints,
    model_validator,
)

from zenml.utils.enum_utils import StrEnum
from zenml.utils.time_utils import utc_now

Sha256Digest = Annotated[
    str,
    StringConstraints(pattern=r"^[0-9a-f]{64}$"),
]


class ExecutionArchiveState(StrEnum):
    """Internal states of an execution archive generation."""

    DISCOVERED = "discovered"
    EXPORTING = "exporting"
    EXPORTED = "exported"
    VERIFIED = "verified"
    COMMITTED = "committed"
    COMPACTING = "compacting"
    COLD = "cold"
    RESTORING = "restoring"
    RESTORED = "restored"
    HELD = "held"
    FAILED_RETRYABLE = "failed_retryable"
    CORRUPT = "corrupt"


class ExecutionArchiveObject(BaseModel):
    """Exact immutable object version referenced by an archive manifest."""

    key: str = Field(min_length=1, max_length=1024)
    version_id: str = Field(min_length=1, max_length=1024)
    sha256: Sha256Digest
    stored_bytes: NonNegativeInt

    model_config = ConfigDict(frozen=True)


class ExecutionArchiveManifest(BaseModel):
    """Versioned manifest for one archived root execution family."""

    schema_version: Literal[1] = 1
    archive_id: UUID
    workspace_id: UUID
    project_id: UUID
    root_run_id: UUID
    generation: PositiveInt
    writer_version: str = Field(min_length=1, max_length=64)
    writer_alembic_revision: str = Field(min_length=1, max_length=64)
    policy_version: NonNegativeInt
    source_fingerprint: Sha256Digest
    canonical_bytes: NonNegativeInt
    run_ids: List[UUID] = Field(min_length=1)
    step_run_ids: List[UUID] = Field(default_factory=list)
    snapshot_ids: List[UUID] = Field(default_factory=list)
    table_counts: Dict[str, NonNegativeInt] = Field(default_factory=dict)
    table_hashes: Dict[str, Sha256Digest] = Field(default_factory=dict)
    objects: List[ExecutionArchiveObject] = Field(min_length=1)
    kms_key_id: str = Field(min_length=1, max_length=2048)
    retention_until: datetime
    legal_hold: bool = False
    created_at: datetime = Field(default_factory=utc_now)

    model_config = ConfigDict(frozen=True)

    @model_validator(mode="after")
    def _validate_unique_references(self) -> "ExecutionArchiveManifest":
        """Reject ambiguous object and entity references.

        Returns:
            The validated manifest.

        Raises:
            ValueError: If an object key or entity ID is repeated.
        """
        object_keys = [item.key for item in self.objects]
        if len(object_keys) != len(set(object_keys)):
            raise ValueError("Archive object keys must be unique.")

        for name, identifiers in (
            ("run_ids", self.run_ids),
            ("step_run_ids", self.step_run_ids),
            ("snapshot_ids", self.snapshot_ids),
        ):
            if len(identifiers) != len(set(identifiers)):
                raise ValueError(f"Archive {name} must be unique.")

        return self
