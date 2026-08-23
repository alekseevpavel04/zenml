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
"""SQLModel schema for the execution archive catalog."""

from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import TEXT, BigInteger, Column, String, UniqueConstraint
from sqlmodel import Field

from zenml.zen_stores.execution_archive.models import ExecutionArchiveState
from zenml.zen_stores.schemas.base_schemas import BaseSchema
from zenml.zen_stores.schemas.schema_utils import build_index


class ExecutionArchiveSchema(BaseSchema, table=True):
    """Transactional authority record for one archive generation.

    Project and root-run identifiers intentionally remain logical identifiers
    rather than foreign keys. Archive tombstones and exact object pointers must
    survive deletion of their hot execution locators.
    """

    __tablename__ = "execution_archive"
    __table_args__ = (
        UniqueConstraint(
            "root_run_id",
            "generation",
            name="unique_execution_archive_root_generation",
        ),
        build_index(
            table_name=__tablename__,
            column_names=["project_id", "state", "eligible_at"],
        ),
    )

    project_id: UUID
    root_run_id: UUID
    generation: int
    state: str = Field(
        default=ExecutionArchiveState.DISCOVERED.value,
        sa_column=Column(String(32), nullable=False),
    )
    policy_version: int
    eligible_at: datetime
    source_fingerprint: str = Field(
        sa_column=Column(String(64), nullable=False)
    )
    bucket: Optional[str] = Field(
        default=None,
        sa_column=Column(String(63), nullable=True),
    )
    manifest_key: Optional[str] = Field(
        default=None,
        sa_column=Column(TEXT, nullable=True),
    )
    manifest_version_id: Optional[str] = Field(
        default=None,
        sa_column=Column(TEXT, nullable=True),
    )
    manifest_sha256: Optional[str] = Field(
        default=None,
        sa_column=Column(String(64), nullable=True),
    )
    canonical_bytes: Optional[int] = Field(
        default=None,
        sa_column=Column(BigInteger, nullable=True),
    )
    stored_bytes: Optional[int] = Field(
        default=None,
        sa_column=Column(BigInteger, nullable=True),
    )
    checkpoint: Optional[str] = Field(
        default=None,
        sa_column=Column(TEXT, nullable=True),
    )
    legal_hold: bool = Field(default=False)
    tombstoned_at: Optional[datetime] = Field(default=None, nullable=True)
    tombstoned_by: Optional[UUID] = Field(default=None, nullable=True)
    committed_at: Optional[datetime] = Field(default=None, nullable=True)
    compacted_at: Optional[datetime] = Field(default=None, nullable=True)
    last_error: Optional[str] = Field(
        default=None,
        sa_column=Column(TEXT, nullable=True),
    )
