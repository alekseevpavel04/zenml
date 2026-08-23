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
"""Tests for execution archive foundation contracts."""

from datetime import timedelta
from uuid import uuid4

import pytest

from zenml.utils.time_utils import utc_now
from zenml.zen_stores.execution_archive import (
    ChecksumMismatchError,
    ExecutionArchiveManifest,
    ExecutionArchiveObject,
    decode_manifest,
    encode_manifest,
    sha256_digest,
    verify_sha256,
)
from zenml.zen_stores.schemas.execution_archive_schemas import (
    ExecutionArchiveSchema,
)


def _manifest() -> ExecutionArchiveManifest:
    """Create a minimal valid manifest for codec tests."""
    created_at = utc_now()
    run_id = uuid4()
    return ExecutionArchiveManifest(
        archive_id=uuid4(),
        workspace_id=uuid4(),
        project_id=uuid4(),
        root_run_id=run_id,
        generation=1,
        writer_version="0.96.3",
        writer_alembic_revision="1dfbad4fc7c1",
        policy_version=3,
        source_fingerprint="a" * 64,
        canonical_bytes=1024,
        run_ids=[run_id],
        step_run_ids=[uuid4()],
        snapshot_ids=[uuid4()],
        table_counts={"pipeline_run": 1},
        table_hashes={"pipeline_run": "b" * 64},
        objects=[
            ExecutionArchiveObject(
                key="executions/bundle.json.gz",
                version_id="object-version",
                sha256="c" * 64,
                stored_bytes=512,
            )
        ],
        kms_key_id="kms-key",
        retention_until=created_at + timedelta(days=30),
        created_at=created_at,
    )


def test_manifest_codec_is_deterministic_and_fails_closed() -> None:
    """Manifest bytes round-trip exactly and corruption is rejected."""
    manifest = _manifest()
    encoded = encode_manifest(manifest)
    digest = sha256_digest(encoded)

    assert encode_manifest(manifest) == encoded
    assert decode_manifest(encoded) == manifest
    verify_sha256(encoded, digest)

    with pytest.raises(ChecksumMismatchError):
        verify_sha256(encoded + b"corrupt", digest)


def test_catalog_keeps_generation_and_worker_query_constraints() -> None:
    """The catalog enforces one generation and supports state polling."""
    table = ExecutionArchiveSchema.__table__

    assert {constraint.name for constraint in table.constraints} >= {
        "unique_execution_archive_root_generation"
    }
    assert {index.name for index in table.indexes} >= {
        "ix_execution_archive_project_id_state_eligible_at"
    }
    assert not table.c.root_run_id.foreign_keys
