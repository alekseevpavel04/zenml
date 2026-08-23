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
"""Canonical manifest encoding and checksum verification."""

import hashlib
import hmac
import json

from zenml.zen_stores.execution_archive.models import (
    ExecutionArchiveManifest,
    Sha256Digest,
)


class ChecksumMismatchError(ValueError):
    """Raised when immutable archive content fails checksum verification."""


def encode_manifest(manifest: ExecutionArchiveManifest) -> bytes:
    """Encode a manifest as canonical UTF-8 JSON.

    Args:
        manifest: Manifest to encode.

    Returns:
        Deterministically encoded manifest bytes.
    """
    payload = manifest.model_dump(mode="json")
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def decode_manifest(payload: bytes) -> ExecutionArchiveManifest:
    """Decode and validate canonical manifest bytes.

    Args:
        payload: UTF-8 JSON manifest bytes.

    Returns:
        The validated manifest.
    """
    return ExecutionArchiveManifest.model_validate_json(payload)


def sha256_digest(payload: bytes) -> str:
    """Calculate the lowercase SHA-256 digest of a payload.

    Args:
        payload: Bytes to hash.

    Returns:
        Lowercase hexadecimal SHA-256 digest.
    """
    return hashlib.sha256(payload).hexdigest()


def verify_sha256(payload: bytes, expected: Sha256Digest) -> None:
    """Verify payload bytes against an expected SHA-256 digest.

    Args:
        payload: Bytes to verify.
        expected: Expected lowercase hexadecimal SHA-256 digest.

    Raises:
        ChecksumMismatchError: If the payload does not match the digest.
    """
    actual = sha256_digest(payload)
    if not hmac.compare_digest(actual, expected):
        raise ChecksumMismatchError(
            f"Archive checksum mismatch: expected {expected}, got {actual}."
        )
