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
"""Immutable object storage used by execution archives."""

import base64
import hashlib
from datetime import datetime
from typing import Any, Protocol, cast
from urllib.parse import urlencode

from zenml.config.execution_archive import ExecutionArchiveTarget
from zenml.utils.time_utils import to_utc_timezone
from zenml.zen_stores.execution_archive.codec import (
    ChecksumMismatchError,
    sha256_digest,
    verify_sha256,
)
from zenml.zen_stores.execution_archive.models import ExecutionArchiveObject


class ExecutionArchiveStorageError(RuntimeError):
    """Raised when immutable archive storage violates its contract."""


class ExecutionArchiveObjectStore(Protocol):
    """Minimal immutable object-store contract required by the exporter."""

    @property
    def bucket(self) -> str:
        """Return the archive bucket name."""
        ...

    @property
    def key_prefix(self) -> str:
        """Return the archive object key prefix."""
        ...

    @property
    def kms_key_id(self) -> str:
        """Return the archive KMS key identifier."""
        ...

    @property
    def object_lock_days(self) -> int:
        """Return the minimum Object Lock retention duration."""
        ...

    def put_immutable(
        self,
        *,
        key: str,
        payload: bytes,
        retain_until: datetime,
    ) -> ExecutionArchiveObject:
        """Create or verify one immutable object.

        Args:
            key: Full object key.
            payload: Exact bytes to store.
            retain_until: Minimum Object Lock retention time.

        Returns:
            The exact immutable object version.
        """
        ...

    def get_exact(self, object_: ExecutionArchiveObject) -> bytes:
        """Read and verify one exact object version.

        Args:
            object_: Exact immutable object reference.

        Returns:
            Verified object bytes.
        """
        ...


class S3ExecutionArchiveObjectStore:
    """Version-pinned, KMS-encrypted S3 execution archive storage."""

    def __init__(self, client: Any, target: ExecutionArchiveTarget) -> None:
        """Initialize the S3 object store.

        Args:
            client: Boto3-compatible S3 client using workspace credentials.
            target: Immutable archive destination.
        """
        self._client = client
        self._target = target

    @property
    def bucket(self) -> str:
        """Return the archive bucket name.

        Returns:
            The configured S3 bucket.
        """
        return self._target.bucket

    @property
    def key_prefix(self) -> str:
        """Return the configured object key prefix.

        Returns:
            The normalized object key prefix.
        """
        return self._target.key_prefix

    @property
    def kms_key_id(self) -> str:
        """Return the configured archive KMS key identifier.

        Returns:
            The KMS key identifier.
        """
        return self._target.kms_key_id

    @property
    def object_lock_days(self) -> int:
        """Return the configured Object Lock duration.

        Returns:
            The retention duration in days.
        """
        return self._target.object_lock_days

    def put_immutable(
        self,
        *,
        key: str,
        payload: bytes,
        retain_until: datetime,
    ) -> ExecutionArchiveObject:
        """Create or verify an immutable, version-pinned S3 object.

        Args:
            key: Full object key.
            payload: Exact bytes to store.
            retain_until: Minimum Object Lock retention time.

        Returns:
            The exact immutable S3 object version.

        Raises:
            ExecutionArchiveStorageError: If S3 does not provide the required
                versioning, encryption, checksum, or retention guarantees.
            Exception: If the S3 client fails for a reason other than an
                existing immutable object.
        """
        retain_until = to_utc_timezone(retain_until)
        digest = sha256_digest(payload)
        checksum = base64.b64encode(hashlib.sha256(payload).digest()).decode()
        version_id = None
        try:
            response = self._client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=payload,
                ChecksumSHA256=checksum,
                ServerSideEncryption="aws:kms",
                SSEKMSKeyId=self._target.kms_key_id,
                ObjectLockMode="GOVERNANCE",
                ObjectLockRetainUntilDate=retain_until,
                IfNoneMatch="*",
                Tagging=urlencode({"purpose": "execution-archive"}),
            )
            version_id = response.get("VersionId")
        except Exception as exc:
            if not _is_precondition_failure(exc):
                raise

        head_request = {
            "Bucket": self.bucket,
            "Key": key,
            "ChecksumMode": "ENABLED",
        }
        if version_id:
            head_request["VersionId"] = version_id
        metadata = self._client.head_object(
            **head_request,
        )
        version_id = metadata.get("VersionId")
        if not version_id or version_id == "null":
            raise ExecutionArchiveStorageError(
                "The execution archive bucket must have versioning enabled."
            )
        self._verify_metadata(
            metadata=metadata,
            checksum=checksum,
            stored_bytes=len(payload),
            retain_until=retain_until,
        )
        return ExecutionArchiveObject(
            key=key,
            version_id=version_id,
            sha256=digest,
            stored_bytes=len(payload),
        )

    def get_exact(self, object_: ExecutionArchiveObject) -> bytes:
        """Read and verify an exact S3 object version.

        Args:
            object_: Exact immutable object reference.

        Returns:
            Verified object bytes.

        Raises:
            ChecksumMismatchError: If S3 returns different content.
            ExecutionArchiveStorageError: If S3 returns a different version.
        """
        response = self._client.get_object(
            Bucket=self.bucket,
            Key=object_.key,
            VersionId=object_.version_id,
            ChecksumMode="ENABLED",
        )
        if response.get("VersionId") != object_.version_id:
            raise ExecutionArchiveStorageError(
                "S3 returned a different execution archive object version."
            )

        body = response["Body"]
        try:
            payload = cast(bytes, body.read())
        finally:
            body.close()

        verify_sha256(payload, object_.sha256)
        if len(payload) != object_.stored_bytes:
            raise ChecksumMismatchError(
                "Archive object size does not match its manifest."
            )
        return payload

    def _verify_metadata(
        self,
        *,
        metadata: Any,
        checksum: str,
        stored_bytes: int,
        retain_until: datetime,
    ) -> None:
        """Verify S3 identity and protection metadata.

        Args:
            metadata: Boto3 ``head_object`` response.
            checksum: Expected AWS SHA-256 checksum.
            stored_bytes: Expected object length.
            retain_until: Minimum Object Lock retention time.

        Raises:
            ExecutionArchiveStorageError: If any protection is missing.
        """
        if metadata.get("ChecksumSHA256") != checksum:
            raise ExecutionArchiveStorageError(
                "S3 did not preserve the execution archive checksum."
            )
        if metadata.get("ContentLength") != stored_bytes:
            raise ExecutionArchiveStorageError(
                "S3 execution archive object length does not match."
            )
        if metadata.get("ServerSideEncryption") != "aws:kms":
            raise ExecutionArchiveStorageError(
                "Execution archive objects must use SSE-KMS."
            )
        if metadata.get("SSEKMSKeyId") != self._target.kms_key_id:
            raise ExecutionArchiveStorageError(
                "S3 used an unexpected execution archive KMS key."
            )
        if metadata.get("ObjectLockMode") not in {"GOVERNANCE", "COMPLIANCE"}:
            raise ExecutionArchiveStorageError(
                "Execution archive objects must be retention-protected."
            )
        actual_retention = metadata.get("ObjectLockRetainUntilDate")
        if actual_retention is None or actual_retention < retain_until:
            raise ExecutionArchiveStorageError(
                "Execution archive Object Lock retention is too short."
            )


def _is_precondition_failure(exc: Exception) -> bool:
    """Return whether an S3 exception means the key already exists.

    Args:
        exc: Boto3-compatible client exception.

    Returns:
        Whether the exception represents an HTTP 412 precondition failure.
    """
    response = getattr(exc, "response", {})
    error = response.get("Error", {})
    metadata = response.get("ResponseMetadata", {})
    return (
        error.get("Code") in {"PreconditionFailed", "412"}
        or metadata.get("HTTPStatusCode") == 412
    )
