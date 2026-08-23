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
"""Tests for execution archive policy validation."""

import pytest
from pydantic import ValidationError

from zenml.config.execution_archive import ExecutionArchivePolicy


def test_execution_archive_policy_defaults_and_retention_bounds() -> None:
    """The policy stays off by default and rejects unsafe retention windows."""
    policy = ExecutionArchivePolicy()

    assert policy.enabled is False
    assert policy.hot_retention_days == 180
    assert policy.policy_version == 0

    for retention_days in (89, 731):
        with pytest.raises(ValidationError):
            ExecutionArchivePolicy(hot_retention_days=retention_days)
