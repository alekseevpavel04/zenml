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
"""Configuration for execution-history archival."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt

from zenml.utils.time_utils import utc_now

MIN_HOT_RETENTION_DAYS = 90
MAX_HOT_RETENTION_DAYS = 730
DEFAULT_HOT_RETENTION_DAYS = 180


class ExecutionArchivePolicy(BaseModel):
    """Immutable workspace policy snapshot used by archive workers."""

    enabled: bool = Field(
        default=False,
        title="Whether new execution archive work is enabled.",
    )
    hot_retention_days: int = Field(
        default=DEFAULT_HOT_RETENTION_DAYS,
        ge=MIN_HOT_RETENTION_DAYS,
        le=MAX_HOT_RETENTION_DAYS,
        title="Days execution payload remains authoritative in hot storage.",
    )
    policy_version: NonNegativeInt = Field(
        default=0,
        title="Monotonically increasing workspace policy version.",
    )
    effective_at: datetime = Field(
        default_factory=utc_now,
        title="Time from which this policy version is effective.",
    )

    model_config = ConfigDict(frozen=True)
