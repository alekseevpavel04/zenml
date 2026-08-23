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
"""Internal read boundary for hot and archived execution history."""

from typing import List, Optional, Protocol
from uuid import UUID

from zenml.models import (
    Page,
    PipelineRunDAG,
    PipelineRunFilter,
    PipelineRunResponse,
    PipelineSnapshotFilter,
    PipelineSnapshotResponse,
    StepRunFilter,
    StepRunResponse,
)


class ExecutionHistoryReader(Protocol):
    """Reads needed to hydrate execution history without exposing storage."""

    def get_run(
        self,
        run_id: UUID,
        hydrate: bool = True,
        include_full_metadata: bool = False,
        include_python_packages: bool = False,
    ) -> PipelineRunResponse:
        """Get a pipeline run.

        Args:
            run_id: ID of the pipeline run.
            hydrate: Whether to hydrate response metadata.
            include_full_metadata: Whether to include step metadata.
            include_python_packages: Whether to include Python packages.

        Returns:
            The pipeline run response.
        """
        ...

    def list_runs(
        self,
        runs_filter_model: PipelineRunFilter,
        hydrate: bool = False,
        include_full_metadata: bool = False,
    ) -> Page[PipelineRunResponse]:
        """List pipeline runs.

        Args:
            runs_filter_model: Run filters and pagination.
            hydrate: Whether to hydrate response metadata.
            include_full_metadata: Whether to include step metadata.

        Returns:
            A page of pipeline runs.
        """
        ...

    def get_snapshot(
        self,
        snapshot_id: UUID,
        hydrate: bool = True,
        step_configuration_filter: Optional[List[str]] = None,
        include_config_schema: Optional[bool] = None,
    ) -> PipelineSnapshotResponse:
        """Get a pipeline snapshot.

        Args:
            snapshot_id: ID of the pipeline snapshot.
            hydrate: Whether to hydrate response metadata.
            step_configuration_filter: Step configurations to include.
            include_config_schema: Whether to include configuration schemas.

        Returns:
            The pipeline snapshot response.
        """
        ...

    def list_snapshots(
        self,
        snapshot_filter_model: PipelineSnapshotFilter,
        hydrate: bool = False,
    ) -> Page[PipelineSnapshotResponse]:
        """List pipeline snapshots.

        Args:
            snapshot_filter_model: Snapshot filters and pagination.
            hydrate: Whether to hydrate response metadata.

        Returns:
            A page of pipeline snapshots.
        """
        ...

    def get_run_step(
        self, step_run_id: UUID, hydrate: bool = True
    ) -> StepRunResponse:
        """Get a run step.

        Args:
            step_run_id: ID of the run step.
            hydrate: Whether to hydrate response metadata.

        Returns:
            The run step response.
        """
        ...

    def list_run_steps(
        self,
        step_run_filter_model: StepRunFilter,
        hydrate: bool = False,
    ) -> Page[StepRunResponse]:
        """List run steps.

        Args:
            step_run_filter_model: Step filters and pagination.
            hydrate: Whether to hydrate response metadata.

        Returns:
            A page of run steps.
        """
        ...

    def get_pipeline_run_dag(
        self,
        pipeline_run_id: UUID,
        include_step_metadata: Optional[List[str]] = None,
    ) -> PipelineRunDAG:
        """Get a pipeline run DAG.

        Args:
            pipeline_run_id: ID of the pipeline run.
            include_step_metadata: Step metadata keys to include.

        Returns:
            The pipeline run DAG.
        """
        ...
