# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Sync results for Airbyte Cloud workspaces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from airbyte_api.models.shared import JobStatusEnum, JobTypeEnum

from airbyte._util import api_util


if TYPE_CHECKING:
    from airbyte.cloud._workspaces import CloudWorkspace


FINAL_STATUSES = {
    JobStatusEnum.SUCCEEDED,
    JobStatusEnum.FAILED,
    JobStatusEnum.CANCELLED,
}


@dataclass
class SyncResult:
    """The result of a sync operation."""

    workspace: CloudWorkspace
    connection_id: str
    job_id: str
    _final_status: JobStatusEnum | None = None

    def is_job_complete(self) -> bool:
        """Check if the sync job is complete."""
        return self.get_job_status() in FINAL_STATUSES

    def get_job_status(self) -> JobStatusEnum:
        """Check if the sync job is still running."""
        if self._final_status:
            return self._final_status

        job_info = api_util.get_job_info(
            job_id=self.job_id,
            api_root=self.workspace.api_root,
            api_key=self.workspace.api_key,
        )
        if job_info.status in FINAL_STATUSES:
            self._final_status = job_info.status

        return job_info.status
