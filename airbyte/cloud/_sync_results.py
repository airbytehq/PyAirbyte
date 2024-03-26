# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Sync results for Airbyte Cloud workspaces."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from airbyte_api.models.shared import JobStatusEnum

from airbyte._util import api_util
from airbyte.exceptions import HostedConnectionSyncError, HostedConnectionSyncTimeoutError


DEFAULT_SYNC_TIMEOUT_SECONDS = 30 * 60  # 30 minutes


if TYPE_CHECKING:
    from airbyte.cloud._workspaces import CloudWorkspace


FINAL_STATUSES = {
    JobStatusEnum.SUCCEEDED,
    JobStatusEnum.FAILED,
    JobStatusEnum.CANCELLED,
}
FAILED_STATUSES = {
    JobStatusEnum.FAILED,
    JobStatusEnum.CANCELLED,
}


@dataclass
class SyncResult:
    """The result of a sync operation."""

    workspace: CloudWorkspace
    connection_id: str
    job_id: str
    _latest_status: JobStatusEnum | None = None

    def is_job_complete(self) -> bool:
        """Check if the sync job is complete."""
        return self.get_job_status() in FINAL_STATUSES

    def get_job_status(self) -> JobStatusEnum:
        """Check if the sync job is still running."""
        if self._latest_status and self._latest_status in FINAL_STATUSES:
            return self._latest_status

        job_info = api_util.get_job_info(
            job_id=self.job_id,
            api_root=self.workspace.api_root,
            api_key=self.workspace.api_key,
        )
        self._latest_status = job_info.status

        return job_info.status

    def raise_failure_status(
        self,
        *,
        refresh_status: bool = False,
    ) -> None:
        """Raise an exception if the sync job failed.

        By default, this method will use the latest status available. If you want to refresh the status
        before checking for failure, set `refresh_status=True`. If the job has failed, this method will
        raise a `HostedConnectionSyncError`.

        Otherwise, do nothing.
        """
        latest_status = self._latest_status
        if refresh_status:
            latest_status = self.get_job_status()

        if latest_status in FAILED_STATUSES:
            raise HostedConnectionSyncError(
                workspace=self.workspace,
                connection_id=self.connection_id,
                job_id=self.job_id,
                job_status=self._latest_status,
            )

    def wait_for_completion(
        self,
        *,
        wait_timeout: int = DEFAULT_SYNC_TIMEOUT_SECONDS,
        raise_timeout: bool = True,
        raise_failure: bool = False,
    ) -> JobStatusEnum:
        """Wait for a job to finish running."""
        start_time = time.time()
        while True:
            latest_status = self.get_job_status()
            if latest_status in FINAL_STATUSES:
                if raise_failure:
                    # No-op if the job succeeded or is still running:
                    self.raise_failure_status()

                return latest_status

            if time.time() - start_time > wait_timeout:
                if raise_timeout:
                    raise HostedConnectionSyncTimeoutError(
                        workspace=self.workspace,
                        connection_id=self.connection_id,
                        job_id=self.job_id,
                        job_status=latest_status,
                        timeout=wait_timeout,
                    )

                return latest_status  # This will be a non-final status

            time.sleep(api_util.JOB_WAIT_INTERVAL_SECS)
