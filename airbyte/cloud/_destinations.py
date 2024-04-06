# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Cloud destinations for Airbyte."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from airbyte._util import api_util
from airbyte.cloud import _destination_util as dest_util


if TYPE_CHECKING:
    from airbyte_api.models.shared import (
        DestinationBigquery,
        DestinationDuckdb,
        DestinationPostgres,
        DestinationResponse,
        DestinationSnowflake,
    )
    from sqlalchemy.engine import Engine

    from airbyte.caches.base import CacheBase
    from airbyte.cloud._workspaces import CloudWorkspace


@dataclass
class CloudDestination:
    """A cloud destination for Airbyte."""

    workspace: CloudWorkspace
    destination_id: str
    destination_type: str

    _destination_response: DestinationResponse | None = None
    _as_cache: CacheBase | None = None

    def _get_destination_response(self, *, force_refresh: bool = False) -> DestinationResponse:
        """Get the destination response."""
        if self._destination_response is None or force_refresh:
            self._destination_response = api_util.get_destination(
                destination_id=self.destination_id,
                api_root=self.workspace.api_root,
                api_key=self.workspace.api_key,
            )

        return self._destination_response

    def get_destination_config(
        self,
    ) -> DestinationBigquery | DestinationDuckdb | DestinationPostgres | DestinationSnowflake | Any:
        """Get the destination configuration."""
        return self._get_destination_response().configuration

    def as_cache(self) -> CacheBase:
        """Get the cache for the destination."""
        if self._as_cache is None:
            self._as_cache = dest_util.create_cache_from_destination_config(
                destination_configuration=self.get_destination_config(),
            )

        return self._as_cache

    def get_sql_engine(self) -> Engine:
        """Get the SQL engine for the destination."""
        return self.as_cache().get_sql_engine()
