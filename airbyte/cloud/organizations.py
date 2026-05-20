# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte classes and methods for Airbyte Cloud organizations."""

from __future__ import annotations

import logging
from typing import Any

from airbyte._util import api_util
from airbyte.cloud._credentials import _AirbyteCredentials
from airbyte.secrets.base import SecretString


logger = logging.getLogger(__name__)


class CloudOrganization:
    """Information about an organization in Airbyte Cloud.

    This class provides lazy loading of organization attributes including billing status.
    It is typically created via `CloudWorkspace.get_organization()`.
    """

    def __init__(
        self,
        organization_id: str,
        organization_name: str | None = None,
        email: str | None = None,
        *,
        client_id: str | SecretString | None = None,
        client_secret: str | SecretString | None = None,
        bearer_token: str | SecretString | None = None,
        public_api_root: str | None = None,
        config_api_root: str | None = None,
    ) -> None:
        """Initialize a `CloudOrganization`."""
        self.organization_id = organization_id
        """The organization ID."""

        self._organization_name = organization_name
        """Display name of the organization."""

        self._email = email
        """Email associated with the organization."""

        self._credentials = _AirbyteCredentials(
            client_id=SecretString(client_id) if client_id else None,
            client_secret=SecretString(client_secret) if client_secret else None,
            bearer_token=SecretString(bearer_token) if bearer_token else None,
            public_api_root=public_api_root or api_util.CLOUD_API_ROOT,
            config_api_root=config_api_root,
            organization_id=organization_id,
        )
        self._organization_info: dict[str, Any] | None = None
        self._organization_info_fetch_failed: bool = False

    def _fetch_organization_info(self, *, force_refresh: bool = False) -> dict[str, Any]:
        """Fetch and cache organization info including billing status."""
        if force_refresh:
            self._organization_info_fetch_failed = False

        if self._organization_info_fetch_failed and self._organization_info is None:
            return {}

        if not force_refresh and self._organization_info is not None:
            return self._organization_info

        try:
            self._organization_info = api_util.get_organization_info(
                organization_id=self.organization_id,
                api_root=self._credentials.public_api_root,
                config_api_root=self._credentials.config_api_root,
                client_id=self._credentials.client_id,
                client_secret=self._credentials.client_secret,
                bearer_token=self._credentials.bearer_token,
            )
        except Exception as ex:
            logger.debug("Failed to fetch organization info.", exc_info=ex)
            if self._organization_info is None:
                self._organization_info_fetch_failed = True
            return self._organization_info or {}
        else:
            return self._organization_info

    @property
    def organization_name(self) -> str | None:
        """Display name of the organization."""
        if self._organization_name is not None:
            return self._organization_name
        info = self._fetch_organization_info()
        return info.get("organizationName")

    @property
    def email(self) -> str | None:
        """Email associated with the organization."""
        if self._email is not None:
            return self._email
        info = self._fetch_organization_info()
        return info.get("email")

    @property
    def payment_status(self) -> str | None:
        """Payment status of the organization."""
        info = self._fetch_organization_info()
        return (info.get("billing") or {}).get("paymentStatus")

    @property
    def subscription_status(self) -> str | None:
        """Subscription status of the organization."""
        info = self._fetch_organization_info()
        return (info.get("billing") or {}).get("subscriptionStatus")

    @property
    def is_account_locked(self) -> bool:
        """Whether the account is locked due to billing issues."""
        return api_util.is_account_locked(self.payment_status, self.subscription_status)
