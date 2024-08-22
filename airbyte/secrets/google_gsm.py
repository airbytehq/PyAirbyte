# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Secret manager that retrieves secrets from Google Secrets Manager (GSM).

Usage Example:

```python
import ab
from airbyte import secrets

# Initialize the Google GSM secret manager
gsm = secrets.GoogleGSMSecretManager(
    project="my-project",
    credentials_json=ab.get_secret("GCP_GSM_CREDENTIALS"),
)
# Get secrets with the label 'connector=source-github'
secrets = gsm.fetch_connector_secrets("source-github")

# Get the first secret and parse the JSON value
config: dict = next(secrets, None).parse_json()

# Pass the config to your source
source = ab.get_source(
    "source-github",
    config=config,
    streams="*",
)
read_result = source.read()
```

"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

from google.cloud import secretmanager_v1 as secretmanager

from airbyte import exceptions as exc
from airbyte.secrets.base import SecretHandle, SecretSourceEnum, SecretString
from airbyte.secrets.custom import CustomSecretManager


if TYPE_CHECKING:
    from collections.abc import Iterable, MutableMapping

    from google.cloud.secretmanager_v1.services.secret_manager_service.pagers import (
        ListSecretsPager,
    )


class GSMSecretHandle(SecretHandle):
    """A handle for a secret stored in Google Secrets Manager (GSM).

    This class inherits from `SecretHandle` and adds a `labels` attribute for inspecting GSM
    labels.
    """

    parent: GoogleGSMSecretManager

    def _get_gsm_secret_object(self) -> secretmanager.Secret:
        """Get the `Secret` object from GSM."""
        return self.parent.secret_client.get_secret(
            name=self.secret_name,
        )

    @property
    def labels(self) -> MutableMapping[str, str]:
        """Get the labels of the secret."""
        return self._get_gsm_secret_object().labels


class GoogleGSMSecretManager(CustomSecretManager):
    """Secret manager that retrieves secrets from Google Secrets Manager (GSM).

    This class inherits from `CustomSecretManager` and also adds methods
    that are specific to this implementation: `fetch_secrets()`,
    `fetch_secrets_by_label()` and `fetch_connector_secrets()`.

    This secret manager is not enabled by default. To use it, you must provide the project ID and
    the credentials for a service account with the necessary permissions to access the secrets.

    The `fetch_connector_secret()` method assumes a label name of `connector`
    matches the name of the connector (`source-github`, `destination-snowflake`, etc.)
    """

    name = SecretSourceEnum.GOOGLE_GSM.value
    auto_register = False
    as_backup = False
    replace_existing = False

    CONNECTOR_LABEL = "connector"
    """The label key used to filter secrets by connector name."""

    def __init__(
        self,
        project: str,
        *,
        credentials_path: str | None = None,
        credentials_json: str | SecretString | None = None,
        auto_register: bool = False,
        as_backup: bool = False,
    ) -> None:
        """Instantiate a new Google GSM secret manager instance.

        You can provide either the path to the credentials file or the JSON contents of the
        credentials file. If both are provided, a `PyAirbyteInputError` will be raised.
        """
        if credentials_path and credentials_json:
            raise exc.PyAirbyteInputError(
                guidance=("You can provide `credentials_path` or `credentials_json` but not both."),
            )

        self.project = project

        if credentials_json is not None and not isinstance(credentials_json, SecretString):
            credentials_json = SecretString(credentials_json)

        if not credentials_json and not credentials_path:
            if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
                credentials_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

            elif "GCP_GSM_CREDENTIALS" in os.environ:
                credentials_json = SecretString(os.environ["GCP_GSM_CREDENTIALS"])

        if credentials_path:
            credentials_json = SecretString(Path(credentials_path).read_text(encoding="utf-8"))

        if not credentials_json:
            raise exc.PyAirbyteInputError(
                guidance=(
                    "No Google Cloud credentials found. You can provide the path to the "
                    "credentials file using the `credentials_path` argument, or provide the JSON "
                    "contents of the credentials file using the `credentials_json` argument."
                ),
            )

        self.secret_client = secretmanager.SecretManagerServiceClient.from_service_account_info(
            json.loads(credentials_json)
        )

        if auto_register:
            self.auto_register = auto_register

        if as_backup:
            self.as_backup = as_backup

        super().__init__()  # Handles the registration if needed

    def _fully_qualified_secret_name(self, secret_name: str) -> str:
        """Get the fully qualified secret name."""
        full_name = secret_name
        if "projects/" not in full_name:
            # This is not yet fully qualified
            full_name = f"projects/{self.project}/secrets/{secret_name}/versions/latest"

        if "/versions/" not in full_name:
            full_name += "/versions/latest"

        return full_name

    def get_secret(self, secret_name: str) -> SecretString | None:
        """Get a named secret from Google Colab user secrets."""
        return SecretString(
            self.secret_client.access_secret_version(
                name=self._fully_qualified_secret_name(secret_name)
            ).payload.data.decode("UTF-8")
        )

    def get_secret_handle(
        self,
        secret_name: str,
    ) -> GSMSecretHandle:
        """Fetch secret in the secret manager, using the secret name.

        Unlike `get_secret`, this method returns a `GSMSecretHandle` object, which can be used to
        inspect the secret's labels and other metadata.

        Args:
            secret_name (str): The name of the connector to filter by.

        Returns:
            GSMSecretHandle: A handle for the matching secret.
        """
        return GSMSecretHandle(
            parent=self,
            secret_name=self._fully_qualified_secret_name(secret_name),
        )

    def fetch_secrets(
        self,
        *,
        filter_string: str,
    ) -> Iterable[GSMSecretHandle]:
        """List all available secrets in the secret manager.

        Example filter strings:
        - `labels.connector=source-bigquery`: Filter for secrets with the labe 'source-bigquery'.

        Args:
            filter_string (str): A filter string to apply to the list of secrets, following the
                format described in the Google Secret Manager documentation:
                https://cloud.google.com/secret-manager/docs/filtering

        Returns:
            Iterable[GSMSecretHandle]: An iterable of `GSMSecretHandle` objects for the matching
            secrets.
        """
        gsm_secrets: ListSecretsPager = self.secret_client.list_secrets(
            request=secretmanager.ListSecretsRequest(
                filter=filter_string,
                parent=f"projects/{self.project}",
            ),
        )

        return [
            GSMSecretHandle(
                parent=self,
                secret_name=secret.name,
            )
            for secret in gsm_secrets
        ]

    def fetch_secrets_by_label(
        self,
        label_key: str,
        label_value: str,
    ) -> Iterable[GSMSecretHandle]:
        """List all available secrets in the secret manager.

        Args:
            label_key (str): The key of the label to filter by.
            label_value (str): The value of the label to filter by.

        Returns:
            Iterable[GSMSecretHandle]: An iterable of `GSMSecretHandle` objects for the matching
            secrets.
        """
        return self.fetch_secrets(filter_string=f"labels.{label_key}={label_value}")

    def fetch_connector_secrets(
        self,
        connector_name: str,
    ) -> Iterable[GSMSecretHandle]:
        """Fetch secrets in the secret manager, using the connector name as a filter for the label.

        The label key used to filter the secrets is defined by the `CONNECTOR_LABEL` attribute,
        which defaults to 'connector'.

        Args:
            connector_name (str): The name of the connector to filter by.

        Returns:
            Iterable[GSMSecretHandle]: An iterable of `GSMSecretHandle` objects for the matching
            secrets.
        """
        return self.fetch_secrets_by_label(
            label_key=self.CONNECTOR_LABEL,
            label_value=connector_name,
        )

    def fetch_connector_secret(
        self,
        connector_name: str,
    ) -> GSMSecretHandle:
        """Fetch secret in the secret manager, using the connector name as a filter for the label.

        This method is a convenience method that returns the first secret found for the connector.

        The label key used to filter the secrets is defined by the `CONNECTOR_LABEL` attribute,
        which defaults to 'connector'.

        Args:
            connector_name (str): The name of the connector to filter by.

        Returns:
            GSMSecretHandle: A handle for the matching secret.
        """
        results: Iterable[GSMSecretHandle] = self.fetch_connector_secrets(connector_name)
        try:
            result = next(iter(results))
        except StopIteration:
            raise exc.PyAirbyteError(
                message="No secrets found for connector.",
                guidance=(
                    "Please check that the connector name is correct "
                    "and that the secret is correctly labeled."
                ),
                context={
                    "project": self.project,
                    "connector_name": connector_name,
                    "label_key": self.CONNECTOR_LABEL,
                },
            ) from None

        return result
