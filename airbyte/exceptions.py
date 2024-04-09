# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""All exceptions used in the PyAirbyte.

This design is modeled after structlog's exceptions, in that we bias towards auto-generated
property prints rather than sentence-like string concatenation.

E.g. Instead of this:

> `Subprocess failed with exit code '1'`

We do this:

> `Subprocess failed. (exit_code=1)`

The benefit of this approach is that we can easily support structured logging, and we can
easily add new properties to exceptions without having to update all the places where they
are raised. We can also support any arbitrary number of properties in exceptions, without spending
time on building sentence-like string constructions with optional inputs.


In addition, the following principles are applied for exception class design:

- All exceptions inherit from a common base class.
- All exceptions have a message attribute.
- The first line of the docstring is used as the default message.
- The default message can be overridden by explicitly setting the message attribute.
- Exceptions may optionally have a guidance attribute.
- Exceptions may optionally have a help_url attribute.
- Rendering is automatically handled by the base class.
- Any helpful context not defined by the exception class can be passed in the `context` dict arg.
- Within reason, avoid sending PII to the exception constructor.
- Exceptions are dataclasses, so they can be instantiated with keyword arguments.
- Use the 'from' syntax to chain exceptions when it is helpful to do so.
  E.g. `raise AirbyteConnectorNotFoundError(...) from FileNotFoundError(connector_path)`
- Any exception that adds a new property should also be decorated as `@dataclass`.
"""
from __future__ import annotations

from dataclasses import dataclass
from textwrap import indent
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from airbyte._util.api_duck_types import AirbyteApiResponseDuckType
    from airbyte.cloud.workspaces import CloudWorkspace


NEW_ISSUE_URL = "https://github.com/airbytehq/airbyte/issues/new/choose"
DOCS_URL = "https://docs.airbyte.io/"


# Base error class


@dataclass
class PyAirbyteError(Exception):
    """Base class for exceptions in Airbyte."""

    guidance: str | None = None
    help_url: str | None = None
    log_text: str | list[str] | None = None
    context: dict[str, Any] | None = None
    message: str | None = None

    def get_message(self) -> str:
        """Return the best description for the exception.

        We resolve the following in order:
        1. The message sent to the exception constructor (if provided).
        2. The first line of the class's docstring.
        """
        if self.message:
            return self.message

        return self.__doc__.split("\n")[0] if self.__doc__ else ""

    def __str__(self) -> str:
        special_properties = ["message", "guidance", "help_url", "log_text", "context"]
        display_properties = {
            k: v
            for k, v in self.__dict__.items()
            if k not in special_properties and not k.startswith("_") and v is not None
        }
        display_properties.update(self.context or {})
        context_str = "\n    ".join(
            f"{str(k).replace('_', ' ').title()}: {v!r}" for k, v in display_properties.items()
        )
        exception_str = f"{self.__class__.__name__}: {self.get_message()}\n"
        if context_str:
            exception_str += "    " + context_str

        if self.log_text:
            if isinstance(self.log_text, list):
                self.log_text = "\n".join(self.log_text)

            exception_str += f"\nLog output: \n    {indent(self.log_text, '    ')}"

        if self.guidance:
            exception_str += f"\nSuggestion: {self.guidance}"

        if self.help_url:
            exception_str += f"\nMore info: {self.help_url}"

        return exception_str

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        properties_str = ", ".join(
            f"{k}={v!r}" for k, v in self.__dict__.items() if not k.startswith("_")
        )
        return f"{class_name}({properties_str})"

    def safe_logging_dict(self) -> dict[str, Any]:
        """Return a dictionary of the exception's properties which is safe for logging.

        We avoid any properties which could potentially contain PII.
        """
        result = {
            # The class name is safe to log:
            "class": self.__class__.__name__,
            # We discourage interpolated strings in 'message' so that this should never contain PII:
            "message": self.get_message(),
        }
        safe_attrs = ["connector_name", "stream_name", "violation", "exit_code"]
        for attr in safe_attrs:
            if hasattr(self, attr):
                result[attr] = getattr(self, attr)

        return result


# PyAirbyte Internal Errors (these are probably bugs)


@dataclass
class PyAirbyteInternalError(PyAirbyteError):
    """An internal error occurred in PyAirbyte."""

    guidance = "Please consider reporting this error to the Airbyte team."
    help_url = NEW_ISSUE_URL


# PyAirbyte Input Errors (replaces ValueError for user input)


@dataclass
class PyAirbyteInputError(PyAirbyteError, ValueError):
    """The input provided to PyAirbyte did not match expected validation rules.

    This inherits from ValueError so that it can be used as a drop-in replacement for
    ValueError in the PyAirbyte API.
    """

    # TODO: Consider adding a help_url that links to the auto-generated API reference.

    guidance = "Please check the provided value and try again."
    input_value: str | None = None


@dataclass
class PyAirbyteNoStreamsSelectedError(PyAirbyteInputError):
    """No streams were selected for the source."""

    guidance = (
        "Please call `select_streams()` to select at least one stream from the list provided. "
        "You can also call `select_all_streams()` to select all available streams for this source."
    )
    connector_name: str | None = None
    available_streams: list[str] | None = None


# PyAirbyte Cache Errors


class PyAirbyteCacheError(PyAirbyteError):
    """Error occurred while accessing the cache."""


@dataclass
class PyAirbyteCacheTableValidationError(PyAirbyteCacheError):
    """Cache table validation failed."""

    violation: str | None = None


@dataclass
class AirbyteConnectorConfigurationMissingError(PyAirbyteCacheError):
    """Connector is missing configuration."""

    connector_name: str | None = None


# Subprocess Errors


@dataclass
class AirbyteSubprocessError(PyAirbyteError):
    """Error when running subprocess."""

    run_args: list[str] | None = None


@dataclass
class AirbyteSubprocessFailedError(AirbyteSubprocessError):
    """Subprocess failed."""

    exit_code: int | None = None


# Connector Registry Errors


class AirbyteConnectorRegistryError(PyAirbyteError):
    """Error when accessing the connector registry."""


@dataclass
class AirbyteConnectorNotRegisteredError(AirbyteConnectorRegistryError):
    """Connector not found in registry."""

    connector_name: str | None = None
    guidance = "Please double check the connector name."


@dataclass
class AirbyteConnectorNotPyPiPublishedError(AirbyteConnectorRegistryError):
    """Connector found, but not published to PyPI."""

    connector_name: str | None = None
    guidance = "This likely means that the connector is not ready for use with PyAirbyte."


# Connector Errors


@dataclass
class AirbyteConnectorError(PyAirbyteError):
    """Error when running the connector."""

    connector_name: str | None = None


class AirbyteConnectorExecutableNotFoundError(AirbyteConnectorError):
    """Connector executable not found."""


class AirbyteConnectorInstallationError(AirbyteConnectorError):
    """Error when installing the connector."""


class AirbyteConnectorReadError(AirbyteConnectorError):
    """Error when reading from the connector."""


class AirbyteNoDataFromConnectorError(AirbyteConnectorError):
    """No data was provided from the connector."""


class AirbyteConnectorMissingCatalogError(AirbyteConnectorError):
    """Connector did not return a catalog."""


class AirbyteConnectorMissingSpecError(AirbyteConnectorError):
    """Connector did not return a spec."""


class AirbyteConnectorValidationFailedError(AirbyteConnectorError):
    """Connector config validation failed."""

    guidance = (
        "Please double-check your config and review the validation errors for more information."
    )


class AirbyteConnectorCheckFailedError(AirbyteConnectorError):
    """Connector check failed."""

    guidance = (
        "Please double-check your config or review the connector's logs for more information."
    )


@dataclass
class AirbyteConnectorFailedError(AirbyteConnectorError):
    """Connector failed."""

    exit_code: int | None = None


@dataclass
class AirbyteStreamNotFoundError(AirbyteConnectorError):
    """Connector stream not found."""

    stream_name: str | None = None
    available_streams: list[str] | None = None


@dataclass
class PyAirbyteSecretNotFoundError(PyAirbyteError):
    """Secret not found."""

    guidance = "Please ensure that the secret is set."
    help_url = (
        "https://docs.airbyte.com/using-airbyte/airbyte-lib/getting-started#secrets-management"
    )

    secret_name: str | None = None
    sources: list[str] | None = None


# Airbyte API Errors


@dataclass
class AirbyteError(PyAirbyteError):
    """An error occurred while communicating with the hosted Airbyte instance."""

    response: AirbyteApiResponseDuckType | None = None
    """The API response from the failed request."""

    workspace: CloudWorkspace | None = None
    """The workspace where the error occurred."""

    @property
    def workspace_url(self) -> str | None:
        if self.workspace:
            return self.workspace.workspace_url

        return None


@dataclass
class AirbyteConnectionError(AirbyteError):
    """An connection error occurred while communicating with the hosted Airbyte instance."""

    connection_id: str | None = None
    """The connection ID where the error occurred."""

    job_id: str | None = None
    """The job ID where the error occurred (if applicable)."""

    job_status: str | None = None
    """The latest status of the job where the error occurred (if applicable)."""

    @property
    def connection_url(self) -> str | None:
        if self.workspace_url and self.connection_id:
            return f"{self.workspace_url}/connections/{self.connection_id}"

        return None

    @property
    def job_history_url(self) -> str | None:
        if self.connection_url:
            return f"{self.connection_url}/job-history"

        return None

    @property
    def job_url(self) -> str | None:
        if self.job_history_url and self.job_id:
            return f"{self.job_history_url}#{self.job_id}::0"

        return None


@dataclass
class AirbyteConnectionSyncError(AirbyteConnectionError):
    """An error occurred while executing the remote Airbyte job."""


@dataclass
class AirbyteConnectionSyncTimeoutError(AirbyteConnectionSyncError):
    """An timeout occurred while waiting for the remote Airbyte job to complete."""

    timeout: int | None = None
    """The timeout in seconds that was reached."""


# Airbyte Resource Errors (General)


@dataclass
class AirbyteMissingResourceError(AirbyteError):
    """Remote Airbyte resources does not exist."""

    resource_type: str | None = None
    resource_name_or_id: str | None = None


@dataclass
class AirbyteMultipleResourcesError(AirbyteError):
    """Could not locate the resource because multiple matching resources were found."""

    resource_type: str | None = None
    resource_name_or_id: str | None = None
