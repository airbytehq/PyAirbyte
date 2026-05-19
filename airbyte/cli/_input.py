# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Input helpers for CLI commands."""

from __future__ import annotations

import functools
import inspect
import json
import types
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Annotated,
    ParamSpec,
    TypeVar,
    cast,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)

import typing_extensions
import yaml
from cyclopts import Parameter

from airbyte.exceptions import PyAirbyteInputError


if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


WorkspaceIdArg = Annotated[
    str | None,
    Parameter(
        name="--workspace-id",
        env_var=["AIRBYTE_WORKSPACE_ID", "AIRBYTE_CLOUD_WORKSPACE_ID"],
        help="The workspace ID.",
    ),
]
ClientIdArg = Annotated[
    str | None,
    Parameter(env_var=["AIRBYTE_CLIENT_ID", "AIRBYTE_CLOUD_CLIENT_ID"], help="Airbyte client ID."),
]
ClientSecretArg = Annotated[
    str | None,
    Parameter(
        env_var=["AIRBYTE_CLIENT_SECRET", "AIRBYTE_CLOUD_CLIENT_SECRET"],
        help="Airbyte client secret.",
    ),
]
ConfigApiRootArg = Annotated[
    str | None,
    Parameter(
        name="--config-api-root",
        env_var=["AIRBYTE_CONFIG_API_ROOT", "AIRBYTE_CLOUD_CONFIG_API_URL"],
        help="Airbyte Config API root URL for self-managed instances.",
    ),
]
ApiUrlArg = Annotated[
    str | None,
    Parameter(
        name="--public-api-root",
        env_var=["AIRBYTE_API_ROOT", "AIRBYTE_CLOUD_API_URL"],
        help="Airbyte public API root URL override.",
    ),
]
ConnectionIdArg = Annotated[
    str | None, Parameter(name="--connection-id", help="The connection ID.")
]
SourceIdArg = Annotated[str | None, Parameter(name="--source-id", help="The source ID.")]
DestinationIdArg = Annotated[
    str | None,
    Parameter(name="--destination-id", help="The destination ID."),
]
JobIdArg = Annotated[int | None, Parameter(name="--job-id", help="The job ID.")]
PositionalIdArg = Annotated[str, Parameter(show=False, consume_multiple=True)]

JsonInputArg = Annotated[
    str | None,
    Parameter(
        name="--json",
        help="Inline JSON object, or file path prefixed with @, ., or /.",
    ),
]
JsonFileInputArg = Annotated[
    Path | None,
    Parameter(
        name="--json-file",
        help="Path to a JSON file containing command input values.",
    ),
]


_P = ParamSpec("_P")
_R = TypeVar("_R")
_MISSING = object()


def parse_json_input_options(
    *,
    json_input: str | None = None,
    json_file: Path | None = None,
) -> dict[str, object]:
    """Parse command input from an inline JSON object or JSON file."""
    if json_input and json_file:
        raise PyAirbyteInputError(
            message="Only one JSON input source is allowed.",
            context={"options": "--json, --json-file"},
        )
    if not json_input and not json_file:
        return {}

    if json_input and json_input[0] in {"@", ".", "/"}:
        json_file = Path(json_input.removeprefix("@"))
        json_input = None

    if json_file:
        if not json_file.is_file():
            raise PyAirbyteInputError(
                message="JSON input file does not exist.",
                context={"path": str(json_file)},
            )
        json_input = json_file.read_text(encoding="utf-8")

    if not json_input:
        return {}
    parsed_json = json.loads(json_input)
    if not isinstance(parsed_json, dict):
        raise PyAirbyteInputError(message="JSON input must be an object.")
    return parsed_json


def _normalize_json_input_fields(
    json_values: dict[str, object],
    *,
    field_aliases: Mapping[str, str],
    comma_list_fields: set[str],
    json_string_fields: set[str],
) -> dict[str, object]:
    """Normalize JSON input field names and values for command parameters."""
    resolved = {}
    for input_key, input_value in json_values.items():
        field_name = field_aliases.get(input_key, input_key)
        value = input_value
        if field_name in comma_list_fields and isinstance(input_value, list):
            value = ",".join(str(item) for item in input_value)
        elif field_name in json_string_fields and not isinstance(input_value, str):
            value = json.dumps(input_value, sort_keys=True)
        if field_name in resolved and resolved[field_name] != value:
            raise PyAirbyteInputError(
                message="JSON input field values conflict.",
                context={"field": field_name},
            )
        resolved[field_name] = value
    return resolved


def _json_string_values_match(named_value: object, json_value: object) -> bool:
    """Return `True` when JSON string field values are semantically equal."""
    if not isinstance(named_value, str) or not isinstance(json_value, str):
        return False
    try:
        return json.loads(named_value) == json.loads(json_value)
    except json.JSONDecodeError:
        return False


def _annotation_allows_none(annotation: object) -> bool:
    """Return `True` when an annotation accepts `None`."""
    if annotation is inspect.Parameter.empty:
        return False
    origin = get_origin(annotation)
    if origin is Annotated:
        annotation = get_args(annotation)[0]
        origin = get_origin(annotation)
    return annotation is None or (
        (origin is types.UnionType or str(origin) == "typing.Union")
        and type(None) in get_args(annotation)
    )


def _make_annotation_optional(annotation: object) -> object:
    """Return an annotation that accepts `None`."""
    if annotation is inspect.Parameter.empty:
        return annotation
    if _annotation_allows_none(annotation):
        return annotation
    typed_annotation = cast("type[object]", annotation)
    origin = get_origin(annotation)
    if origin is Annotated:
        inner_annotation, *metadata = get_args(annotation)
        return _make_annotated_optional(cast("type[object]", inner_annotation), metadata)
    return typed_annotation | None


def _make_annotated_optional(
    inner_annotation: type[object],
    metadata: list[object],
) -> object:
    """Return an `Annotated` annotation that accepts `None`."""
    return typing_extensions.Annotated[(inner_annotation | None, *metadata)]


def _is_default_cli_value(value: object, parameter: inspect.Parameter) -> bool:
    """Return `True` when a CLI value matches the wrapped command default."""
    if parameter.default is inspect.Parameter.empty:
        return value is None
    return value == parameter.default


def resolve_json_input(
    json_values: dict[str, object],
    named_values: dict[str, object],
    *,
    required_fields: set[str],
    json_string_fields: set[str] | None = None,
) -> dict[str, object]:
    """Merge JSON command input with named CLI values."""
    json_string_fields = json_string_fields or set()
    resolved = {}
    for key, json_value in json_values.items():
        if key not in named_values:
            raise PyAirbyteInputError(
                message="JSON input field is not supported.",
                context={"field": key},
            )

        named_value = named_values[key]
        if named_value is _MISSING:
            resolved[key] = json_value
            continue
        if named_value == json_value:
            continue
        if key in json_string_fields and _json_string_values_match(named_value, json_value):
            continue
        raise PyAirbyteInputError(
            message="JSON input value conflicts with named CLI argument.",
            context={"field": key},
        )

    missing_fields = sorted(
        field
        for field in required_fields
        if named_values.get(field) is _MISSING and field not in json_values
    )
    if missing_fields:
        raise PyAirbyteInputError(
            message="Required command input is missing.",
            context={"fields": ", ".join(missing_fields)},
        )
    return resolved


@overload
def with_json_input(command: Callable[_P, _R]) -> Callable[_P, _R]: ...


@overload
def with_json_input(
    *,
    comma_list_fields: set[str] | None = None,
    field_aliases: Mapping[str, str] | None = None,
    json_string_fields: set[str] | None = None,
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]: ...


def with_json_input(
    command: Callable[_P, _R] | None = None,
    *,
    comma_list_fields: set[str] | None = None,
    field_aliases: Mapping[str, str] | None = None,
    json_string_fields: set[str] | None = None,
) -> Callable[_P, _R] | Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    """Allow a Cyclopts command to accept `--json` or `--json-file` input."""
    comma_list_fields = comma_list_fields or set()
    field_aliases = field_aliases or {}
    json_string_fields = json_string_fields or set()

    def decorate(command: Callable[_P, _R]) -> Callable[_P, _R]:
        """Decorate a command with JSON input support."""
        signature = inspect.signature(command)
        resolved_annotations = get_type_hints(command, include_extras=True)

        @functools.wraps(command)
        def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            json_values = parse_json_input_options(
                json_input=kwargs.pop("json_input", None),  # pyrefly: ignore[bad-argument-type]
                json_file=kwargs.pop("json_file", None),  # pyrefly: ignore[bad-argument-type]
            )
            json_values = _normalize_json_input_fields(
                json_values,
                comma_list_fields=comma_list_fields,
                field_aliases=field_aliases,
                json_string_fields=json_string_fields,
            )
            named_values = {}
            required_fields = set()
            for name, parameter in signature.parameters.items():
                if parameter.kind is not inspect.Parameter.KEYWORD_ONLY:
                    continue
                if parameter.default is inspect.Parameter.empty:
                    required_fields.add(name)
                named_value = kwargs.get(name, _MISSING)
                if named_value is _MISSING or _is_default_cli_value(named_value, parameter):
                    named_values[name] = _MISSING
                else:
                    named_values[name] = named_value

            resolved_values = resolve_json_input(
                json_values,
                named_values,
                required_fields=required_fields,
                json_string_fields=json_string_fields,
            )
            kwargs.update(resolved_values)
            return command(*args, **kwargs)

        parameters = []
        for parameter in signature.parameters.values():
            resolved_parameter = parameter.replace(
                annotation=resolved_annotations.get(parameter.name, parameter.annotation)
            )
            if (
                parameter.kind is inspect.Parameter.KEYWORD_ONLY
                and parameter.default is inspect.Parameter.empty
            ):
                resolved_parameter = resolved_parameter.replace(
                    annotation=_make_annotation_optional(resolved_parameter.annotation),
                    default=None,
                )
            parameters.append(resolved_parameter)

        parameters.extend(
            [
                inspect.Parameter(
                    "json_input",
                    inspect.Parameter.KEYWORD_ONLY,
                    default=None,
                    annotation=JsonInputArg,
                ),
                inspect.Parameter(
                    "json_file",
                    inspect.Parameter.KEYWORD_ONLY,
                    default=None,
                    annotation=JsonFileInputArg,
                ),
            ]
        )
        wrapper.__signature__ = signature.replace(parameters=parameters)  # type: ignore[attr-defined]
        wrapper.__annotations__ = {parameter.name: parameter.annotation for parameter in parameters}
        if hasattr(wrapper, "__wrapped__"):
            delattr(wrapper, "__wrapped__")
        return wrapper  # type: ignore[return-value]

    if command is None:
        return decorate
    return decorate(command)


def parse_config_options(
    *,
    config_json: str | None = None,
    config_file: Path | None = None,
) -> dict[str, object]:
    """Parse connector configuration from JSON text or a YAML/JSON file."""
    if bool(config_json) == bool(config_file):
        raise PyAirbyteInputError(
            message="Exactly one config input is required.",
            context={"options": "--config-json, --config-file"},
        )

    if config_json:
        parsed_json = json.loads(config_json)
        if not isinstance(parsed_json, dict):
            raise PyAirbyteInputError(message="Config JSON must be an object.")
        return parsed_json

    if not config_file:
        raise PyAirbyteInputError(message="Config file is required.")
    if not config_file.exists():
        raise PyAirbyteInputError(message="Config file does not exist.")
    parsed_file = yaml.safe_load(config_file.read_text(encoding="utf-8"))
    if not isinstance(parsed_file, dict):
        raise PyAirbyteInputError(message="Config file must contain an object.")
    return parsed_file


def parse_csv(value: str | None) -> list[str]:
    """Parse a comma-separated CLI option value."""
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def resolve_entity_id(
    args: tuple[str, ...],
    option_value: str | None,
    *,
    option_name: str,
) -> str:
    """Resolve an entity ID from one positional argument or a named option."""
    if len(args) > 1:
        raise PyAirbyteInputError(message="Only one entity ID argument is allowed.")

    arg_value = args[0] if args else None
    if arg_value and option_value and arg_value != option_value:
        raise PyAirbyteInputError(message="Entity ID arguments must match.")

    entity_id = arg_value or option_value
    if not entity_id:
        raise PyAirbyteInputError(
            message="Entity ID is required.",
            context={"option": option_name},
        )

    return entity_id
