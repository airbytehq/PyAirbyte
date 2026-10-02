# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Privacy-preserving MCP tool argument records.

Every registered tool argument is classified once at startup. Per call, each
argument becomes a compact JSON record under `airbyte.mcp.arg.<name>`:

| Category | Keyed record | Keyless record |
|---|---|---|
| `VALUE` | `{"value": v}` (closed set or bounded int) | same |
| `PRESENT` | `{"present": true}` | same |
| `HASH` | `{"eq": h}` plus `"fp"` for short text | `{"present": true}` |
| `EQ_ONLY` | `{"eq": h}` | `{"present": true}` |
| `LIST` | `{"count": n, "eq": h, "fp": f}` | `{"count": n, "present": true}` |
| `ENTITY` | `{"value": s}` within bounds, else `{"eq": h}` | `{"value": s}` or present |

`eq` is a keyed equality digest; `fp` is a keyed 128-bit similarity bitset. Both
are scoped by `ArgKeys`, derived from the hosted-only master secret and the
verified principal, so values can be compared only within one scope and are
never recoverable. Without keys no digest of any value is produced. Raw values
leave this module only in `VALUE` and bounded `ENTITY` records.

This module is pure: no I/O, no tracing imports. `validate` re-checks every
record at the export boundary so forged or malformed attributes never leave.
"""

from __future__ import annotations

import hashlib
import hmac
import inspect
import json
import math
import re
import types
import typing
import unicodedata
from dataclasses import dataclass
from enum import Enum, StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, Union, get_args, get_origin

from fastmcp import Context
from mcp.types import TextContent

from airbyte.mcp import _arg_resolvers


if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from fastmcp.tools import ToolResult


ARG_PREFIX = "airbyte.mcp.arg."
VALID_PREFIX = "airbyte.mcp.arg_valid."
ENTITY_VALID_KEY = VALID_PREFIX + "entity_type"
TRACING_KEY = "airbyte.mcp.arg_tracing"
KEY_SCOPE_KEY = "airbyte.mcp.arg_key_scope"
SCOPE_ID_KEY = "airbyte.mcp.arg_scope_id"
RESULT_ERROR_LIKE_KEY = "airbyte.mcp.result_error_like"
DROPPED_KEY = "airbyte.mcp.arg_trace_dropped"
# Every key in these families is removed before export and only re-added after `validate`.
NEW_KEY_PREFIXES: tuple[str, ...] = ("airbyte.mcp.arg", RESULT_ERROR_LIKE_KEY)
LLMOBS_ALLOWED = frozenset(
    {"arg_tracing", "arg_key_scope", "result_error_like", "arg_valid.entity_type"}
)
TRACING_STATES = frozenset({"ok", "no_key", "no_scope", "error"})
KEY_SCOPES = frozenset({"conversation", "transport_session", "approximate", "none"})
ALLOWED_RECORD_KEYS = frozenset({"value", "present", "eq", "fp", "count", "valid"})

MAX_RECORD_LENGTH = 400
MAX_RAW_STRING = 65_536
MAX_CANONICAL_BYTES = 65_536
MAX_DEPTH = 20
MAX_NODES = 5_000
MAX_LIST_ITEMS = 1_000
MAX_COUNT = 1_000_000
MAX_VALUE_LIST = 16
MAX_ENTITY_LENGTH = 256
FP_RAW_MAX = 160
FP_TEXT_MAX = 40
FP_LIST_MAX = 32
FP_LIST_ITEM_MAX = 200
_MAX_SAFE_INT = 2**53
_HEX16 = re.compile(r"[0-9a-f]{16}")
_HEX32 = re.compile(r"[0-9a-f]{32}")
FP_BITS = 128

_LABEL_EQ = b"airbyte.mcp.v1|arg-eq|"
_LABEL_FP = b"airbyte.mcp.v1|arg-fp|"
_LABEL_KEY_ID = b"airbyte.mcp.v1|arg-key-id"
_LABEL_SCOPE_ID = b"airbyte.mcp.v1|arg-scope-id"


class Cat(StrEnum):
    """Argument trace category."""

    VALUE = "value"
    PRESENT = "present"
    HASH = "hash"
    EQ_ONLY = "eq_only"
    LIST = "list"
    ENTITY = "entity"
    SKIP = "skip"


@dataclass(frozen=True)
class ArgClass:
    """Static trace classification of one tool argument.

    `allow_list` lets a `VALUE` argument also carry a sorted list of `allowed`
    members (`list[Literal]` hints). `json_object` parses a JSON string into an
    object before hashing, matching `resolve_api_args`.
    """

    cat: Cat
    allowed: frozenset[str | int | bool] | None = None
    num_range: tuple[float, float] | None = None
    no_fp: bool = False
    int_only: bool = False
    list_parser: Literal["strings", "dicts"] = "strings"
    allow_list: bool = False
    json_object: bool = False

    def describe(self) -> str:
        """Return a stable one-line summary for the inventory fixture."""
        parts = [self.cat.value]
        if self.allowed is not None:
            parts.append("allowed=" + _dump(sorted(self.allowed, key=_dump)))
        if self.allow_list:
            parts.append("list")
        if self.num_range is not None:
            low, high = self.num_range
            parts.append(
                f"range={int(low)}..{int(high)}" if self.int_only else f"range={low}..{high}"
            )
        if self.no_fp:
            parts.append("no_fp")
        if self.cat is Cat.LIST and self.list_parser != "strings":
            parts.append(f"parser={self.list_parser}")
        return " ".join(parts)


_PRESENT = ArgClass(Cat.PRESENT)
_EQ_ONLY = ArgClass(Cat.EQ_ONLY)
_HASH = ArgClass(Cat.HASH)
_SKIP = ArgClass(Cat.SKIP)
_SUPPORT_LEVELS = ArgClass(Cat.VALUE, allowed=frozenset({"", "certified", "community", "archived"}))

SKIP_ARGS = frozenset({"intent", "telemetry", "workspace_id"})
MANUAL_MAP: dict[str | tuple[str, str], ArgClass] = {
    "config": _PRESENT,
    "testing_values": _PRESENT,
    "manifest_yaml": _PRESENT,
    "custom_scenarios": _PRESENT,
    "entity_type": ArgClass(Cat.ENTITY),
    "api_args": ArgClass(Cat.EQ_ONLY, json_object=True),
    "sql": _EQ_ONLY,
    "sql_query": _EQ_ONLY,
    "cursor": _EQ_ONLY,
    "user_email": _EQ_ONLY,
    "attempt_number": _EQ_ONLY,
    "sql_dialect": ArgClass(Cat.VALUE, allowed=frozenset({"snowflake", "bigquery"})),
    ("show_connectors_list", "support_level"): _SUPPORT_LEVELS,
    "min_support_level": _SUPPORT_LEVELS,
    ("show_connectors_list", "connector_type"): ArgClass(
        Cat.VALUE, allowed=frozenset({"", "source", "destination"})
    ),
    "page_size": ArgClass(Cat.VALUE, num_range=(0, 10_000), int_only=True),
    "limit": ArgClass(Cat.VALUE, num_range=(0, 10_000), int_only=True),
    "offset": ArgClass(Cat.VALUE, num_range=(0, 10_000_000), int_only=True),
    ("execute_external_search_query", "streams"): ArgClass(
        Cat.LIST, no_fp=True, list_parser="dicts"
    ),
}


class _Unsupported(Exception):  # noqa: N818  # Internal control flow, never raised to callers.
    """A cap, cycle, or unsupported value: the argument becomes `present`."""


# ---------------------------------------------------------------- classification


def classify_tool(func: Callable[..., object]) -> dict[str, ArgClass]:
    """Classify every client-supplied parameter of a registered tool."""
    tool = func.__name__
    try:
        hints = typing.get_type_hints(func, include_extras=True)
    except (NameError, TypeError, AttributeError):
        hints = {}
    result: dict[str, ArgClass] = {}
    for name, param in inspect.signature(func).parameters.items():
        if param.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}:
            continue
        hint = hints.get(name)
        if _unwrap(hint) is Context:
            continue
        result[name] = classify_arg(tool, name, hint)
    return result


def classify_arg(tool: str, name: str, hint: object) -> ArgClass:
    """Apply the first matching classification rule."""
    if name in SKIP_ARGS:
        return _SKIP
    manual = MANUAL_MAP.get((tool, name)) or MANUAL_MAP.get(name)
    if manual is not None:
        return manual
    if name.endswith("_id"):
        return _EQ_ONLY
    return _hint_class(hint)


def _unwrap(hint: object) -> object:
    while get_origin(hint) is Annotated:
        hint = get_args(hint)[0]
    return hint


def _union_members(hint: object) -> list[object]:
    hint = _unwrap(hint)
    if get_origin(hint) in {Union, types.UnionType}:
        members: list[object] = []
        for arg in get_args(hint):
            members.extend(_union_members(arg))
        return members
    return [] if hint is type(None) else [hint]


def _closed_values(hint: object) -> frozenset[str | int | bool] | None:
    """Return the closed value set of `bool`, a `Literal`, or an `Enum`, else `None`."""
    if hint is bool:
        return frozenset({True, False})
    if get_origin(hint) is Literal:
        values = get_args(hint)
        if values and all(isinstance(value, (str, int, bool)) for value in values):
            return frozenset(values)
        return None
    if inspect.isclass(hint) and issubclass(hint, Enum):
        values = [member.value for member in hint]
        if values and all(isinstance(value, (str, int, bool)) for value in values):
            return frozenset(values)
    return None


def _list_item(hint: object) -> object | None:
    if get_origin(hint) is list and len(get_args(hint)) == 1:
        return _unwrap(get_args(hint)[0])
    return None


def _hint_class(hint: object) -> ArgClass:
    members = _union_members(hint)
    if not members:
        return _HASH
    scalars: set[str | int | bool] = set()
    has_list = has_str = has_str_list = False
    for member in members:
        if (closed := _closed_values(member)) is not None:
            scalars |= closed
        elif (item := _list_item(member)) is not None and (
            closed := _closed_values(item)
        ) is not None:
            scalars |= closed
            has_list = True
        elif member is str:
            has_str = True
        elif _list_item(member) is str:
            has_str_list = True
        else:
            return _HASH
    if scalars and not (has_str or has_str_list):
        return ArgClass(Cat.VALUE, allowed=frozenset(scalars), allow_list=has_list)
    if has_str_list and not scalars:
        return ArgClass(Cat.LIST)
    return _HASH


def literal_error_strings(func: Callable[..., object]) -> frozenset[str]:
    """Return every `str` inside a `Literal[...]` of the return annotation."""
    try:
        hint = typing.get_type_hints(func, include_extras=True).get("return")
    except (NameError, TypeError, AttributeError):
        return frozenset()
    found: set[str] = set()
    for member in _union_members(hint):
        if get_origin(member) is Literal:
            found.update(value for value in get_args(member) if isinstance(value, str))
    return frozenset(found)


def is_error_like(result: ToolResult, errors: frozenset[str]) -> bool:
    """Return whether a result is exactly one of the tool's declared error strings."""
    if not errors:
        return False
    structured = result.structured_content
    if isinstance(structured, dict) and structured.keys() == {"result"}:
        value = structured["result"]
        if isinstance(value, str) and value in errors:
            return True
    content = result.content
    return len(content) == 1 and isinstance(content[0], TextContent) and content[0].text in errors


# ---------------------------------------------------------------- canonicalization


def _dump(value: object) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def _raw_too_long(value: str) -> bool:
    return len(value) > MAX_RAW_STRING or (
        len(value.encode("utf-8", "surrogatepass")) > MAX_RAW_STRING
    )


def _normalize(value: object, depth: int, budget: list[int]) -> object:
    budget[0] -= 1
    if budget[0] < 0:
        raise _Unsupported
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise _Unsupported
        return int(value) if value.is_integer() and abs(value) < _MAX_SAFE_INT else value
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, dict):
        if depth >= MAX_DEPTH or not all(isinstance(key, str) for key in value):
            raise _Unsupported
        return {key: _normalize(item, depth + 1, budget) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        if depth >= MAX_DEPTH or len(value) > MAX_LIST_ITEMS:
            raise _Unsupported
        return [_normalize(item, depth + 1, budget) for item in value]
    raise _Unsupported


def _encode(normalized: object) -> bytes:
    try:
        encoded = _dump(normalized).encode("utf-8")
    except (UnicodeEncodeError, ValueError) as exc:
        raise _Unsupported from exc
    if len(encoded) > MAX_CANONICAL_BYTES:
        raise _Unsupported
    return encoded


def _canonical(value: object) -> bytes:
    return _encode(_normalize(value, 0, [MAX_NODES]))


def _canonical_items(items: list[object]) -> bytes:
    budget = [MAX_NODES - 1]
    encoded = sorted(_encode(_normalize(item, 1, budget)) for item in items)
    joined = b"[" + b",".join(encoded) + b"]"
    if len(joined) > MAX_CANONICAL_BYTES:
        raise _Unsupported
    return joined


def _list_items(value: object, cls: ArgClass) -> list[object] | None:
    """Return parsed list items, or `None` when the value is hashed whole."""
    if isinstance(value, (list, tuple, set, frozenset)):
        items = list[object](value)
    elif isinstance(value, str):
        if _raw_too_long(value):
            raise _Unsupported
        parsed: object
        try:
            if cls.list_parser == "dicts":
                parsed = json.loads(value)
                if not (
                    isinstance(parsed, list) and all(isinstance(item, dict) for item in parsed)
                ):
                    return None
            else:
                parsed = _arg_resolvers.resolve_list_of_strings(value)
        except (ValueError, TypeError, RecursionError):
            return None
        items: list[object] = list(parsed) if isinstance(parsed, list) else []
    else:
        return None
    if len(items) > MAX_LIST_ITEMS:
        raise _Unsupported
    return items


def _parsed_object(value: object, cls: ArgClass) -> object:
    if cls.json_object and isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (ValueError, RecursionError):
            return value
        return parsed if isinstance(parsed, dict) else value
    return value


def canonical_bytes(value: object, cls: ArgClass) -> bytes | None:
    """Return the canonical encoding used for `eq`, or `None` for `present`."""
    try:
        if isinstance(value, str) and _raw_too_long(value):
            return None
        if cls.cat is Cat.LIST:
            items = _list_items(value, cls)
            if items is not None:
                return _canonical_items(items)
        return _canonical(_parsed_object(value, cls))
    except Exception:
        return None


# ---------------------------------------------------------------- keys


@dataclass(frozen=True)
class ArgKeys:
    """Per-scope key material; kept in process memory only."""

    kind: Literal["conversation", "transport_session", "approximate"]
    k_eq: bytes
    master: bytes
    scope_input: bytes

    def __repr__(self) -> str:
        """Never render key material."""
        return f"ArgKeys(kind={self.kind!r})"


def _hmac(key: bytes, message: bytes) -> bytes:
    return hmac.new(key, message, hashlib.sha256).digest()


def key_id(master: bytes) -> str:
    """Return a short non-secret identifier of the master key for logs."""
    return _hmac(master, _LABEL_KEY_ID)[:4].hex()


def _keys(master: bytes, kind: str, scope_input: bytes) -> ArgKeys:
    return ArgKeys(
        kind=kind,  # type: ignore[arg-type]  # Callers pass the closed set.
        k_eq=_hmac(master, _LABEL_EQ + scope_input),
        master=master,
        scope_input=scope_input,
    )


def keys_for(master: bytes, kind: str, principal: str, raw_digest: str) -> ArgKeys:
    """Return keys for a grouped (`conversation` / `transport_session`) scope."""
    if kind not in {"conversation", "transport_session"}:
        raise ValueError("unsupported grouping kind")
    return _keys(master, kind, f"{kind}|{principal}|{raw_digest}".encode())


def approximate_keys_for(
    master: bytes, principal: str, client_name: str, client_major: str, bucket: int
) -> ArgKeys:
    """Return keys for the approximate (principal, client, 30-minute bucket) scope."""
    client = hashlib.sha256(f"{client_name}\x00{client_major}".encode()).hexdigest()
    return _keys(master, "approximate", f"approximate|{principal}|{client}|{int(bucket)}".encode())


def scope_id(k_eq: bytes) -> str:
    """Return the exported scope identifier for comparing records."""
    return _hmac(k_eq, _LABEL_SCOPE_ID)[:8].hex()


def k_fp(keys: ArgKeys, tool: str, arg: str) -> bytes:
    """Return the similarity key for one (scope, tool, argument)."""
    return _hmac(
        keys.master,
        _LABEL_FP + keys.scope_input + b"|" + tool.encode() + b"|" + arg.encode(),
    )


def eq_hex(k_eq: bytes, canonical: bytes) -> str:
    """Return the keyed equality digest of canonical bytes."""
    return _hmac(k_eq, canonical)[:16].hex()


def _bitset(features: Iterable[str], key: bytes) -> str | None:
    bits = 0
    for feature in features:
        bits |= 1 << (_hmac(key, feature.encode("utf-8", "surrogatepass"))[0] % FP_BITS)
    return f"{bits:032x}" if bits else None


def fp_text(s: str, key: bytes) -> str | None:
    """Return a keyed trigram bitset for short text, else `None`."""
    if len(s) > FP_RAW_MAX:
        return None
    text = " ".join(unicodedata.normalize("NFKC", s).casefold().split())
    if not 1 <= len(text) <= FP_TEXT_MAX:
        return None
    marked = "\x02" + text + "\x03"
    return _bitset((marked[i : i + 3] for i in range(len(marked) - 2)), key)


def fp_list(items: list[object], key: bytes) -> str | None:
    """Return a keyed item bitset for 1-32 strings, else `None`."""
    if not 1 <= len(items) <= FP_LIST_MAX or not all(isinstance(i, str) for i in items):
        return None
    return _bitset(
        (
            "i:" + unicodedata.normalize("NFKC", item).casefold()
            for item in typing.cast("list[str]", items)
            if len(item) <= FP_LIST_ITEM_MAX
        ),
        key,
    )


# ---------------------------------------------------------------- records


_PRESENT_RECORD: dict[str, object] = {"present": True}


def _member(value: object, allowed: frozenset[str | int | bool]) -> bool:
    # Exact type match: `True == 1` and `1 == 1.0` must not alias closed values.
    return type(value) in {str, int, bool} and any(
        type(item) is type(value) and item == value for item in allowed
    )


def _value_record(value: object, cls: ArgClass) -> dict[str, object] | None:
    if cls.allowed is not None:
        if _member(value, cls.allowed):
            return {"value": value}
        if (
            cls.allow_list
            and isinstance(value, list)
            and len(value) <= MAX_VALUE_LIST
            and all(_member(item, cls.allowed) for item in value)
        ):
            return {"value": sorted(value, key=_dump)}
    if cls.num_range is not None:
        low, high = cls.num_range
        numeric = (
            type(value) is int
            if cls.int_only
            else type(value) in {int, float} and math.isfinite(typing.cast("float", value))
        )
        if numeric and low <= typing.cast("float", value) <= high:
            return {"value": value}
    return None


def entity_in_bounds(value: object) -> bool:
    """Return whether an `entity_type` may be exported verbatim."""
    return (
        isinstance(value, str)
        and 0 < len(value) <= MAX_ENTITY_LENGTH
        and value.isprintable()
        and value == value.strip()
    )


def _hashed_record(
    tool: str, name: str, value: object, cls: ArgClass, keys: ArgKeys | None
) -> dict[str, object]:
    if keys is None:
        return _PRESENT_RECORD
    canonical = canonical_bytes(value, cls)
    if canonical is None:
        return _PRESENT_RECORD
    record: dict[str, object] = {"eq": eq_hex(keys.k_eq, canonical)}
    if cls.cat in {Cat.HASH, Cat.VALUE} and isinstance(value, str) and not cls.no_fp:
        fingerprint = fp_text(value, k_fp(keys, tool, name))
        if fingerprint is not None:
            record["fp"] = fingerprint
    return record


def _list_record(
    tool: str, name: str, value: object, cls: ArgClass, keys: ArgKeys | None
) -> dict[str, object]:
    items = _list_items(value, cls)
    if items is None:
        return _hashed_record(tool, name, value, ArgClass(Cat.EQ_ONLY), keys)
    if keys is None:
        return {"count": len(items), "present": True}
    record: dict[str, object] = {
        "count": len(items),
        "eq": eq_hex(keys.k_eq, _canonical_items(items)),
    }
    if not cls.no_fp:
        fingerprint = fp_list(items, k_fp(keys, tool, name))
        if fingerprint is not None:
            record["fp"] = fingerprint
    return record


def _record(
    tool: str, name: str, value: object, cls: ArgClass, keys: ArgKeys | None
) -> dict[str, object]:
    if cls.cat is Cat.PRESENT:
        return _PRESENT_RECORD
    if cls.cat is Cat.VALUE and (record := _value_record(value, cls)) is not None:
        return record
    if cls.cat is Cat.ENTITY:
        if entity_in_bounds(value):
            return {"value": value}
        return _hashed_record(tool, name, value, ArgClass(Cat.EQ_ONLY), keys)
    if cls.cat is Cat.LIST:
        return _list_record(tool, name, value, cls, keys)
    return _hashed_record(tool, name, value, cls, keys)


def build_records(
    tool: str,
    args: Mapping[str, object],
    classes: Mapping[str, ArgClass],
    keys: ArgKeys | None,
) -> dict[str, str]:
    """Return `{"airbyte.mcp.arg.<name>": json}` for each traced argument present."""
    records: dict[str, str] = {}
    for name, cls in classes.items():
        if cls.cat is Cat.SKIP or name not in args or args[name] is None:
            continue
        try:
            encoded = _dump(_record(tool, name, args[name], cls, keys))
            if len(encoded) > MAX_RECORD_LENGTH:
                encoded = _dump(_PRESENT_RECORD)
        except Exception:
            encoded = _dump(_PRESENT_RECORD)
        records[ARG_PREFIX + name] = encoded
    return records


# ---------------------------------------------------------------- validation


def is_new_key(key: str) -> bool:
    """Return whether `key` belongs to an argument-tracing family."""
    return key.startswith(NEW_KEY_PREFIXES)


def _parse_record(raw: object) -> dict[str, object] | None:
    if not isinstance(raw, str) or len(raw) > MAX_RECORD_LENGTH:
        return None
    try:
        record = json.loads(raw)
    except (ValueError, RecursionError):
        return None
    if not isinstance(record, dict) or not record or not record.keys() <= ALLOWED_RECORD_KEYS:
        return None
    try:
        if _dump(record) != raw:
            return None
    except ValueError:
        return None
    return record


def _count_ok(value: object) -> bool:
    return type(value) is int and 0 <= value <= MAX_COUNT


def _record_ok(record: dict[str, object], cls: ArgClass) -> bool:  # noqa: PLR0911
    keys = set(record)
    if "eq" in keys and not (isinstance(record["eq"], str) and _HEX32.fullmatch(record["eq"])):
        return False
    if "fp" in keys and not (
        not cls.no_fp
        and "eq" in keys
        and isinstance(record["fp"], str)
        and _HEX32.fullmatch(record["fp"])
    ):
        return False
    if "present" in keys and (record["present"] is not True or "eq" in keys):
        return False
    if "count" in keys and not (cls.cat is Cat.LIST and _count_ok(record["count"])):
        return False
    if cls.cat is Cat.PRESENT:
        return keys == {"present"}
    if cls.cat is Cat.ENTITY:
        if "value" in keys:
            return (
                keys <= {"value", "valid"}
                and entity_in_bounds(record["value"])
                and (type(record.get("valid", True)) is bool)
            )
        return keys in ({"eq"}, {"present"})
    if cls.cat is Cat.VALUE and "value" in keys:
        value = record["value"]
        return keys == {"value"} and _value_record(value, cls) == record
    if cls.cat is Cat.LIST:
        return keys in (
            {"count", "present"},
            {"count", "eq"},
            {"count", "eq", "fp"},
            {"eq"},
            {"present"},
        )
    if cls.cat is Cat.EQ_ONLY:
        return keys in ({"eq"}, {"present"})
    return keys in ({"eq"}, {"eq", "fp"}, {"present"})


def validate(
    tool: str,
    attrs: Mapping[str, object],
    classes: Mapping[str, Mapping[str, ArgClass]],
) -> tuple[dict[str, str | bool | int], int]:
    """Return the argument-tracing keys that may be exported, and the dropped count.

    Non-tracing keys are ignored. An incoming `arg_trace_dropped` is never trusted.
    """
    new = {key: value for key, value in attrs.items() if is_new_key(key) and key != DROPPED_KEY}
    tool_classes = classes.get(tool)
    if tool_classes is None:
        return {}, len(new)
    accepted: dict[str, str | bool | int] = {}
    parsed: dict[str, dict[str, object]] = {}
    dropped = 0
    tracing = new.get(TRACING_KEY)
    tracing = tracing if isinstance(tracing, str) and tracing in TRACING_STATES else None
    for key, value in new.items():
        if not key.startswith(ARG_PREFIX):
            continue
        cls = tool_classes.get(key.removeprefix(ARG_PREFIX))
        record = _parse_record(value)
        if cls is None or cls.cat is Cat.SKIP or record is None:
            dropped += 1
            continue
        # Digests only exist with keys; anything else is forged or a bug.
        if not _record_ok(record, cls) or ("eq" in record and tracing != "ok"):
            dropped += 1
            continue
        accepted[key] = typing.cast("str", value)
        parsed[key.removeprefix(ARG_PREFIX)] = record
    scope = new.get(KEY_SCOPE_KEY)
    for key, value in new.items():
        if key.startswith(ARG_PREFIX):
            continue
        ok = False
        if key.startswith(VALID_PREFIX):
            name = key.removeprefix(VALID_PREFIX)
            cls = tool_classes.get(name)
            ok = (
                cls is not None
                and cls.cat is Cat.ENTITY
                and type(value) is bool
                and parsed.get(name, {}).get("valid") is value
            )
        elif key == TRACING_KEY:
            ok = tracing is not None
        elif key == KEY_SCOPE_KEY:
            ok = (
                isinstance(value, str)
                and value in KEY_SCOPES
                and tracing is not None
                and (value == "none") == (tracing != "ok")
            )
        elif key == SCOPE_ID_KEY:
            ok = isinstance(value, str) and bool(_HEX16.fullmatch(value)) and tracing == "ok"
        elif key == RESULT_ERROR_LIKE_KEY:
            ok = type(value) is bool
        if ok:
            accepted[key] = typing.cast("str | bool", value)
        else:
            dropped += 1
    if scope is not None and KEY_SCOPE_KEY not in accepted:
        accepted.pop(SCOPE_ID_KEY, None)
    return accepted, dropped


def merge_tool_flats(attrs: dict[str, object]) -> None:
    """Copy a late `arg_valid.entity_type` flat into its record, else drop the flat.

    The flat is kept beside the record so it stays searchable. A flat without a
    `value` record (absent, `eq`, or `present`) is removed without counting.
    """
    for key in [key for key in attrs if key.startswith(VALID_PREFIX)]:
        flat = attrs[key]
        record_key = ARG_PREFIX + key.removeprefix(VALID_PREFIX)
        record = _parse_record(attrs.get(record_key))
        if key == ENTITY_VALID_KEY and type(flat) is bool and record and "value" in record:
            record["valid"] = flat
            attrs[record_key] = _dump(record)
        else:
            attrs.pop(key)
