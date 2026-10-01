# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Pure argument-trace classification, canonicalization, keys, records, and validation."""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import subprocess
import sys
import time
from enum import Enum
from pathlib import Path
from typing import Annotated, Literal

import pytest
from fastmcp import Context
from fastmcp.tools import ToolResult
from mcp.types import TextContent

from airbyte.mcp import _arg_trace as t
from airbyte.mcp._arg_trace import ArgClass, Cat


FIXTURES = Path(__file__).parent / "fixtures"
SENTINEL = "zz-SENTINEL-7f3a-do-not-export"
MASTER = bytes(range(32, 64))
PRINCIPAL = "https://issuer.example\x00user-0001"
DIGEST = "ab" * 32
KEYS = t.keys_for(MASTER, "transport_session", PRINCIPAL, DIGEST)

HASH = ArgClass(Cat.HASH)
EQ_ONLY = ArgClass(Cat.EQ_ONLY)
LIST = ArgClass(Cat.LIST)


def _jaccard(a: str, b: str) -> float:
    x, y = int(a, 16), int(b, 16)
    return (x & y).bit_count() / (x | y).bit_count()


def _records(args, classes, keys=KEYS, tool="synthetic_tool"):
    return {
        key.removeprefix(t.ARG_PREFIX): json.loads(value)
        for key, value in t.build_records(tool, args, classes, keys).items()
    }


# ---------------------------------------------------------------- classification


class _Color(Enum):
    RED = "red"
    BLUE = "blue"


def _synthetic_tool(  # noqa: PLR0913
    ctx: Context,
    intent: str,
    workspace_id: str,
    connection_id: str,
    flag: bool,
    mode: Literal["a", "b"],
    color: _Color,
    modes: list[Literal["x", "y"]] | None,
    names: str | list[str] | None,
    free: Annotated[str, "doc"],
    mixed: str | Literal["auto"],
    number: int,
    config: dict | str | None = None,
    api_args: str | None = None,
    entity_type: str = "",
) -> Literal["Error: nope", "Error: other"] | str:
    return ""


def test_classify_tool_rules():
    classes = t.classify_tool(_synthetic_tool)
    assert "ctx" not in classes
    assert {name: cls.cat for name, cls in classes.items()} == {
        "intent": Cat.SKIP,
        "workspace_id": Cat.SKIP,
        "connection_id": Cat.EQ_ONLY,
        "flag": Cat.VALUE,
        "mode": Cat.VALUE,
        "color": Cat.VALUE,
        "modes": Cat.VALUE,
        "names": Cat.LIST,
        "free": Cat.HASH,
        "mixed": Cat.HASH,
        "number": Cat.HASH,
        "config": Cat.PRESENT,
        "api_args": Cat.EQ_ONLY,
        "entity_type": Cat.ENTITY,
    }
    assert classes["color"].allowed == {"red", "blue"}
    assert classes["modes"].allow_list
    assert t.literal_error_strings(_synthetic_tool) == {"Error: nope", "Error: other"}


def test_manual_map_tool_specific_entries():
    assert t.classify_arg("show_connectors_list", "connector_type", str).allowed == {
        "",
        "source",
        "destination",
    }
    assert t.classify_arg("other", "connector_type", str).cat is Cat.HASH
    streams = t.classify_arg("execute_external_search_query", "streams", str)
    assert (streams.cat, streams.no_fp, streams.list_parser) == (
        Cat.LIST,
        True,
        "dicts",
    )
    assert t.classify_arg("x", "page_size", int).describe() == "value range=0..10000"


def test_inventory_matches_fixture():
    """Every registered tool argument has the reviewed classification."""
    import airbyte.mcp.server  # noqa: F401, PLC0415  # Registers every tool.
    from fastmcp_extensions.decorators import _REGISTERED_TOOLS  # noqa: PLC2701, PLC0415

    actual = {
        func.__name__: {
            name: cls.describe() for name, cls in t.classify_tool(func).items()
        }
        for func, _ in _REGISTERED_TOOLS
    }
    expected = json.loads((FIXTURES / "arg_trace_classes.json").read_text())
    assert actual == expected


# ---------------------------------------------------------------- canonicalization


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Ab c", b'"Ab c"'),
        ("", b'""'),
        (0, b"0"),
        (True, b"true"),
        (1, b"1"),
        (1.0, b"1"),
        (1.5, b"1.5"),
        (float(2**53), b"9007199254740992.0"),
        (Path("a/b"), b'"a/b"'),
        ({"b": 1, "a": "é"}, '{"a":"é","b":1}'.encode()),
        ([], b"[]"),
    ],
)
def test_canonical_bytes(value, expected):
    assert t.canonical_bytes(value, HASH) == expected


@pytest.mark.parametrize(
    "value",
    [
        math.nan,
        math.inf,
        {1: "x"},
        object(),
        b"bytes",
        "x" * (t.MAX_RAW_STRING + 1),
        "é" * (t.MAX_RAW_STRING // 2 + 1),
        "\ud800",
    ],
)
def test_canonical_bytes_unsupported(value):
    assert t.canonical_bytes(value, HASH) is None


def _nested(depth: int) -> object:
    value: object = "leaf"
    for _ in range(depth):
        value = [value]
    return value


def test_canonical_caps():
    assert t.canonical_bytes(_nested(t.MAX_DEPTH), HASH) is not None
    assert t.canonical_bytes(_nested(t.MAX_DEPTH + 1), HASH) is None
    assert t.canonical_bytes({str(i): i for i in range(t.MAX_NODES - 1)}, HASH)
    assert t.canonical_bytes({str(i): i for i in range(t.MAX_NODES)}, HASH) is None
    assert t.canonical_bytes(["a"] * t.MAX_LIST_ITEMS, LIST) is not None
    assert t.canonical_bytes(["a"] * (t.MAX_LIST_ITEMS + 1), LIST) is None
    assert t.canonical_bytes(["x" * 30_000, "y" * 40_000], LIST) is None
    cycle: list[object] = []
    cycle.append(cycle)
    assert t.canonical_bytes(cycle, HASH) is None


def test_list_canonical_is_order_free_and_keeps_duplicates():
    a = t.canonical_bytes(["b", "a", "a"], LIST)
    assert a == t.canonical_bytes(["a", "b", "a"], LIST)
    assert a == t.canonical_bytes('["a","b","a"]', LIST)
    assert a == t.canonical_bytes("a, b,a", LIST)
    assert a != t.canonical_bytes(["a", "b"], LIST)


def test_api_args_json_string_equals_object():
    cls = t.MANUAL_MAP["api_args"]
    assert t.canonical_bytes('{"b":2, "a":1}', cls) == t.canonical_bytes(
        {"a": 1, "b": 2}, cls
    )
    assert t.canonical_bytes("not json", cls) == b'"not json"'


def test_raw_cap_never_calls_list_resolver(monkeypatch):
    def fail(value):
        raise AssertionError("resolver must not run on over-cap input")

    monkeypatch.setattr(t._arg_resolvers, "resolve_list_of_strings", fail)  # noqa: SLF001
    assert _records({"names": "a," * 40_000}, {"names": LIST}) == {
        "names": {"present": True}
    }


# ---------------------------------------------------------------- records


def test_null_omitted_and_empty_zero_traced():
    classes = {"a": HASH, "b": HASH, "c": t.MANUAL_MAP["limit"], "d": LIST, "e": HASH}
    records = _records({"a": None, "b": "", "c": 0, "d": [], "e": False}, classes)
    assert "a" not in records
    assert set(records["b"]) == {"eq"}
    assert records["c"] == {"value": 0}
    assert records["d"]["count"] == 0
    assert set(records["e"]) == {"eq"}


def test_value_records():
    support = t.MANUAL_MAP["min_support_level"]
    flag = t.classify_arg("x", "flag", bool)
    modes = t.classify_arg("x", "modes", list[Literal["x", "y"]] | None)
    records = _records(
        {
            "s": "certified",
            "s2": "CERTIFIED",
            "f": True,
            "f2": "true",
            "m": ["y", "x"],
            "p": 10_001,
            "p2": 5.0,
            "p3": True,
        },
        {
            "s": support,
            "s2": support,
            "f": flag,
            "f2": flag,
            "m": modes,
            "p": t.MANUAL_MAP["page_size"],
            "p2": t.MANUAL_MAP["page_size"],
            "p3": t.MANUAL_MAP["page_size"],
        },
    )
    assert records["s"] == {"value": "certified"}
    assert records["f"] == {"value": True}
    assert records["m"] == {"value": ["x", "y"]}
    for name in ("s2", "f2", "p", "p2", "p3"):
        assert "value" not in records[name]
        assert "eq" in records[name]


def test_present_list_and_entity_records():
    records = _records(
        {
            "config": {"password": SENTINEL},
            "names": "a,b,c",
            "raw_list": "[x]",
            "entity_type": "customers",
            "long_entity": "e" * 257,
        },
        {
            "config": ArgClass(Cat.PRESENT),
            "names": LIST,
            "raw_list": LIST,
            "entity_type": ArgClass(Cat.ENTITY),
            "long_entity": ArgClass(Cat.ENTITY),
        },
    )
    assert records["config"] == {"present": True}
    assert records["names"]["count"] == 3
    assert set(records["names"]) == {"count", "eq", "fp"}
    assert set(records["raw_list"]) == {"eq"}
    assert records["entity_type"] == {"value": "customers"}
    assert set(records["long_entity"]) == {"eq"}


@pytest.mark.parametrize("value", ["", " x", "x\n", "e" * 257, 5])
def test_entity_bounds_reject(value):
    assert not t.entity_in_bounds(value)


def test_keyless_records_never_digest():
    classes = {"a": HASH, "b": EQ_ONLY, "c": LIST, "d": ArgClass(Cat.ENTITY)}
    records = _records(
        {"a": SENTINEL, "b": SENTINEL, "c": [SENTINEL], "d": "x" * 300},
        classes,
        keys=None,
    )
    assert records == {
        "a": {"present": True},
        "b": {"present": True},
        "c": {"count": 1, "present": True},
        "d": {"present": True},
    }


def test_builder_exception_is_per_argument(monkeypatch):
    real = t._record  # noqa: SLF001

    def flaky(tool, name, value, cls, keys):
        if name == "bad":
            raise RuntimeError(SENTINEL)
        return real(tool, name, value, cls, keys)

    monkeypatch.setattr(t, "_record", flaky)
    records = _records({"bad": "x", "good": "y"}, {"bad": HASH, "good": HASH})
    assert records["bad"] == {"present": True}
    assert "eq" in records["good"]


def test_sentinel_absent_from_every_non_value_record():
    classes = {
        "h": HASH,
        "e": EQ_ONLY,
        "l": LIST,
        "p": ArgClass(Cat.PRESENT),
        "api_args": t.MANUAL_MAP["api_args"],
        "entity": ArgClass(Cat.ENTITY),
        "mode": ArgClass(Cat.VALUE, allowed=frozenset({"a"})),
    }
    args = {
        "h": SENTINEL,
        "e": SENTINEL,
        "l": [SENTINEL, "x"],
        "p": {SENTINEL: SENTINEL},
        "api_args": json.dumps({SENTINEL: SENTINEL}),
        "entity": SENTINEL + " ",
        "mode": SENTINEL,
        SENTINEL: SENTINEL,
    }
    for keys in (KEYS, None):
        encoded = json.dumps(t.build_records("synthetic_tool", args, classes, keys))
        assert SENTINEL not in encoded
    positive = t.build_records("synthetic_tool", {"entity": SENTINEL}, classes, KEYS)
    assert SENTINEL in json.dumps(positive)


def test_records_fit_cap():
    classes = {"m": ArgClass(Cat.VALUE, allowed=frozenset({"x" * 390}))}
    assert _records({"m": "x" * 390}, classes)["m"] == {"present": True}


# ---------------------------------------------------------------- fingerprints and keys


def test_fp_near_miss_and_unrelated():
    key = t.k_fp(KEYS, "synthetic_tool", "name")
    base = "synthetic-connection-name-0001"
    near = t.fp_text(base[:-1] + "2", key)
    assert near is not None
    assert _jaccard(t.fp_text(base, key), near) >= 0.6
    assert (
        _jaccard(t.fp_text(base, key), t.fp_text("Quarterly Revenue By Rgn", key)) < 0.6
    )
    assert t.fp_text("A  b", key) == t.fp_text("a b", key)
    assert t.fp_text("x" * 41, key) is None
    assert t.fp_text("x" * 161, key) is None
    assert t.fp_text("   ", key) is None


def test_fp_list_near_miss():
    key = t.k_fp(KEYS, "synthetic_tool", "fields")
    a = t.fp_list(["id", "name", "email", "created_at", "updated_at"], key)
    b = t.fp_list(["updated_at", "name", "email", "id", "created_on"], key)
    assert _jaccard(a, b) >= 0.6
    assert t.fp_list([], key) is None
    assert t.fp_list(["a"] * 33, key) is None
    assert t.fp_list([{"a": 1}], key) is None


def test_no_fp_for_ids_sql_api_args_streams():
    classes = {
        "connection_id": EQ_ONLY,
        "sql": t.MANUAL_MAP["sql"],
        "api_args": t.MANUAL_MAP["api_args"],
        "streams": t.MANUAL_MAP[("execute_external_search_query", "streams")],
        "long": HASH,
    }
    records = _records(
        {
            "connection_id": "abc",
            "sql": "select 1",
            "api_args": '{"a":1}',
            "streams": '[{"name":"s"}]',
            "long": "x" * 41,
        },
        classes,
    )
    for record in records.values():
        assert "fp" not in record
    assert records["streams"]["count"] == 1


def test_scoping():
    other_principal = t.keys_for(MASTER, "transport_session", PRINCIPAL + "2", DIGEST)
    other_session = t.keys_for(MASTER, "transport_session", PRINCIPAL, "cd" * 32)
    other_kind = t.keys_for(MASTER, "conversation", PRINCIPAL, DIGEST)
    scopes = {
        t.scope_id(k.k_eq) for k in (KEYS, other_principal, other_session, other_kind)
    }
    assert len(scopes) == 4
    assert all(t._HEX16.fullmatch(s) for s in scopes)  # noqa: SLF001
    assert t.scope_id(KEYS.k_eq) == t.scope_id(
        t.keys_for(MASTER, "transport_session", PRINCIPAL, DIGEST).k_eq
    )
    assert t.k_fp(KEYS, "a", "b") != t.k_fp(KEYS, "a", "c")
    assert MASTER.hex() not in repr(KEYS) and str(MASTER) not in repr(KEYS)
    with pytest.raises(ValueError):
        t.keys_for(MASTER, "approximate", PRINCIPAL, DIGEST)


def test_approximate_buckets_and_clients():
    def sid(client="c", major="1", bucket=1):
        return t.scope_id(
            t.approximate_keys_for(MASTER, PRINCIPAL, client, major, bucket).k_eq
        )

    assert sid() == sid()
    assert len({sid(), sid(bucket=2), sid(client="d"), sid(major="2")}) == 4


def test_golden_vectors_and_reference_formulas():
    golden = json.loads((FIXTURES / "arg_trace_golden.json").read_text())
    master = bytes.fromhex(golden["master_hex"])
    assert t.key_id(master) == golden["key_id"]

    def ref_mac(key: bytes, msg: bytes) -> bytes:
        return hmac.new(key, msg, hashlib.sha256).digest()

    for kind, expected in golden["scopes"].items():
        if kind == "approximate":
            keys = t.approximate_keys_for(
                master,
                golden["principal"],
                expected["client_name"],
                expected["client_major"],
                expected["bucket"],
            )
        else:
            keys = t.keys_for(master, kind, golden["principal"], golden["raw_digest"])
            scope_input = (
                f"{kind}|{golden['principal']}|{golden['raw_digest']}".encode()
            )
            assert keys.scope_input == scope_input
            ref_eq = ref_mac(master, b"airbyte.mcp.v1|arg-eq|" + scope_input)
            assert keys.k_eq == ref_eq
            assert (
                t.scope_id(ref_eq)
                == ref_mac(ref_eq, b"airbyte.mcp.v1|arg-scope-id")[:8].hex()
            )
        assert t.scope_id(keys.k_eq) == expected["scope_id"]
        for canonical, digest in expected["eq"].items():
            assert t.eq_hex(keys.k_eq, canonical.encode()) == digest
            assert digest == ref_mac(keys.k_eq, canonical.encode())[:8].hex()
        for text, fingerprint in expected["fp_text"].items():
            assert (
                t.fp_text(text, t.k_fp(keys, "synthetic_tool", "name")) == fingerprint
            )
        assert (
            t.fp_list(
                ["alpha", "beta", "gamma"], t.k_fp(keys, "synthetic_tool", "fields")
            )
            == expected["fp_list"]
        )


def test_determinism_across_hash_seeds():
    script = (
        "import json;from airbyte.mcp import _arg_trace as t;"
        "k=t.keys_for(bytes(range(32)),'transport_session','p\\x00u','ab'*32);"
        "c={'a':t.ArgClass(t.Cat.LIST),'b':t.ArgClass(t.Cat.HASH)};"
        "print(json.dumps(t.build_records('x',{'a':['q','r','s'],'b':{'z':1,'y':[2,1]}},c,k)))"
    )
    outputs = {
        subprocess.run(
            [sys.executable, "-c", script],
            env={**os.environ, "PYTHONHASHSEED": seed},
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        for seed in ("0", "1", "12345")
    }
    assert len(outputs) == 1


def test_per_call_cost_at_caps():
    classes = {f"h{i}": HASH for i in range(8)} | {
        "l": LIST,
        "api_args": t.MANUAL_MAP["api_args"],
    }
    args: dict[str, object] = {f"h{i}": "x" * 40 for i in range(8)}
    args["l"] = [f"field_{i}" for i in range(t.FP_LIST_MAX)]
    args["api_args"] = json.dumps({
        str(i): "v" * 10 for i in range(t.MAX_NODES // 2 - 1)
    })
    runs = 20
    start = time.perf_counter()
    for _ in range(runs):
        t.build_records("synthetic_tool", args, classes, KEYS)
    per_call_ms = (time.perf_counter() - start) * 1000 / runs
    print(f"arg-trace per-call cost at caps: {per_call_ms:.2f} ms")
    assert per_call_ms < 50  # Spec target is < 5 ms locally; loose bound for shared CI.


# ---------------------------------------------------------------- validation


CLASSES = {
    "synthetic_tool": {
        "h": HASH,
        "l": LIST,
        "entity_type": ArgClass(Cat.ENTITY),
        "mode": ArgClass(Cat.VALUE, allowed=frozenset({"a", "b"})),
        "intent": ArgClass(Cat.SKIP),
    }
}


def _span_attrs(**extra):
    attrs = dict(
        t.build_records(
            "synthetic_tool",
            {"h": "v", "l": ["a"], "mode": "a"},
            CLASSES["synthetic_tool"],
            KEYS,
        )
    )
    attrs |= {
        t.TRACING_KEY: "ok",
        t.KEY_SCOPE_KEY: "transport_session",
        t.SCOPE_ID_KEY: t.scope_id(KEYS.k_eq),
        t.RESULT_ERROR_LIKE_KEY: False,
        "airbyte.mcp.intent": "unrelated",
    }
    return attrs | extra


def test_validate_accepts_builder_output():
    attrs = _span_attrs()
    accepted, dropped = t.validate("synthetic_tool", attrs, CLASSES)
    assert dropped == 0
    assert accepted == {k: v for k, v in attrs.items() if t.is_new_key(k)}


@pytest.mark.parametrize(
    "extra",
    [
        {t.ARG_PREFIX + SENTINEL: '{"present":true}'},
        {t.ARG_PREFIX + "intent": '{"value":"x"}'},
        {t.ARG_PREFIX + "h": json.dumps({"value": SENTINEL})},
        {t.ARG_PREFIX + "h": '{"eq":"' + "0" * 15 + '"}'},
        {t.ARG_PREFIX + "h": '{"eq": "0000000000000000"}'},
        {t.ARG_PREFIX + "h": '{"fp":"0000000000000000"}'},
        {t.ARG_PREFIX + "h": '{"eq":"0000000000000000","invalid_count":1}'},
        {t.ARG_PREFIX + "mode": '{"value":"c"}'},
        {t.ARG_PREFIX + "l": '{"count":-1,"present":true}'},
        {t.ARG_PREFIX + "l": '{"count":true,"present":true}'},
        {t.ARG_PREFIX + "entity_type": json.dumps({"value": "x" * 300})},
        {t.ARG_PREFIX + "h": '{"present":true,"eq":"0000000000000000"}'},
        {t.VALID_PREFIX + "h": True},
        {t.ENTITY_VALID_KEY: True},
        {t.TRACING_KEY: SENTINEL},
        {t.SCOPE_ID_KEY: SENTINEL},
        {t.RESULT_ERROR_LIKE_KEY: "true"},
        {"airbyte.mcp.arguments": SENTINEL},
    ],
)
def test_validate_rejects_and_counts(extra):
    accepted, dropped = t.validate("synthetic_tool", _span_attrs(**extra), CLASSES)
    assert dropped >= 1
    assert SENTINEL not in json.dumps(accepted)
    for key in extra:
        if key in accepted:
            assert accepted[key] != extra[key]


def test_validate_drops_digests_without_ok_tracing():
    attrs = _span_attrs(**{t.TRACING_KEY: "no_key", t.KEY_SCOPE_KEY: "none"})
    accepted, dropped = t.validate("synthetic_tool", attrs, CLASSES)
    assert not any(
        "eq" in str(v) for k, v in accepted.items() if k.startswith(t.ARG_PREFIX)
    )
    assert t.SCOPE_ID_KEY not in accepted
    assert dropped == 3  # `h` and `l` digests plus the scope id.


def test_validate_unknown_tool_drops_everything():
    accepted, dropped = t.validate("other_tool", _span_attrs(), CLASSES)
    assert accepted == {}
    assert dropped == 7


def test_validate_ignores_incoming_dropped_count():
    accepted, dropped = t.validate(
        "synthetic_tool", _span_attrs(**{t.DROPPED_KEY: 99}), CLASSES
    )
    assert dropped == 0
    assert t.DROPPED_KEY not in accepted


def test_merge_tool_flats_entity_valid():
    attrs: dict[str, object] = {
        t.ARG_PREFIX + "entity_type": '{"value":"customers"}',
        t.ENTITY_VALID_KEY: True,
        t.TRACING_KEY: "no_key",
        t.KEY_SCOPE_KEY: "none",
    }
    t.merge_tool_flats(attrs)
    assert attrs[t.ARG_PREFIX + "entity_type"] == '{"valid":true,"value":"customers"}'
    accepted, dropped = t.validate("synthetic_tool", attrs, CLASSES)
    assert dropped == 0
    assert accepted[t.ENTITY_VALID_KEY] is True

    hashed: dict[str, object] = {
        t.ARG_PREFIX + "entity_type": '{"present":true}',
        t.ENTITY_VALID_KEY: True,
    }
    t.merge_tool_flats(hashed)
    assert t.ENTITY_VALID_KEY not in hashed
    orphan: dict[str, object] = {t.ENTITY_VALID_KEY: True, t.VALID_PREFIX + "h": True}
    t.merge_tool_flats(orphan)
    assert orphan == {}


# ---------------------------------------------------------------- error-like results


def test_is_error_like_is_structural():
    errors = frozenset({"Error: nope"})
    assert t.is_error_like(
        ToolResult(content=[TextContent(type="text", text="Error: nope")]), errors
    )
    assert t.is_error_like(
        ToolResult(structured_content={"result": "Error: nope"}), errors
    )
    assert not t.is_error_like(
        ToolResult(content=[TextContent(type="text", text="Error: nope, details")]),
        errors,
    )
    assert not t.is_error_like(
        ToolResult(content=[TextContent(type="text", text="ok")]), errors
    )
    assert not t.is_error_like(
        ToolResult(content=[TextContent(type="text", text="Error: nope")]), frozenset()
    )
