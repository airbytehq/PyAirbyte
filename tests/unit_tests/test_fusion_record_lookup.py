"""Fusion search-key record retrieval coverage."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from airbyte._direct_connectors.models import ExternalApiExecuteResult
from airbyte.mcp import cloud
from airbyte.mcp._record_lookup import (
    canonical_row,
    compact_record,
    is_row_key,
    key_where,
    normalize_entity_id,
    row_key,
)


@pytest.mark.parametrize(
    "canonical", ['[["int",42]]', '[["str","abc"],["int",3]]', "row-sha256:" + "a" * 64]
)
@pytest.mark.parametrize(
    "variant",
    [
        "exact",
        "once",
        "twice",
        "slashes_one",
        "slashes_two",
        "slashes_four",
        "quotes",
        "single_quotes",
        "whitespace",
    ],
)
def test_normalize_entity_id(canonical: str, variant: str) -> None:
    variants = {
        "exact": canonical,
        "once": json.dumps(canonical),
        "twice": json.dumps(json.dumps(canonical)),
        "slashes_one": canonical.replace('"', '\\"'),
        "slashes_two": canonical.replace('"', '\\\\"'),
        "slashes_four": canonical.replace('"', '\\\\\\\\"'),
        "quotes": '"' + canonical + '"',
        "single_quotes": "'" + canonical + "'",
        "whitespace": " \n\t" + canonical + "  ",
    }
    assert normalize_entity_id(variants[variant]) == canonical


def test_normalization_preserves_valid_string_values() -> None:
    canonical = json.dumps([["str", 'a"b\\c']], separators=(",", ":"))
    assert normalize_entity_id(json.dumps(json.dumps(canonical))) == canonical
    assert normalize_entity_id(' [ [ "int", 42 ] ] ') == '[["int",42]]'


@pytest.mark.parametrize(
    "key", ["", "garbage", "null", "42", "{}", '[["int",42]', '"row-sha256:abc"']
)
def test_normalize_malformed(key: str) -> None:
    with pytest.raises(ValueError, match=r'Malformed entity_id.*\[\["int",42\]\]'):
        normalize_entity_id(key)


def test_normalization_is_bounded() -> None:
    key = '[["int",42]]'
    for _ in range(20):
        key = json.dumps(key)
    with pytest.raises(ValueError, match="Malformed entity_id"):
        normalize_entity_id(key)


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ('[["int",42]]', "`id` = 42"),
        ('[["str","a\\"b"]]', '`id` = "a\\"b"'),
        ('[["bool",false]]', "`id` = FALSE"),
        ('[["date","2026-01-01"]]', '`id` = CAST("2026-01-01" AS DATE)'),
    ],
)
def test_key_predicate(key: str, expected: str) -> None:
    assert key_where(key, [["id"]]) == expected


def test_composite_and_nested_key() -> None:
    assert (
        key_where('[["string","abc"],["int",3]]', [["parent", "id"], ["n"]])
        == '`parent`.`id` = "abc" AND `n` = 3'
    )


@pytest.mark.parametrize(
    "key",
    [
        "",
        "null",
        "{}",
        "[]",
        '[["int",true]]',
        '[["int",null]]',
        '[["x",1]]',
        '[["str","x"],["int",2]]',
    ],
)
def test_invalid_keys(key: str) -> None:
    with pytest.raises(ValueError, match="entity_id"):
        key_where(key, [["id"]])


def test_identifier_rejects_injection() -> None:
    with pytest.raises(ValueError, match="identifier"):
        key_where('[["int",1]]', [["id` OR TRUE --"]])


def test_compaction() -> None:
    assert compact_record({
        "_airbyte_meta": {},
        "null": None,
        "empty": "",
        "list": [],
        "zero": 0,
        "false": False,
        "nested": {"x": None, "y": "text"},
    }) == {"zero": 0, "false": False, "nested": {"y": "text"}}
    assert compact_record({"x": [None, "", {}]}) == {}


def test_row_hash() -> None:
    assert canonical_row({"b": None, "a": 42}) == [
        "object",
        [["a", ["int", 42]], ["b", ["null"]]],
    ]
    assert row_key({"b": None, "a": 42}) == row_key({
        "a": 42,
        "b": None,
        "_airbyte_raw_id": "ignored",
    })
    assert row_key({"a": 42}) != row_key({"a": "42"})
    assert row_key({"b": None}) != row_key({})
    assert is_row_key(row_key({}))
    with pytest.raises(ValueError, match="digest"):
        is_row_key("row-sha256:abc")


@pytest.fixture
def lookup(monkeypatch: pytest.MonkeyPatch) -> tuple[Mock, Mock, SimpleNamespace]:
    destination = SimpleNamespace(
        definition_id="22f6c74f-5699-40ff-833c-4a879ea40133",
        configuration={"project_id": "project", "dataset_id": "dataset"},
    )
    connection = SimpleNamespace(
        destination=destination,
        destination_id="dest",
        table_prefix="pre_",
        namespace_definition="destination",
        namespace_format=None,
        dump_raw_catalog=Mock(
            return_value={
                "streams": [{"stream": {"name": "issues"}, "primary_key": [["id"]]}]
            }
        ),
    )
    source = Mock()
    source.list_connections.return_value = [connection]
    workspace = Mock()
    workspace.get_connector.return_value = source
    monkeypatch.setattr(
        cloud, "_get_cloud_workspace_for_resource", Mock(return_value=workspace)
    )
    query = Mock(
        return_value=ExternalApiExecuteResult(
            status="success",
            result={
                "data": [
                    {
                        "record_json": '{"id":42,"body":"full text","empty":null,"_airbyte_meta":{}}'
                    }
                ]
            },
        )
    )
    monkeypatch.setattr(cloud, "execute_external_sql_query", query)
    return source, query, connection


@pytest.mark.parametrize("wrapped", [False, True])
def test_tool_flow(lookup: tuple[Mock, Mock, SimpleNamespace], wrapped: bool) -> None:
    _, query, _ = lookup
    result = cloud.get_external_record(
        None,
        source_id="source",
        stream_name="issues",
        entity_id=(
            json.dumps(json.dumps('[["int",42]]')) if wrapped else '[["int",42]]'
        ),
    )
    assert result.result == {
        "source_id": "source",
        "stream": "issues",
        "entity_id": '[["int",42]]',
        "data": {"id": 42, "body": "full text"},
    }
    assert (
        query.call_args.kwargs["sql"]
        == "SELECT TO_JSON_STRING(t) AS record_json FROM `project.dataset.pre_issues` AS t WHERE `id` = 42 LIMIT 1"
    )


@pytest.mark.parametrize(
    ("mode", "message"),
    [
        ("no_destination", "No BigQuery"),
        ("unknown", "Unknown stream"),
        ("ambiguous", "multiple"),
        ("missing", "No matching"),
        ("malformed", "Malformed"),
    ],
)
def test_tool_errors(
    lookup: tuple[Mock, Mock, SimpleNamespace], mode: str, message: str
) -> None:
    source, query, connection = lookup
    if mode == "no_destination":
        source.list_connections.return_value = []
    if mode == "ambiguous":
        source.list_connections.return_value = [connection, connection]
    if mode == "missing":
        query.return_value = ExternalApiExecuteResult(
            status="success", result={"data": []}
        )
    result = cloud.get_external_record(
        None,
        source_id="source",
        stream_name="unknown" if mode == "unknown" else "issues",
        entity_id="bad" if mode == "malformed" else '[["int",42]]',
    )
    assert result.status == "error"
    assert message in result.errors[0]


def test_hash_flow(lookup: tuple[Mock, Mock, SimpleNamespace]) -> None:
    _, query, _ = lookup
    key = row_key({"id": 42, "body": "full text", "empty": None})
    result = cloud.get_external_record(
        None,
        source_id="source",
        stream_name="issues",
        entity_id=json.dumps(json.dumps(key)),
    )
    assert result.status == "success"
    assert result.result["entity_id"] == key
    assert query.call_args.kwargs["sql"].endswith("WHERE TRUE LIMIT 1000")


@pytest.mark.parametrize(
    ("definition", "template", "expected"),
    [
        ("source", None, "source_namespace"),
        ("custom_format", "copy_${SOURCE_NAMESPACE}", "copy_source_namespace"),
    ],
)
def test_prefixed_hit_namespace(
    lookup: tuple[Mock, Mock, SimpleNamespace],
    definition: str,
    template: str | None,
    expected: str,
) -> None:
    _, query, connection = lookup
    connection.namespace_definition = definition
    connection.namespace_format = template
    connection.dump_raw_catalog.return_value["streams"][0]["stream"]["namespace"] = (
        "source_namespace"
    )
    result = cloud.get_external_record(
        None, source_id="source", stream_name="pre_issues", entity_id='[["int",42]]'
    )
    assert result.status == "success"
    assert f"`project.{expected}.pre_issues`" in query.call_args.kwargs["sql"]


def test_warehouse_error(lookup: tuple[Mock, Mock, SimpleNamespace]) -> None:
    _, query, _ = lookup
    query.return_value = ExternalApiExecuteResult(
        status="error", errors=["Access is disabled."]
    )
    result = cloud.get_external_record(
        None, source_id="source", stream_name="issues", entity_id='[["int",42]]'
    )
    assert result.errors == ["Access is disabled."]
