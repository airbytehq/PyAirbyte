import pytest
from airbyte import exceptions as exc
from airbyte._util.name_normalizers import LowerCaseNormalizer
from airbyte.constants import AB_INTERNAL_COLUMNS
from airbyte.records import StreamRecord, StreamRecordHandler
from airbyte._processors.sql.postgres import PostgresNormalizer


@pytest.fixture
def stream_json_schema() -> dict:
    return {
        "type": "object",
        "properties": {
            "Upper": {"type": "integer"},
            "lower": {"type": "integer"},
        },
        "required": ["Upper", "lower"],
    }


def test_record_columns_list(
    stream_json_schema: dict,
) -> None:
    stream_record_handler = StreamRecordHandler(
        json_schema=stream_json_schema,
        prune_extra_fields=False,
    )
    # Initialize a StreamRecord
    cid = StreamRecord(
        {"Upper": 1, "lower": 2},
        stream_record_handler=stream_record_handler,
    )
    keys = list(cid.keys())
    assert {"Upper", "lower"} <= set(keys)
    for internal_column in AB_INTERNAL_COLUMNS:
        assert internal_column in cid
        assert internal_column in keys
        cid.pop(internal_column)

    # Initialize a StreamRecord
    cid = StreamRecord(
        {"Upper": 1, "lower": 2},
        stream_record_handler=stream_record_handler,
        with_internal_columns=True,
    )
    keys = list(cid.keys())
    assert {"Upper", "lower"} <= set(keys)
    for internal_column in AB_INTERNAL_COLUMNS:
        assert internal_column in cid
        assert internal_column in keys
        cid.pop(internal_column)


def test_case_insensitive_dict(
    stream_json_schema: dict,
) -> None:
    stream_record_handler = StreamRecordHandler(
        json_schema=stream_json_schema,
        prune_extra_fields=True,
        normalize_keys=True,
    )
    cid = StreamRecord(
        {"Upper": 1, "lower": 2},
        stream_record_handler=stream_record_handler,
    )
    for internal_column in AB_INTERNAL_COLUMNS:
        assert internal_column in cid
        cid.pop(internal_column)

    # Test __getitem__
    assert cid["Upper"] == 1
    assert cid["lower"] == 2

    # Test __len__
    assert len(cid) == 2

    # Test __setitem__ and __getitem__ with case mismatch
    cid["upper"] = 3
    cid["Lower"] = 4
    assert len(cid) == 2

    assert cid["upper"] == 3
    assert cid["Lower"] == 4

    # Test __contains__
    assert "Upper" in cid
    assert "lower" in cid
    assert "Upper" in cid
    assert "lower" in cid

    # Test __contains__ with case-insensitive normalizer
    assert "Upper" in cid
    assert "lower" in cid
    assert "upper" in cid
    assert "Lower" in cid

    # Assert __eq__
    # When converting to dict, the keys should be normalized to the original case.
    assert dict(cid) != {"Upper": 3, "lower": 4}
    assert dict(cid) == {"upper": 3, "lower": 4}
    # When comparing directly to a dict, should use case insensitive comparison.
    assert cid == {"Upper": 3, "lower": 4}
    assert cid == {"upper": 3, "Lower": 4}

    # Test __iter__
    assert set(key for key in cid) == {"upper", "lower"}

    # Test __delitem__
    del cid["lower"]
    with pytest.raises(KeyError):
        _ = cid["lower"]
    assert len(cid) == 1

    del cid["upper"]
    with pytest.raises(KeyError):
        _ = cid["upper"]
    assert len(cid) == 0


def test_case_insensitive_dict_w(
    stream_json_schema: dict,
) -> None:
    stream_json_schema = stream_json_schema.copy()
    stream_json_schema["properties"]["other"] = {"type": "integer"}
    stream_record_handler = StreamRecordHandler(
        json_schema=stream_json_schema,
        prune_extra_fields=True,
        normalize_keys=True,
    )
    # Initialize a StreamRecord
    cid = StreamRecord(
        {"Upper": 1, "lower": 2},
        stream_record_handler=stream_record_handler,
    )
    for internal_column in AB_INTERNAL_COLUMNS:
        assert internal_column in cid
        cid.pop(internal_column)

    # Test __len__
    assert len(cid) == 3

    # Test __getitem__
    assert cid["Upper"] == 1
    assert cid["lower"] == 2
    assert cid["other"] is None

    # Use dict() to test exact contents
    assert dict(cid) == {"upper": 1, "lower": 2, "other": None}

    # Assert case insensitivity when comparing natively to a dict
    assert cid == {"UPPER": 1, "lower": 2, "other": None}
    assert cid == {"upper": 1, "lower": 2, "other": None}


def test_case_insensitive_w_pretty_keys(
    stream_json_schema: dict,
) -> None:
    stream_json_schema = stream_json_schema.copy()
    stream_json_schema["properties"]["other"] = {"type": "integer"}
    stream_record_handler = StreamRecordHandler(
        json_schema=stream_json_schema,
        prune_extra_fields=True,
        normalize_keys=False,
    )
    # Initialize a StreamRecord
    cid = StreamRecord(
        {"Upper": 1, "lower": 2},
        stream_record_handler=stream_record_handler,
    )
    for internal_column in AB_INTERNAL_COLUMNS:
        assert internal_column in cid
        cid.pop(internal_column)

    # Test __len__
    assert len(cid) == 3

    # Test __getitem__
    assert cid["Upper"] == 1
    assert cid["lower"] == 2
    assert cid["other"] is None

    # Because we're not normalizing keys, the keys should be stored as-is
    assert dict(cid) == {"Upper": 1, "lower": 2, "other": None}

    # Assert case insensitivity when comparing natively to a dict
    assert cid == {"UPPER": 1, "lower": 2, "other": None}
    assert cid == {"upper": 1, "lower": 2, "other": None}


@pytest.mark.parametrize(
    "raw_value, expected_result, should_raise, normalizer_class",
    [
        ("_airbyte_meta", "_airbyte_meta", False, LowerCaseNormalizer),
        ("Test_String", "test_string", False, LowerCaseNormalizer),
        ("ANOTHER-TEST", "another_test", False, LowerCaseNormalizer),
        ("another.test", "another_test", False, LowerCaseNormalizer),
        ("sales(%)", "sales___", False, LowerCaseNormalizer),
        ("something_-_-_-_else", "something_______else", False, LowerCaseNormalizer),
        ("sales (%)", "sales____", False, LowerCaseNormalizer),
        ("sales-%", "sales__", False, LowerCaseNormalizer),
        ("sales(#)", "sales___", False, LowerCaseNormalizer),
        ("sales (#)", "sales____", False, LowerCaseNormalizer),
        ("sales--(#)", "sales_____", False, LowerCaseNormalizer),
        ("sales-#", "sales__", False, LowerCaseNormalizer),
        ("+1", "_1", False, LowerCaseNormalizer),
        ("1", "_1", False, LowerCaseNormalizer),
        ("2", "_2", False, LowerCaseNormalizer),
        ("3", "_3", False, LowerCaseNormalizer),
        ("-1", "_1", False, LowerCaseNormalizer),
        ("+#$", "", True, LowerCaseNormalizer),
        ("+", "", True, LowerCaseNormalizer),
        ("", "", True, LowerCaseNormalizer),
        ("*", "", True, LowerCaseNormalizer),
        ("!@$", "", True, LowerCaseNormalizer),
        ("some.col", "some_col", False, LowerCaseNormalizer),
        # Check that the default normalizer doesn't truncate:
        ("a" * 60, "a" * 60, False, LowerCaseNormalizer),
        ("a" * 70, "a" * 70, False, LowerCaseNormalizer),
        ("a" * 100, "a" * 100, False, LowerCaseNormalizer),
        # Check that postgres normalizer truncates to 63 characters:
        ("a" * 60, "a" * 60, False, PostgresNormalizer),
        ("a" * 70, "a" * 63, False, PostgresNormalizer),
        ("a" * 100, "a" * 63, False, PostgresNormalizer),
        # Check that postgres also properly inherits special characters and casing:
        ("Test.String", "test_string", False, PostgresNormalizer),
    ],
)
def test_lower_case_normalizer(
    raw_value,
    expected_result,
    should_raise,
    normalizer_class,
):
    normalizer = normalizer_class()
    if should_raise:
        with pytest.raises(exc.PyAirbyteNameNormalizationError):
            assert normalizer.normalize(raw_value) == expected_result
    else:
        assert normalizer.normalize(raw_value) == expected_result
