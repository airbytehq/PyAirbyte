import pytest
from airbyte import exceptions as exc
from airbyte._util.name_normalizers import LowerCaseNormalizer
from airbyte.constants import AB_INTERNAL_COLUMNS
from airbyte.records import StreamRecord


def test_case_insensitive_dict() -> None:
    # Initialize a StreamRecord
    cid = StreamRecord(
        {"Upper": 1, "lower": 2},
        prune_extra_fields=True,
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


def test_case_insensitive_dict_w() -> None:
    # Initialize a StreamRecord
    cid = StreamRecord(
        {"Upper": 1, "lower": 2},
        expected_keys=["Upper", "lower", "other"],
        prune_extra_fields=True,
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


def test_case_insensitive_w_pretty_keys() -> None:
    # Initialize a StreamRecord
    cid = StreamRecord(
        {"Upper": 1, "lower": 2},
        expected_keys=["Upper", "lower", "other"],
        normalize_keys=False,
        prune_extra_fields=True,
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
    "raw_value, expected_result, should_raise",
    [
        ("Test_String", "test_string", False),
        ("ANOTHER-TEST", "another_test", False),
        ("another.test", "another_test", False),
        ("sales(%)", "sales___", False),
        ("something_-_-_-_else", "something_______else", False),
        ("sales (%)", "sales____", False),
        ("sales-%", "sales__", False),
        ("sales(#)", "sales___", False),
        ("sales (#)", "sales____", False),
        ("sales--(#)", "sales_____", False),
        ("sales-#", "sales__", False),
        ("+1", "_1", False),
        ("1", "_1", False),
        ("2", "_2", False),
        ("3", "_3", False),
        ("-1", "_1", False),
        ("+#$", "", True),
        ("+", "", True),
        ("", "", True),
        ("*", "", True),
        ("!@$", "", True),
    ],
)
def test_lower_case_normalizer(raw_value, expected_result, should_raise):
    normalizer = LowerCaseNormalizer()

    if should_raise:
        with pytest.raises(exc.PyAirbyteNameNormalizationError):
            assert normalizer.normalize(raw_value) == expected_result
    else:
        assert normalizer.normalize(raw_value) == expected_result
