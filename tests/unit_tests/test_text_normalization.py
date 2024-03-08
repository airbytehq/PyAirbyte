from math import exp
import pytest
from airbyte._util.name_normalizers import CaseInsensitiveDict, LowerCaseNormalizer

def test_case_insensitive_dict():
    # Initialize a CaseInsensitiveDict
    cid = CaseInsensitiveDict({"Upper": 1, "lower": 2})

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
    # Initialize a CaseInsensitiveDict
    cid = CaseInsensitiveDict({"Upper": 1, "lower": 2}, expected_keys=["Upper", "lower", "other"])

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
    # Initialize a CaseInsensitiveDict
    cid = CaseInsensitiveDict(
        {"Upper": 1, "lower": 2},
        expected_keys=["Upper", "lower", "other"],
        normalize_keys=False,
    )

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
