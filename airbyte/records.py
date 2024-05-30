# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte Records module.

## Understanding record handling in PyAirbyte

PyAirbyte models record handling after Airbyte's "Destination V2" ("Dv2") record handling. This
includes the below implementation details.

### Field Name Normalization

1. PyAirbyte normalizes top-level record keys to lowercase, replacing spaces and hyphens with
   underscores.
2. PyAirbyte does not normalize nested keys on sub-properties.

For example, the following record:

```json
{

    "My-Field": "value",
    "Nested": {
        "MySubField": "value"
    }
}
```

Would be normalized to:

```json
{
    "my_field": "value",
    "nested": {
        "MySubField": "value"
    }
}
```

### Table Name Normalization

Similar to column handling, PyAirbyte normalizes table names to the lowercase version of the stream
name and may remove or normalize special characters.

### Airbyte-Managed Metadata Columns

PyAirbyte adds the following columns to every record:

- `ab_raw_id`: A unique identifier for the record.
- `ab_extracted_at`: The time the record was extracted.
- `ab_meta`: A dictionary of extra metadata about the record.

The names of these columns are included in the `airbyte.constants` module for programmatic
reference.

## Schema Evolution

PyAirbyte supports a very basic form of schema evolution:

1. Columns are always auto-added to cache tables whenever newly arriving properties are detected
   as not present in the cache table.
2. Column types will not be modified or expanded to fit changed types in the source catalog.
   - If column types change, we recommend user to manually alter the column types.
3. At any time, users can run a full sync with a `WriteStrategy` of 'replace'. This will create a
   fresh table from scratch and then swap the old and new tables after table sync is complete.

---

"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

import pytz
import ulid

from airbyte._util.name_normalizers import LowerCaseNormalizer, NameNormalizerBase
from airbyte.constants import (
    AB_EXTRACTED_AT_COLUMN,
    AB_INTERNAL_COLUMNS,
    AB_META_COLUMN,
    AB_RAW_ID_COLUMN,
)


if TYPE_CHECKING:
    from airbyte_protocol.models import (
        AirbyteRecordMessage,
    )


class StreamRecord(dict[str, Any]):
    """The StreamRecord class is a case-aware, case-insensitive dictionary implementation.

    It has these behaviors:
    - When a key is retrieved, deleted, or checked for existence, it is always checked in a
      case-insensitive manner.
    - The original case is stored in a separate dictionary, so that the original case can be
      retrieved when needed.
    - Because it is subclassed from `dict`, the `StreamRecord` class can be passed as a normal
      Python dictionary.
    - In addition to the properties of the stream's records, the dictionary also stores the Airbyte
      metadata columns: `_airbyte_raw_id`, `_airbyte_extracted_at`, and `_airbyte_meta`.

    This behavior mirrors how a case-aware, case-insensitive SQL database would handle column
    references.

    There are two ways this class can store keys internally:
    - If normalize_keys is True, the keys are normalized using the given normalizer.
    - If normalize_keys is False, the original case of the keys is stored.

    In regards to missing values, the dictionary accepts an 'expected_keys' input. When set, the
    dictionary will be initialized with the given keys. If a key is not found in the input data, it
    will be initialized with a value of None. When provided, the 'expected_keys' input will also
    determine the original case of the keys.
    """

    def _display_case(self, key: str) -> str:
        """Return the original case of the key."""
        return self._pretty_case_keys[self._normalizer.normalize(key)]

    def _index_case(self, key: str) -> str:
        """Return the internal case of the key.

        If normalize_keys is True, return the normalized key.
        Otherwise, return the original case of the key.
        """
        if self._normalize_keys:
            return self._normalizer.normalize(key)

        return self._display_case(key)

    @classmethod
    def from_record_message(
        cls,
        record_message: AirbyteRecordMessage,
        *,
        prune_extra_fields: bool,
        normalize_keys: bool = True,
        normalizer: type[NameNormalizerBase] | None = None,
        expected_keys: list[str] | None = None,
    ) -> StreamRecord:
        """Return a StreamRecord from a RecordMessage."""
        data_dict: dict[str, Any] = record_message.data.copy()
        data_dict[AB_RAW_ID_COLUMN] = str(ulid.ULID())
        data_dict[AB_EXTRACTED_AT_COLUMN] = datetime.fromtimestamp(
            record_message.emitted_at / 1000, tz=pytz.utc
        )
        data_dict[AB_META_COLUMN] = {}

        return cls(
            from_dict=data_dict,
            prune_extra_fields=prune_extra_fields,
            normalize_keys=normalize_keys,
            normalizer=normalizer,
            expected_keys=expected_keys,
        )

    def __init__(
        self,
        from_dict: dict,
        *,
        prune_extra_fields: bool,
        normalize_keys: bool = True,
        normalizer: type[NameNormalizerBase] | None = None,
        expected_keys: list[str] | None = None,
    ) -> None:
        """Initialize the dictionary with the given data.

        Args:
            from_dict: The dictionary to initialize the StreamRecord with.
            prune_extra_fields: If `True`, unexpected fields will be removed.
            normalize_keys: If `True`, the keys will be normalized using the given normalizer.
            normalizer: The normalizer to use when normalizing keys. If not provided, the
                LowerCaseNormalizer will be used.
            expected_keys: If provided and `prune_extra_fields` is True, then unexpected fields
                will be removed. This option is ignored if `expected_keys` is not provided.
        """
        # If no normalizer is provided, use LowerCaseNormalizer.
        self._normalize_keys = normalize_keys
        self._normalizer: type[NameNormalizerBase] = normalizer or LowerCaseNormalizer

        # If no expected keys are provided, use all keys from the input dictionary.
        if not expected_keys:
            expected_keys = list(from_dict.keys())
            prune_extra_fields = False  # No expected keys provided.
        else:
            expected_keys = list(expected_keys)

        for internal_col in AB_INTERNAL_COLUMNS:
            if internal_col not in expected_keys:
                expected_keys.append(internal_col)

        # Store a lookup from normalized keys to pretty cased (originally cased) keys.
        self._pretty_case_keys: dict[str, str] = {
            self._normalizer.normalize(pretty_case.lower()): pretty_case
            for pretty_case in expected_keys
        }

        if normalize_keys:
            index_keys = [self._normalizer.normalize(key) for key in expected_keys]
        else:
            index_keys = expected_keys

        self.update(dict.fromkeys(index_keys))  # Start by initializing all values to None
        for k, v in from_dict.items():
            index_cased_key = self._index_case(k)
            if prune_extra_fields and index_cased_key not in index_keys:
                # Dropping undeclared field
                continue

            self[index_cased_key] = v

    def __getitem__(self, key: str) -> Any:  # noqa: ANN401
        if super().__contains__(key):
            return super().__getitem__(key)

        if super().__contains__(self._index_case(key)):
            return super().__getitem__(self._index_case(key))

        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:  # noqa: ANN401
        if super().__contains__(key):
            super().__setitem__(key, value)
            return

        if super().__contains__(self._index_case(key)):
            super().__setitem__(self._index_case(key), value)
            return

        # Store the pretty cased (originally cased) key:
        self._pretty_case_keys[self._normalizer.normalize(key)] = key

        # Store the data with the normalized key:
        super().__setitem__(self._index_case(key), value)

    def __delitem__(self, key: str) -> None:
        if super().__contains__(key):
            super().__delitem__(key)
            return

        if super().__contains__(self._index_case(key)):
            super().__delitem__(self._index_case(key))
            return

        raise KeyError(key)

    def __contains__(self, key: object) -> bool:
        assert isinstance(key, str), "Key must be a string."
        return super().__contains__(key) or super().__contains__(self._index_case(key))

    def __iter__(self) -> Any:  # noqa: ANN401
        return iter(super().__iter__())

    def __len__(self) -> int:
        return super().__len__()

    def __eq__(self, other: object) -> bool:
        if isinstance(other, StreamRecord):
            return dict(self) == dict(other)

        if isinstance(other, dict):
            return {k.lower(): v for k, v in self.items()} == {
                k.lower(): v for k, v in other.items()
            }
        return False

    def __hash__(self) -> int:  # type: ignore [override]  # Doesn't match superclass (dict)
        """Return the hash of the dictionary with keys sorted."""
        items = [(k, v) for k, v in self.items() if not isinstance(v, dict)]
        return hash(tuple(sorted(items)))
