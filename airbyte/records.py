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


class StreamRecordHandler:
    """A class to handle processing of StreamRecords.

    This is a long-lived object that can be used to process multiple StreamRecords, which
    saves on memory and processing time by reusing the same object for all records of the same type.
    """

    def __init__(
        self,
        *,
        json_schema: dict,
        normalizer: type[NameNormalizerBase] = NameNormalizerBase,
        normalize_keys: bool = True,
        prune_extra_fields: bool,
    ) -> None:
        """Initialize the dictionary with the given data.

        Args:
            json_schema: The JSON Schema definition for this stream.
            normalizer: The normalizer to use when normalizing keys. If not provided, the
                LowerCaseNormalizer will be used.
            normalize_keys: If `True`, the keys will be normalized using the given normalizer.
            prune_extra_fields: If `True`, unexpected fields will be removed.
        """
        self._expected_keys: list[str] = list(json_schema.get("properties", {}).keys())
        self._normalizer: type[NameNormalizerBase] = normalizer
        self._normalize_keys: bool = normalize_keys
        self.prune_extra_fields: bool = prune_extra_fields

        self.index_keys: list[str] = [
            self._normalizer.normalize(key) if self._normalize_keys else key
            for key in self._expected_keys
        ]
        self.normalized_keys: list[str] = [
            self._normalizer.normalize(key) for key in self._expected_keys
        ]
        self.quick_lookup: dict[str, str]

        for internal_col in AB_INTERNAL_COLUMNS:
            if internal_col not in self._expected_keys:
                self._expected_keys.append(internal_col)

        # Store a lookup from normalized keys to pretty cased (originally cased) keys.
        self._pretty_case_lookup: dict[str, str] = {
            self._normalizer.normalize(pretty_case.lower()): pretty_case
            for pretty_case in self._expected_keys
        }
        # Store a map from all key versions (normalized and pretty-cased) to their normalized
        # version.
        self.quick_lookup = {
            key: self._normalizer.normalize(key)
            for key in set(self._expected_keys) | set(self._pretty_case_lookup.values())
        }

    def to_display_case(self, key: str) -> str:
        """Return the original case of the key."""
        return self._pretty_case_lookup[self._normalizer.normalize(key)]

    def to_index_case(self, key: str) -> str:
        """Return the internal case of the key.

        If `normalize_keys` is True, returns the normalized key.
        Otherwise, return the original case ("pretty case") of the key.
        """
        try:
            return self.quick_lookup[key]
        except KeyError:
            result = (
                self._normalizer.normalize(key) if self._normalize_keys else self._display_case(key)
            )
            self.quick_lookup[key] = result
            return result


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

    def __init__(
        self,
        from_dict: dict,
        *,
        stream_record_handler: StreamRecordHandler,
    ) -> None:
        """Initialize the dictionary with the given data.

        Args:
            from_dict: The dictionary to initialize the StreamRecord with.
            stream_record_handler: The StreamRecordHandler to use for processing the record.
        """
        self._stream_handler: StreamRecordHandler = stream_record_handler

        # Start by initializing all values to None
        self.update(dict.fromkeys(stream_record_handler.normalized_keys))

        # Update the dictionary with the given data
        if self._stream_handler.prune_extra_fields:
            self.update(
                {
                    self._stream_handler.to_index_case(k): v
                    for k, v in from_dict.items()
                    if self._stream_handler.to_index_case(k) in self._stream_handler.index_keys
                }
            )
        else:
            self.update({self._stream_handler.to_index_case(k): v for k, v in from_dict.items()})

    @classmethod
    def from_record_message(
        cls,
        record_message: AirbyteRecordMessage,
        *,
        stream_record_handler: StreamRecordHandler,
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

    def __getitem__(self, key: str) -> Any:  # noqa: ANN401
        try:
            return super().__getitem__(key)
        except KeyError:
            return super().__getitem__(self._index_case(key))

    def __setitem__(self, key: str, value: Any) -> None:  # noqa: ANN401
        if super().__contains__(key):
            super().__setitem__(key, value)
            return

        if super().__contains__(self._index_case(key)):
            super().__setitem__(self._index_case(key), value)
            return

        # Store the pretty cased (originally cased) key:
        self._pretty_case_lookup[self._normalizer.normalize(key)] = key

        # Store the data with the normalized key:
        super().__setitem__(self._index_case(key), value)

    def __delitem__(self, key: str) -> None:
        try:
            super().__delitem__(key)
        except KeyError:
            if super().__contains__(self._index_case(key)):
                super().__delitem__(self._index_case(key))
                return
        else:
            # No failure. Key was deleted.
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
