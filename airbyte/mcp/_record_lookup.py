# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Decode Fusion keys and prepare safely quoted BigQuery record lookups."""

import hashlib
import json
import math
import re
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any


def normalize_entity_id(entity_id: str) -> str:
    """Unwrap common transport/agent escaping without changing valid key values."""
    candidate = entity_id
    for _ in range(12):
        candidate = candidate.strip()
        if re.fullmatch(r"row-sha256:[0-9a-f]{64}", candidate):
            return candidate
        try:
            decoded = json.loads(candidate)
        except ValueError:
            unescaped = re.sub(r'\\+"', '"', candidate)
            if unescaped == candidate:
                if (
                    len(candidate) > 1
                    and candidate[0] == candidate[-1]
                    and candidate[0] in {"'", '"'}
                ):
                    candidate = candidate[1:-1]
                    continue
                break
            candidate = unescaped
            continue
        if isinstance(decoded, list):
            return json.dumps(decoded, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        if not isinstance(decoded, str):
            break
        candidate = decoded
    raise ValueError(
        'Malformed entity_id. Pass the search hit string unchanged, e.g. [["int",42]] '
        "or row-sha256:<64 hexadecimal characters>, with no extra quoting or escaping."
    )


def quote_identifier(value: str) -> str:
    """Quote a warehouse identifier without permitting SQL delimiters."""
    if not value or any(char in value for char in "`\\\x00\n\r"):
        raise ValueError("Invalid warehouse identifier in the connection catalog.")
    return f"`{value}`"


def key_where(entity_id: str, primary_key: list[list[str]]) -> str:
    """Decode typed primary-key parts into a safely quoted BigQuery predicate."""
    try:
        parts = json.loads(entity_id)
    except (ValueError, TypeError) as error:
        raise ValueError(
            "Malformed entity_id. Use the unchanged key from search results."
        ) from error
    if not primary_key or not isinstance(parts, list) or len(parts) != len(primary_key):
        raise ValueError("Malformed entity_id: key parts do not match the stream primary key.")
    predicates: list[str] = []
    for path, part in zip(primary_key, parts, strict=True):
        if not path or not isinstance(part, list) or len(part) != 2:  # noqa: PLR2004
            raise ValueError("Malformed entity_id: expected typed primary-key pairs.")
        kind, value = part
        column = ".".join(quote_identifier(name) for name in path)
        if (
            isinstance(kind, str)
            and kind in {"str", "string", "date", "datetime", "time", "Decimal"}
            and isinstance(value, str)
        ):
            literal = json.dumps(value, ensure_ascii=True)
            casts = {
                "date": "DATE",
                "datetime": "TIMESTAMP",
                "time": "TIME",
                "Decimal": "BIGNUMERIC",
            }
            if kind in casts:
                literal = f"CAST({literal} AS {casts[kind]})"
        elif kind == "bool" and isinstance(value, bool):
            literal = "TRUE" if value else "FALSE"
        elif (kind == "int" and type(value) is int) or (
            kind == "float" and type(value) in {int, float} and math.isfinite(value)
        ):
            literal = str(value)
        else:
            raise ValueError(
                "Malformed or unsupported entity_id key type. Use a scalar primary key."
            )
        predicates.append(f"{column} = {literal}")
    return " AND ".join(predicates)


def compact_record(value: Any) -> Any:  # noqa: ANN401
    """Remove metadata and empty values, retaining false and zero."""
    if isinstance(value, dict):
        cleaned = {
            key: compact_record(item)
            for key, item in value.items()
            if not key.startswith("_airbyte_")
        }
        return {key: item for key, item in cleaned.items() if item not in (None, "", [], {})}
    if isinstance(value, list):
        return [
            cleaned for item in value if (cleaned := compact_record(item)) not in (None, "", [], {})
        ]
    return value


def canonical_row(value: Any) -> Any:  # noqa: ANN401, PLR0911
    """Apply Sonar fusion_batch's typed row-hash canonicalization."""
    if value is None:
        return ["null"]
    if isinstance(value, dict):
        return ["object", [[key, canonical_row(value[key])] for key in sorted(value)]]
    if isinstance(value, list):
        return ["array", [canonical_row(item) for item in value]]
    if isinstance(value, datetime):
        aware = value.tzinfo is not None
        normalized = value.astimezone(UTC).replace(tzinfo=None) if aware else value
        return [
            "datetime",
            normalized.isoformat(sep=" ", timespec="microseconds") + "000" + ("Z" if aware else ""),
        ]
    if isinstance(value, time):
        return ["time", value.isoformat(timespec="microseconds") + "000"]
    if isinstance(value, date):
        return ["date", value.isoformat()]
    if isinstance(value, Decimal):
        sign, digits, exponent = value.as_tuple()
        coefficient = "".join(str(digit) for digit in digits)
        significant = coefficient.rstrip("0")
        return (
            ["decimal", sign, significant, int(exponent) + len(coefficient) - len(significant)]
            if significant
            else ["decimal", 0, "0", 0]
        )
    if isinstance(value, float):
        return ["float", value.hex()]
    return [type(value).__name__, value]


def row_key(record: dict[str, Any]) -> str:
    """Hash data fields before compaction, including null and empty values."""
    data = {key: value for key, value in record.items() if not key.startswith("_airbyte_")}
    canonical = json.dumps(
        canonical_row(data), ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )
    return "row-sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def is_row_key(entity_id: str) -> bool:
    """Validate a keyless stream's row digest."""
    if entity_id.startswith("row-sha256:"):
        if not re.fullmatch(r"row-sha256:[0-9a-f]{64}", entity_id):
            raise ValueError(
                "Malformed entity_id: row-sha256 requires a 64-character hexadecimal digest."
            )
        return True
    return False
