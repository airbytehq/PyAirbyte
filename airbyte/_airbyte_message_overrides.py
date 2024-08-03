# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Custom Airbyte message classes.

These classes override the default handling, in order to ensure that the data field is always a
jsonified string, rather than a dict.

To use these classes, import them from this module, and use them in place of the default classes.

Example:
```python
from airbyte._airbyte_message_overrides import AirbyteMessageWithStrData

for line in sys.stdin:
    message = AirbyteMessageWithStrData.model_validate_json(line)
```
"""

from __future__ import annotations

import copy
import json
from typing import Any

from pydantic import BaseModel, Field, model_validator

from airbyte_protocol.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
)


AirbyteRecordMessageWithStrData = copy.deepcopy(AirbyteRecordMessage)
AirbyteStateMessageWithStrData = copy.deepcopy(AirbyteStateMessage)
AirbyteMessageWithStrData = copy.deepcopy(AirbyteMessage)

# Modify the data field in the copied class
AirbyteRecordMessageWithStrData.__annotations__["data"] = str
AirbyteStateMessageWithStrData.__annotations__["data"] = str

AirbyteRecordMessageWithStrData.data = Field(..., description="jsonified record data as a str")
AirbyteStateMessageWithStrData.data = Field(..., description="jsonified state data as a str")


# Add a validator to ensure data is a JSON string
@model_validator(mode="before")
def ensure_data_is_string(
    cls: BaseModel,  # type: ignore  # noqa: ARG001, PGH003
    values: dict[str, Any],
) -> None:
    if "data" in values and not isinstance(values["data"], dict):
        values["data"] = json.dumps(values["data"])
    if "data" in values and not isinstance(values["data"], str):
        raise ValueError


AirbyteRecordMessageWithStrData.ensure_data_is_string = classmethod(ensure_data_is_string)  # type: ignore [arg-type]
AirbyteStateMessageWithStrData.ensure_data_is_string = classmethod(ensure_data_is_string)  # type: ignore [arg-type]

AirbyteMessageWithStrData.__annotations__["record"] = AirbyteRecordMessageWithStrData | None
AirbyteMessageWithStrData.__annotations__["state"] = AirbyteStateMessageWithStrData | None
