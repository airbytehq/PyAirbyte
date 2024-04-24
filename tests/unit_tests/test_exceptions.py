# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import inspect
import pytest
import airbyte.exceptions as exceptions_module


def test_exceptions():
    exception_classes = [
        (name, obj)
        for name, obj in inspect.getmembers(exceptions_module)
        if inspect.isclass(obj) and name.endswith("Error")
    ]
    assert "AirbyteError" in [name for name, _ in exception_classes]
    assert "NotAnError" not in [name for name, _ in exception_classes]
    for name, obj in exception_classes:
        instance = obj()
        message = instance.get_message()
        assert isinstance(message, str), "No message for class: " + name
        assert message.count("\n") == 0
        assert message != ""
        assert message.strip() == message
        assert any([
            name.startswith(prefix) for prefix in ["Airbyte", "PyAirbyte"]
        ]), f"{name} does not start with Airbyte or PyAirbyte"
        assert name.endswith("Error")


if __name__ == "__main__":
    pytest.main()
