# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import pytest

from airbyte.secrets.base import SecretSourceEnum
from airbyte.secrets.util import try_get_secret


def test_try_get_secret_rejects_source_keyword() -> None:
    with pytest.raises(TypeError, match="unexpected keyword argument 'source'"):
        try_get_secret("X", source=SecretSourceEnum.ENV)  # type: ignore[call-arg]
