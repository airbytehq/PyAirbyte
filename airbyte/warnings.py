# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Warnings for the PyAirbyte library."""

from __future__ import annotations


class PyAirbyteDataLossWarning(Warning):
    """Warning for potential data loss.

    Users can ignore this warning by running:
    > warnings.filterwarnings("ignore", category="airbyte.exceptions.PyAirbyteDataLossWarning")
    """
