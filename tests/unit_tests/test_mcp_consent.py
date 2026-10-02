# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for best-effort destructive-action consent prompts."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from airbyte.mcp._consent import request_destructive_consent
from fastmcp import Client, Context, FastMCP
from fastmcp.client.elicitation import ElicitResult
from mcp.types import InputRequiredResult


def _build_app() -> FastMCP:
    app = FastMCP("consent-test")

    @app.tool
    def delete_thing(ctx: Context, name: str) -> str | InputRequiredResult:
        consent = request_destructive_consent(ctx, f"Permanently delete '{name}'?")
        if isinstance(consent, InputRequiredResult):
            return consent
        return "deleted" if consent else "not confirmed"

    return app


def _call_delete_thing(mode: str, **client_kwargs: Any) -> str:
    async def call() -> str:
        async with Client(_build_app(), mode=mode, **client_kwargs) as client:
            result = await client.call_tool("delete_thing", {"name": "delete-me"})
        return result.content[0].text

    return asyncio.run(call())


def _handler(answer: dict[str, Any] | ElicitResult, prompts: list[str]):
    async def handler(message: str, *_: Any) -> dict[str, Any] | ElicitResult:  # noqa: RUF029
        prompts.append(message)
        return answer

    return handler


@pytest.mark.parametrize("mode", ["auto", "legacy"])
@pytest.mark.parametrize(
    "answer,expected",
    [
        pytest.param("accept", "deleted", id="accept"),
        pytest.param("decline", "not confirmed", id="decline"),
        pytest.param("cancel", "not confirmed", id="cancel"),
    ],
)
def test_consent_prompt_respects_user_answer(
    mode: str, answer: str, expected: str
) -> None:
    """Capable clients are prompted and the user's answer decides the outcome."""
    prompts: list[str] = []
    if answer == "accept":
        response: dict[str, Any] | ElicitResult = {"confirm": True}
    else:
        response = ElicitResult(action=answer)

    result = _call_delete_thing(mode, elicitation_handler=_handler(response, prompts))

    assert prompts == ["Permanently delete 'delete-me'?"]
    assert result == expected


@pytest.mark.parametrize("mode", ["auto", "legacy"])
def test_consent_fails_open_without_elicitation_support(mode: str) -> None:
    """Clients without elicitation support proceed without a prompt."""
    assert _call_delete_thing(mode) == "deleted"


def test_consent_fails_open_without_request_context() -> None:
    """Direct calls outside an MCP request proceed without a prompt."""
    assert (
        request_destructive_consent(Context(FastMCP("consent-test")), "Delete?") is True
    )
