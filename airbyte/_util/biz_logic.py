# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Business logic utilities for PyAirbyte.

This module contains business logic functions that enforce policies and rules
for various operations in PyAirbyte.
"""

from __future__ import annotations


def validate_version_override_permission(
    *,
    current_version: str,
    new_version: str | None,
    is_setting_override: bool,
    override_reason: str,
    user_email: str,
    existing_override_creator_email: str | None = None,
) -> tuple[bool, str]:
    """Validate whether a version override operation should be permitted.

    This function encapsulates the business logic for determining when version
    overrides should be allowed. It can be refined iteratively based on feedback
    from the team.

    Args:
        current_version: The current version string (e.g., "0.1.0", "0.1.0-dev", "0.1.0-rc")
        new_version: The version to pin to (None if unsetting)
        is_setting_override: True if setting an override, False if clearing
        override_reason: The reason provided for the override
        user_email: Email of the user performing the operation
        existing_override_creator_email: Email of who created the existing override (if any)

    Returns:
        A tuple of (is_permitted, reason_if_not_permitted)
        - is_permitted: True if the operation should be allowed
        - reason_if_not_permitted: Empty string if permitted, otherwise an explanation

    Business Rules (to be refined):
        1. Dev versions (-dev suffix):
           - Only the creator can unpin their own dev version override
           - Anyone can pin to a dev version (for testing)

        2. Release candidates (-rc suffix):
           - Any admin can pin/unpin RC versions
           - These are for pre-release testing

        3. Production versions (no suffix):
           - Should require strong justification
           - Override reason must clearly indicate customer request or support investigation
           - Consider requiring additional approval or stricter validation

    Note:
        This is a placeholder implementation. The actual business logic should be
        refined with input from Catherine Noll and other stakeholders.
    """

    def _get_version_suffix(version: str) -> str | None:
        """Extract suffix from version string (e.g., '-dev', '-rc', or None for prod)."""
        if "-dev" in version:
            return "-dev"
        if "-rc" in version:
            return "-rc"
        return None

    current_suffix = _get_version_suffix(current_version)
    new_suffix = _get_version_suffix(new_version) if new_version else None

    if (
        current_suffix == "-dev"
        and not is_setting_override
        and existing_override_creator_email
        and user_email != existing_override_creator_email
    ):
        return (
            False,
            f"Cannot unpin dev version override created by {existing_override_creator_email}. "
            "Only the creator can remove their own dev version pins.",
        )

    if is_setting_override and new_suffix is None:
        reason_lower = override_reason.lower()
        valid_keywords = ["customer", "support", "investigation", "requested", "incident"]
        if not any(keyword in reason_lower for keyword in valid_keywords):
            return (
                False,
                "Pinning to a production version requires strong justification. "
                "Override reason must mention customer request, support investigation, "
                "or similar critical need. "
                "Keywords: customer, support, investigation, requested, incident",
            )

    if not is_setting_override and current_suffix is None:
        reason_lower = override_reason.lower()
        valid_keywords = ["resolved", "fixed", "safe", "tested", "approved"]
        if not any(keyword in reason_lower for keyword in valid_keywords):
            return (
                False,
                "Clearing a production version override requires justification. "
                "Override reason must indicate the issue is resolved or it's safe to revert. "
                "Keywords: resolved, fixed, safe, tested, approved",
            )

    return (True, "")
