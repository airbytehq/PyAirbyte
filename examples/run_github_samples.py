# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A simple test of PyAirbyte, using the Faker source connector.

Usage (from PyAirbyte root directory):
> poetry run python ./examples/run_github_samples.py

No setup is needed, but you may need to delete the .venv-source-faker folder
if your installation gets interrupted or corrupted.
"""

from __future__ import annotations

import airbyte as ab

# Create a token here: https://github.com/settings/tokens
GITHUB_TOKEN = ab.get_secret("GITHUB_PERSONAL_ACCESS_TOKEN")

FAILING_STREAMS = [
    "collaborators",
    "issue_timeline_events",  # key error: 'converted_to_discussion'
    "projects_v2",
    "team_members",
    "team_memberships",
    "teams",
]

source = ab.get_source(
    "source-github",
    config={
        "repositories": ["airbytehq/airbyte-lib-private-beta"],
        "credentials": {"personal_access_token": GITHUB_TOKEN},
    },
    streams="*",
)

streams = list(set(source.get_available_streams()) - set(FAILING_STREAMS))

source.print_samples(streams=streams)
