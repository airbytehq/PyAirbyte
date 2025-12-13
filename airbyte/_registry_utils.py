# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""@private Utility functions for working with the Airbyte connector registry."""

import logging
import re

import requests


logger = logging.getLogger("airbyte.registry")


def parse_changelog_html(  # noqa: PLR0914
    html_content: str, connector_name: str
) -> list[dict[str, str | list[str] | None]]:
    """Parse changelog HTML to extract version history.

    Returns a list of dicts with keys: version, release_date, docker_image_url,
    changelog_url, pr_url, pr_title, parsing_errors.
    """
    versions: list[dict[str, str | list[str] | None]] = []

    connector_type = "sources" if connector_name.startswith("source-") else "destinations"
    connector_short_name = connector_name.replace("source-", "").replace("destination-", "")

    changelog_url = (
        f"https://docs.airbyte.com/integrations/{connector_type}/{connector_short_name}#changelog"
    )

    row_pattern = re.compile(
        r"<tr><td[^>]*>([^<]+)<td[^>]*>([^<]+)<td[^>]*>(.*?)<td[^>]*>(.*?)<tr>", re.DOTALL
    )

    pr_pattern = re.compile(
        r"<a href=https://github\.com/airbytehq/airbyte/pull/(\d+)[^>]*>(\d+)</a>"
    )

    for match in row_pattern.finditer(html_content):
        version = match.group(1).strip()
        date = match.group(2).strip()
        pr_cell = match.group(3)
        subject = match.group(4).strip()

        if not re.match(r"\d{4}-\d{2}-\d{2}", date):
            continue

        pr_matches = list(pr_pattern.finditer(pr_cell))
        pr_url = None
        pr_title = None
        parsing_errors = []

        if pr_matches:
            first_pr = pr_matches[0]
            pr_number = first_pr.group(1)
            pr_url = f"https://github.com/airbytehq/airbyte/pull/{pr_number}"

            pr_title = re.sub(r"<[^>]+>", "", subject)
            pr_title = pr_title.replace("&quot;", '"').replace("&amp;", "&")
            pr_title = pr_title.replace("&lt;", "<").replace("&gt;", ">")
            pr_title = pr_title.strip()

            if len(pr_matches) > 1:
                parsing_errors.append(
                    f"Multiple PRs found for version {version}, using first PR: {pr_number}"
                )
        else:
            parsing_errors.append(f"No PR link found in changelog for version {version}")

        docker_image_url = f"https://hub.docker.com/r/airbyte/{connector_name}/tags?name={version}"

        versions.append(
            {
                "version": version,
                "release_date": date or None,
                "docker_image_url": docker_image_url,
                "changelog_url": changelog_url,
                "pr_url": pr_url,
                "pr_title": pr_title,
                "parsing_errors": parsing_errors,
            }
        )

    return versions


def fetch_registry_version_date(connector_name: str, version: str) -> str | None:
    """Fetch the release date for a specific version from the registry.

    Returns the release date string (YYYY-MM-DD) if found, None otherwise.
    """
    try:  # noqa: PLR1702
        registry_url = "https://connectors.airbyte.com/files/registries/v0/oss_registry.json"
        response = requests.get(registry_url, timeout=10)
        response.raise_for_status()
        registry_data = response.json()

        connector_list = registry_data.get("sources", []) + registry_data.get("destinations", [])

        for connector in connector_list:
            docker_repo = connector.get("dockerRepository", "")
            if docker_repo == f"airbyte/{connector_name}":
                releases = connector.get("releases", {})
                release_candidates = releases.get("releaseCandidates", {})

                if version in release_candidates:
                    version_data = release_candidates[version]
                    generated = version_data.get("generated", {})
                    git_info = generated.get("git", {})
                    commit_timestamp = git_info.get("commit_timestamp")

                    if commit_timestamp:
                        date_match = re.match(r"(\d{4}-\d{2}-\d{2})", commit_timestamp)
                        if date_match:
                            return date_match.group(1)

                break
        else:
            return None
    except Exception as e:
        logger.debug(f"Failed to fetch registry date for {connector_name} v{version}: {e}")
    return None
