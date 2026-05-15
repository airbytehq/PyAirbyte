# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""CLI modules for PyAirbyte.

## Installation

**Prerequisites:** Install [uv](https://docs.astral.sh/uv/) (the fast Python
package manager):

```bash
brew install uv
```

**Install the CLIs** as persistent tools:

```bash
uv tool install airbyte
```

**Run without installing**:

```bash
uvx --from airbyte airbyte --help
uvx --from airbyte pyab --help
```

## CLI reference

The `airbyte` command is documented in `airbyte.cli.cloud_cli`.
The `pyab` and `pyairbyte` commands are documented in `airbyte.cli.pyab`.

Each `airbyte` command group is documented in its own submodule page below.
The reference content is regenerated locally via `poe docs-generate`; see
`docs/generate_cli.py`.
"""

from airbyte.cli import (
    cloud_cli,
    connections,
    destinations,
    jobs,
    pyab,
    sources,
    workspaces,
)
from airbyte.cli.pyab import cli


__all__ = [
    "cloud_cli",
    "connections",
    "destinations",
    "jobs",
    "pyab",
    "sources",
    "workspaces",
    "cli",
]
