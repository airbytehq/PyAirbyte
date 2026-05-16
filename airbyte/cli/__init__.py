# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""CLI modules for PyAirbyte.

## Installation

**Prerequisites:** Install [uv](https://docs.astral.sh/uv/) (the fast Python
package manager):

```bash
brew install uv
```

**Install the CLI** as a persistent tool:

```bash
uv tool install airbyte
```

**Run without installing**:

```bash
uvx airbyte cloud --help
uvx airbyte local --help
```

## CLI reference

The `airbyte cloud` command is documented in `airbyte.cli.cloud`.
The `airbyte local` command is documented in `airbyte.cli.local`.

Each `airbyte cloud` and `airbyte local` command group is documented in its own
submodule page below.
The reference content is regenerated locally via `poe docs-generate`; see
`docs/generate_cli.py`.
"""

from airbyte.cli import cloud, local
from airbyte.cli._cli import app, main


__all__ = [
    "app",
    "cloud",
    "local",
    "main",
]
