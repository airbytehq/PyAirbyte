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
uvx --from airbyte airbyte cloud --help
uvx --from airbyte airbyte local --help
uvx --from airbyte pyab --help
```

## CLI reference

The `airbyte cloud` command is documented in `airbyte.cli.cloud`.
The `airbyte local` command is documented in `airbyte.cli.local`.
The `pyab` and `pyairbyte` commands are backward-compatible aliases documented
in `airbyte.cli.pyab`.

Each `airbyte cloud` and `airbyte local` command group is documented in its own
submodule page below.
The reference content is regenerated locally via `poe docs-generate`; see
`docs/generate_cli.py`.
"""

from airbyte.cli import (
    cloud,
    local,
    pyab,
)
from airbyte.cli.pyab import cli


__all__ = [
    "cloud",
    "local",
    "pyab",
    "cli",
]
