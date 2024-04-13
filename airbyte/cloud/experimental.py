# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Experimental features for interacting with the Airbyte Cloud API.

You can use this module to access experimental features in Airbyte Cloud, OSS, and Enterprise. These
features are subject to change and may not be available in all environments. **Future versions of
PyAirbyte may remove or change these features without notice.**

To use this module, replace an import like this:

```python
from airbyte.cloud import CloudConnection, CloudWorkspace
```

with an import like this:

```python
from airbyte.cloud.experimental import CloudConnection, CloudWorkspace
```

You can toggle between the stable and experimental versions of these classes by changing the import
path. This allows you to test new features without requiring substantial changes to your codebase.

"""
# ruff: noqa: SLF001  # This file accesses private members of other classes.

from __future__ import annotations

import warnings

from airbyte.cloud.connections import CloudConnection as Stable_CloudConnection
from airbyte.cloud.workspaces import CloudWorkspace as Stable_CloudWorkspace


# This module is not imported anywhere by default, so this warning should only print if the user
# explicitly imports it.
warnings.warn(
    message="The `airbyte.cloud.experimental` module is experimental and may change in the future.",
    category=FutureWarning,
    stacklevel=2,
)

# All experimental methods are now stable.
CloudConnection = Stable_CloudConnection
CloudWorkspace = Stable_CloudWorkspace
