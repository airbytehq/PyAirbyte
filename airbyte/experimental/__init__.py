# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Experimental features which may change.

The experimental `get_source` implementation allows you to run sources
using Docker containers. This feature is still in development and may
change in the future.

To use this feature, import `get_source` from this module and use it in place of the `get_source`
function from the `airbyte` module.

Instead of this:

```python
from airbyte import ab

source = ab.get_source(...)
```

Use this:

```python
from airbyte.experimental import get_source

source = get_source(...)
```

Experimental features may change without notice between minor versions of PyAirbyte. Although rare,
they may also be entirely removed or refactored in future versions of PyAirbyte. Experimental
features may also be less stable than other features, and may not be as well-tested.

You can help improve this product by reporting issues and providing feedback for improvements in our
[GitHub issue tracker](https://github.com/airbytehq/pyairbyte/issues).
"""

from __future__ import annotations

from airbyte.sources.util import _get_source as get_source


__all__ = [
    "get_source",
]
