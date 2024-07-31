# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Experimental features which may change.

> **NOTE:**
> The following "experimental" features are now "stable" and can be accessed directly from the
`airbyte.get_source()` method:
> - Docker sources, using the `docker_image` argument.
> - Yaml sources, using the `source_manifest` argument.

## About Experimental Features

Experimental features may change without notice between minor versions of PyAirbyte. Although rare,
they may also be entirely removed or refactored in future versions of PyAirbyte. Experimental
features may also be less stable than other features, and may not be as well-tested.

You can help improve this product by reporting issues and providing feedback for improvements in our
[GitHub issue tracker](https://github.com/airbytehq/pyairbyte/issues).
"""

from __future__ import annotations

from airbyte.sources.util import get_source


__all__ = [
    "get_source",
]
