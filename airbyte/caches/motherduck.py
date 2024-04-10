"""A MotherDuck implementation of the PyAirbyte cache, built on DuckDB.

## Usage Example

```python
from airbyte as ab
from airbyte.caches import MotherDuckCache

cache = MotherDuckCache(
    database="mydatabase",
    schema_name="myschema",
    api_key=ab.get_secret("MOTHERDUCK_API_KEY"),
)
"""

from __future__ import annotations

from overrides import overrides
from pydantic import Field

from airbyte._processors.sql.motherduck import MotherDuckSqlProcessor
from airbyte.caches.duckdb import DuckDBCache
from airbyte.secrets import SecretString


class MotherDuckCache(DuckDBCache):
    """Cache that uses MotherDuck for external persistent storage."""

    db_path: str = Field(default="md:")
    database: str
    api_key: SecretString

    _sql_processor_class = MotherDuckSqlProcessor

    @overrides
    def get_sql_alchemy_url(self) -> SecretString:
        """Return the SQLAlchemy URL to use."""
        return SecretString(
            f"duckdb:///md:{self.database}?motherduck_token={self.api_key}"
            # f"&schema={self.schema_name}"  # TODO: Debug why this doesn't work
        )

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database."""
        return self.database
