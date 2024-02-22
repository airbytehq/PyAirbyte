# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""SQL datasets class."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, cast

from overrides import overrides
from sqlalchemy import and_, func, select, text

from airbyte.datasets._base import DatasetBase


if TYPE_CHECKING:
    from collections.abc import Iterator

    from pandas import DataFrame
    from sqlalchemy import Selectable, Table
    from sqlalchemy.sql import ClauseElement

    from airbyte.caches.base import CacheBase


class SQLDataset(DatasetBase):
    """A dataset that is loaded incrementally from a SQL query.

    The CachedDataset class is a subclass of this class, which simply passes a SELECT over the full
    table as the query statement.
    """

    def __init__(
        self,
        cache: CacheBase,
        stream_name: str,
        query_statement: Selectable,
    ) -> None:
        self._length: int | None = None
        self._cache: CacheBase = cache
        self._stream_name: str = stream_name
        self._query_statement: Selectable = query_statement
        super().__init__()

    @property
    def stream_name(self) -> str:
        return self._stream_name

    def __iter__(self) -> Iterator[Mapping[str, Any]]:
        with self._cache.processor.get_sql_connection() as conn:
            for row in conn.execute(self._query_statement):
                # Access to private member required because SQLAlchemy doesn't expose a public API.
                # https://pydoc.dev/sqlalchemy/latest/sqlalchemy.engine.row.RowMapping.html
                yield cast(Mapping[str, Any], row._mapping)  # noqa: SLF001

    def __len__(self) -> int:
        """Return the number of records in the dataset.

        This method caches the length of the dataset after the first call.
        """
        if self._length is None:
            count_query = select([func.count()]).select_from(self._query_statement.alias())
            with self._cache.processor.get_sql_connection() as conn:
                self._length = conn.execute(count_query).scalar()

        return self._length

    def to_pandas(self) -> DataFrame:
        return self._cache.processor.get_pandas_dataframe(self._stream_name)

    def with_filter(self, *filter_expressions: ClauseElement | str) -> SQLDataset:
        """Filter the dataset by a set of column values.

        Filters can be specified as either a string or a SQLAlchemy expression.

        Filters are lazily applied to the dataset, so they can be chained together. For example:

                dataset.with_filter("id > 5").with_filter("id < 10")

        is equivalent to:

                dataset.with_filter("id > 5", "id < 10")
        """
        # Convert all strings to TextClause objects.
        filters: list[ClauseElement] = [
            text(expression) if isinstance(expression, str) else expression
            for expression in filter_expressions
        ]
        filtered_select = self._query_statement.where(and_(*filters))
        return SQLDataset(
            cache=self._cache,
            stream_name=self._stream_name,
            query_statement=filtered_select,
        )


class CachedDataset(SQLDataset):
    """A dataset backed by a SQL table cache.

    Because this dataset includes all records from the underlying table, we also expose the
    underlying table as a SQLAlchemy Table object.
    """

    def __init__(self, cache: CacheBase, stream_name: str) -> None:
        """We construct the query statement by selecting all columns from the table.

        This prevents the need to scan the table schema to construct the query statement.
        """
        table_name = cache.processor.get_sql_table_name(stream_name)
        schema_name = cache.schema_name
        query = select("*").select_from(text(f"{schema_name}.{table_name}"))
        super().__init__(
            cache=cache,
            stream_name=stream_name,
            query_statement=query,
        )

    @overrides
    def to_pandas(self) -> DataFrame:
        return self._cache.processor.get_pandas_dataframe(self._stream_name)

    def to_sql_table(self) -> Table:
        if self._sql_table is None:
            self._sql_table: Table = self.cache.processor.get_sql_table(self.stream_name)

        return self._sql_table

    def __eq__(self, value: object) -> bool:
        """Return True if the value is a CachedDataset with the same cache and stream name.

        In the case of CachedDataset objects, we can simply compare the cache and stream name.

        Note that this equality check is only supported on CachedDataset objects and not for
        the base SQLDataset implementation. This is because of the complexity and computational
        cost of comparing two arbitrary SQL queries that could be bound to different variables,
        as well as the chance that two queries can be syntactically equivalent without being
        text-wise equivalent.
        """
        if not isinstance(value, SQLDataset):
            return False

        if self._cache is not value._cache:
            return False

        if self._stream_name != value._stream_name:
            return False

        return True
