# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from typing import Any, cast

from pandas import DataFrame

from airbyte.documents import Document


class DatasetBase(ABC):
    """Base implementation for all datasets."""

    @abstractmethod
    def __iter__(self) -> Iterator[Mapping[str, Any]]:
        """Return the iterator of records."""
        raise NotImplementedError

    def to_pandas(self) -> DataFrame:
        """Return a pandas DataFrame representation of the dataset.

        The base implementation simply passes the record iterator to Panda's DataFrame constructor.
        """
        # Technically, we return an iterator of Mapping objects. However, pandas
        # expects an iterator of dict objects. This cast is safe because we know
        # duck typing is correct for this use case.
        return DataFrame(cast(Iterator[dict[str, Any]], self))

    def to_documents(self) -> Iterator[Document]:
        """Return the iterator of documents."""
        return Document.from_records(self.__iter__())
