# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""PyAirbyte LakeStorage class."""

from __future__ import annotations

import abc
import re
from abc import abstractmethod
from typing import TYPE_CHECKING

import pyarrow.dataset as ds
from pyarrow import fs


if TYPE_CHECKING:
    from pathlib import Path


class LakeStorage(abc.ABC):
    """PyAirbyte LakeStorage class."""

    def __init__(self) -> None:
        """Initialize LakeStorage base class."""
        self.short_name: str

    @property
    @abstractmethod
    def uri_protocol(self) -> str:
        """Return the URI protocol for the lake storage.

        E.g. "file://", "s3://", "gcs://", etc.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @property
    def root_storage_uri(self) -> str:
        """Get the root URI for the lake storage."""
        return f"{self.uri_protocol}{self.root_storage_path}/"

    @property
    def root_storage_path(self) -> str:
        """Get the root path for the lake storage."""
        return "airbyte/lake"

    def path_to_uri(self, path: str) -> str:
        """Convert a relative lake path to a URI."""
        return f"{self.root_storage_uri}{path}"

    def get_stream_root_path(
        self,
        stream_name: str,
    ) -> str:
        """Get the path for a stream in the lake storage."""
        return f"{self.root_storage_path}/{stream_name}/"

    def get_stream_root_uri(
        self,
        stream_name: str,
    ) -> str:
        """Get the URI root for a stream in the lake storage."""
        return self.path_to_uri(self.get_stream_root_path(stream_name))

    @abstractmethod
    def write_dataset(
        self,
        dataset: ds.Dataset,
        table_name: str,
        schema: str,
        cache_dir: Path,
        cleanup: bool,  # noqa: FBT001
    ) -> None:
        """Write an Arrow dataset to the lake storage."""
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def read_dataset(
        self,
        table_name: str,
        schema: str,
        cache_dir: Path,
        cleanup: bool,  # noqa: FBT001
    ) -> ds.Dataset:
        """Read an Arrow dataset from the lake storage."""
        raise NotImplementedError("Subclasses must implement this method.")

    def _validate_short_name(self, short_name: str) -> str:
        """Validate that short_name is lowercase snake_case with no special characters."""
        if not re.match(r"^[a-z][a-z0-9_]*$", short_name):
            raise ValueError(
                f"short_name '{short_name}' must be lowercase snake_case with no special characters"
            )
        return short_name

    def get_artifact_prefix(self) -> str:
        """Get the artifact prefix for this lake storage."""
        return f"AIRBYTE_LAKE_{self.short_name.upper()}_"


class S3LakeStorage(LakeStorage):
    """S3 Lake Storage implementation."""

    def __init__(
        self,
        bucket_name: str,
        region: str,
        access_key_id: str,
        secret_access_key: str,
        short_name: str = "s3",
    ) -> None:
        """Initialize S3LakeStorage with required parameters."""
        self.bucket_name = bucket_name
        self.region = region
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.short_name = self._validate_short_name(short_name)

    @property
    def uri_protocol(self) -> str:
        """Return the URI protocol for S3."""
        return "s3://"

    @property
    def root_storage_uri(self) -> str:
        """Get the root URI for the S3 lake storage."""
        return f"{self.uri_protocol}{self.bucket_name}/"

    def write_dataset(
        self,
        dataset: ds.Dataset,
        table_name: str,
        schema: str,  # noqa: ARG002
        cache_dir: Path,  # noqa: ARG002
        cleanup: bool,  # noqa: ARG002, FBT001
    ) -> None:
        """Write an Arrow dataset to S3 as Parquet files."""
        s3_filesystem = fs.S3FileSystem(
            access_key=self.access_key_id,
            secret_key=self.secret_access_key,
            region=self.region,
        )

        output_path = f"{self.bucket_name}/airbyte_data/{table_name}"

        ds.write_dataset(
            dataset,
            output_path,
            filesystem=s3_filesystem,
            format="parquet",
            partitioning=None,
            existing_data_behavior="overwrite_or_ignore",
        )

    def read_dataset(
        self,
        table_name: str,
        schema: str,  # noqa: ARG002
        cache_dir: Path,  # noqa: ARG002
        cleanup: bool,  # noqa: ARG002, FBT001
    ) -> ds.Dataset:
        """Read an Arrow dataset from S3 Parquet files."""
        s3_filesystem = fs.S3FileSystem(
            access_key=self.access_key_id,
            secret_key=self.secret_access_key,
            region=self.region,
        )

        input_path = f"{self.bucket_name}/airbyte_data/{table_name}"

        return ds.dataset(
            input_path,
            filesystem=s3_filesystem,
            format="parquet",
        )


class GCSLakeStorage(LakeStorage):
    """Google Cloud Storage Lake Storage implementation."""

    def __init__(
        self, bucket_name: str, credentials_path: str | None = None, short_name: str = "gcs"
    ) -> None:
        """Initialize GCSLakeStorage with required parameters."""
        self.bucket_name = bucket_name
        self.credentials_path = credentials_path
        self.short_name = self._validate_short_name(short_name)

    @property
    def uri_protocol(self) -> str:
        """Return the URI protocol for GCS."""
        return "gs://"

    @property
    def root_storage_uri(self) -> str:
        """Get the root URI for the GCS lake storage."""
        return f"{self.uri_protocol}{self.bucket_name}/"

    def write_dataset(
        self,
        dataset: ds.Dataset,
        table_name: str,
        schema: str,  # noqa: ARG002
        cache_dir: Path,  # noqa: ARG002
        cleanup: bool,  # noqa: ARG002, FBT001
    ) -> None:
        """Write an Arrow dataset to GCS as Parquet files."""
        gcs_filesystem = fs.GcsFileSystem()

        output_path = f"{self.bucket_name}/airbyte_data/{table_name}"

        ds.write_dataset(
            dataset,
            output_path,
            filesystem=gcs_filesystem,
            format="parquet",
            partitioning=None,
            existing_data_behavior="overwrite_or_ignore",
        )

    def read_dataset(
        self,
        table_name: str,
        schema: str,  # noqa: ARG002
        cache_dir: Path,  # noqa: ARG002
        cleanup: bool,  # noqa: ARG002, FBT001
    ) -> ds.Dataset:
        """Read an Arrow dataset from GCS Parquet files."""
        gcs_filesystem = fs.GcsFileSystem()

        input_path = f"{self.bucket_name}/airbyte_data/{table_name}"

        return ds.dataset(
            input_path,
            filesystem=gcs_filesystem,
            format="parquet",
        )


__all__ = [
    "LakeStorage",
    "S3LakeStorage",
    "GCSLakeStorage",
]
