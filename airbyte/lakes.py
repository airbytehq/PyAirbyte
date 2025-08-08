# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""PyAirbyte LakeStorage class."""

from __future__ import annotations

import abc
import re
from abc import abstractmethod

from pydantic import BaseModel


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


class FastUnloadResult(BaseModel):
    """Results from a Fast Unload operation."""

    model_config = {"arbitrary_types_allowed": True}

    lake_store: LakeStorage
    lake_path_prefix: str
    table_name: str
    stream_name: str | None = None


class S3LakeStorage(LakeStorage):
    """S3 Lake Storage implementation."""

    def __init__(
        self,
        bucket_name: str,
        region: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        short_name: str = "s3",
    ) -> None:
        """Initialize S3LakeStorage with required parameters."""
        self.bucket_name = bucket_name
        self.region = region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.short_name = self._validate_short_name(short_name)

    @property
    def uri_protocol(self) -> str:
        """Return the URI protocol for S3."""
        return "s3://"

    @property
    def root_storage_uri(self) -> str:
        """Get the root URI for the S3 lake storage."""
        return f"{self.uri_protocol}{self.bucket_name}/"


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


__all__ = [
    "LakeStorage",
    "S3LakeStorage",
    "GCSLakeStorage",
    "FastUnloadResult",
]
