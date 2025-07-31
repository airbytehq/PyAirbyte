# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""PyAirbyte LakeStorage class."""
from __future__ import annotations

import abc
from abc import abstractproperty


class LakeStorage(abc.ABC):
    """PyAirbyte LakeStorage class."""

    @abstractproperty
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


class S3LakeStorage(LakeStorage):
    """S3 Lake Storage implementation."""

    def __init__(self, bucket_name: str, region: str, access_key_id: str, secret_access_key: str):
        """Initialize S3LakeStorage with required parameters."""
        self.bucket_name = bucket_name
        self.region = region
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key

    @property
    def uri_protocol(self) -> str:
        """Return the URI protocol for S3."""
        return "s3://"

    @property
    def root_storage_uri(self) -> str:
        """Get the root URI for the S3 lake storage."""
        return f"{self.uri_protocol}{self.bucket_name}/"
