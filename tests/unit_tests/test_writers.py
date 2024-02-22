# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

from pathlib import Path
import pytest
from airbyte._processors.file.base import FileWriterBase, FileWriterBatchHandle, FileWriterConfigBase
from airbyte._processors.file.parquet import ParquetWriter, ParquetWriterConfig


def test_parquet_writer_config_initialization():
    config = ParquetWriterConfig(cache_dir='test_path')
    assert config.cache_dir == Path('test_path')

def test_parquet_writer_config_inheritance():
    assert issubclass(ParquetWriterConfig, FileWriterConfigBase)

def test_parquet_writer_inheritance():
    assert issubclass(ParquetWriter, FileWriterBase)
