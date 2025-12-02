"""Ingestion module for Arc client.

This module provides high-performance data ingestion for Arc using:
- MessagePack columnar format (recommended, 25-35% faster)
- MessagePack row format (legacy)
- InfluxDB Line Protocol (compatibility)
"""

from arc_client.ingestion.async_buffered import AsyncBufferedWriter
from arc_client.ingestion.async_writer import AsyncWriteClient
from arc_client.ingestion.buffered import BufferedWriter
from arc_client.ingestion.compression import compress_gzip, decompress_gzip, is_gzipped
from arc_client.ingestion.line_protocol import (
    format_columnar_as_lines,
    format_line_protocol,
    format_lines,
)
from arc_client.ingestion.msgpack import (
    dataframe_to_columnar,
    encode_batch,
    encode_columnar,
    encode_records,
    encode_single_record,
)
from arc_client.ingestion.writer import WriteClient

__all__ = [
    # Writers
    "WriteClient",
    "AsyncWriteClient",
    "BufferedWriter",
    "AsyncBufferedWriter",
    # MessagePack
    "encode_columnar",
    "encode_records",
    "encode_single_record",
    "encode_batch",
    "dataframe_to_columnar",
    # Line Protocol
    "format_line_protocol",
    "format_lines",
    "format_columnar_as_lines",
    # Compression
    "compress_gzip",
    "decompress_gzip",
    "is_gzipped",
]
