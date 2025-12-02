"""Compression utilities for Arc ingestion.

Arc supports gzip compression for ingestion payloads. Compressed payloads
are auto-detected by the magic bytes (0x1f 0x8b) at the start.
"""

from __future__ import annotations

import gzip
import io


def compress_gzip(data: bytes, level: int = 6) -> bytes:
    """Compress data using gzip.

    Args:
        data: Raw bytes to compress.
        level: Compression level (0-9). Default 6 is a good balance.
            0 = no compression, 9 = maximum compression.

    Returns:
        Gzip compressed bytes.

    Example:
        >>> raw = b'hello world'
        >>> compressed = compress_gzip(raw)
        >>> compressed[:2]  # Magic bytes
        b'\\x1f\\x8b'
    """
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=level) as f:
        f.write(data)
    return buf.getvalue()


def decompress_gzip(data: bytes) -> bytes:
    """Decompress gzip data.

    Args:
        data: Gzip compressed bytes.

    Returns:
        Decompressed bytes.

    Raises:
        ValueError: If data is not valid gzip.
    """
    if not is_gzipped(data):
        raise ValueError("Data is not gzip compressed")

    buf = io.BytesIO(data)
    with gzip.GzipFile(fileobj=buf, mode="rb") as f:
        return f.read()


def is_gzipped(data: bytes) -> bool:
    """Check if data is gzip compressed.

    Gzip data starts with magic bytes 0x1f 0x8b.

    Args:
        data: Bytes to check.

    Returns:
        True if data appears to be gzip compressed.
    """
    return len(data) >= 2 and data[0] == 0x1F and data[1] == 0x8B
