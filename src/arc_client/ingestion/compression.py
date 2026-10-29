"""Compression utilities for Arc ingestion (compatibility shim).

The canonical implementation now lives in :mod:`arc_client.compression`. This
module re-exports it so existing imports from ``arc_client.ingestion.compression``
keep working.
"""

from __future__ import annotations

from arc_client.compression import (
    Compression,
    compress,
    compress_gzip,
    compress_zstd,
    decompress_gzip,
    decompress_zstd,
    is_gzipped,
    is_zstd,
)

__all__ = [
    "Compression",
    "compress",
    "compress_gzip",
    "compress_zstd",
    "decompress_gzip",
    "decompress_zstd",
    "is_gzipped",
    "is_zstd",
]
