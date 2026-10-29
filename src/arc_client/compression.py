"""Compression utilities for Arc ingestion.

Arc auto-detects the compression codec of an ingestion payload from its leading
magic bytes (gzip = ``1f 8b``, zstd = ``28 b5 2f fd``) rather than from the
``Content-Encoding`` header, so clients should send the raw compressed bytes and
must **not** set a ``Content-Encoding`` header.

The server recommends **zstd** for writes; it is the client default.
"""

from __future__ import annotations

import gzip
import io
from enum import Enum


class Compression(str, Enum):
    """Compression codec used for ingestion payloads.

    Values are lowercase strings so they can be passed directly as config
    (``compression="zstd"``) or as enum members (``Compression.ZSTD``).
    """

    NONE = "none"
    GZIP = "gzip"
    ZSTD = "zstd"

    @classmethod
    def coerce(cls, value: Compression | str | bool) -> Compression:
        """Coerce a config value into a :class:`Compression`.

        Accepts a :class:`Compression`, a string name, or a bool for backward
        compatibility (``True`` -> gzip, ``False`` -> none). ``True`` maps to
        gzip rather than zstd to preserve the pre-0.2.0 default behavior.
        """
        if isinstance(value, Compression):
            return value
        if isinstance(value, bool):
            return cls.GZIP if value else cls.NONE
        if isinstance(value, str):
            try:
                return cls(value.lower())
            except ValueError as exc:
                raise ValueError(
                    f"Invalid compression {value!r}; expected one of {[c.value for c in cls]}"
                ) from exc
        raise TypeError(f"Cannot coerce {type(value).__name__} to Compression")


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


def compress_zstd(data: bytes, level: int = 3) -> bytes:
    """Compress data using zstandard.

    Args:
        data: Raw bytes to compress.
        level: Compression level (1-22). Default 3 is zstd's own default and a
            good speed/ratio balance for ingestion payloads.

    Returns:
        Zstd compressed bytes (magic ``28 b5 2f fd``).

    Raises:
        ImportError: If the ``zstandard`` package is not installed.
    """
    try:
        import zstandard
    except ImportError as exc:  # pragma: no cover - zstandard is a hard dep
        raise ImportError(
            "zstd compression requires the 'zstandard' package. "
            "Install it with: pip install zstandard"
        ) from exc

    return zstandard.ZstdCompressor(level=level).compress(data)


def decompress_zstd(data: bytes) -> bytes:
    """Decompress zstandard data.

    Args:
        data: Zstd compressed bytes.

    Returns:
        Decompressed bytes.

    Raises:
        ImportError: If the ``zstandard`` package is not installed.
        ValueError: If data is not valid zstd.
    """
    try:
        import zstandard
    except ImportError as exc:  # pragma: no cover - zstandard is a hard dep
        raise ImportError(
            "zstd decompression requires the 'zstandard' package. "
            "Install it with: pip install zstandard"
        ) from exc

    if not is_zstd(data):
        raise ValueError("Data is not zstd compressed")

    return zstandard.ZstdDecompressor().decompress(data)


def is_zstd(data: bytes) -> bool:
    """Check if data is zstd compressed.

    Zstd frames start with magic bytes 0x28 0xB5 0x2F 0xFD.

    Args:
        data: Bytes to check.

    Returns:
        True if data appears to be zstd compressed.
    """
    return (
        len(data) >= 4
        and data[0] == 0x28
        and data[1] == 0xB5
        and data[2] == 0x2F
        and data[3] == 0xFD
    )


def compress(data: bytes, codec: Compression) -> bytes:
    """Compress ``data`` with the given codec.

    Args:
        data: Raw bytes to compress.
        codec: The :class:`Compression` codec to apply.

    Returns:
        Compressed bytes, or ``data`` unchanged for :attr:`Compression.NONE`.
    """
    if codec is Compression.NONE:
        return data
    if codec is Compression.GZIP:
        return compress_gzip(data)
    if codec is Compression.ZSTD:
        return compress_zstd(data)
    raise ValueError(f"Unknown compression codec: {codec!r}")
