"""Unit tests for compression utilities."""

from __future__ import annotations

import pytest

from arc_client.ingestion.compression import compress_gzip, decompress_gzip, is_gzipped


class TestCompression:
    """Tests for compression utilities."""

    def test_compress_and_decompress(self) -> None:
        """Test basic compression and decompression."""
        original = b"hello world " * 100
        compressed = compress_gzip(original)
        decompressed = decompress_gzip(compressed)

        assert decompressed == original
        assert len(compressed) < len(original)

    def test_is_gzipped_true(self) -> None:
        """Test detection of gzipped data."""
        data = compress_gzip(b"test data")
        assert is_gzipped(data) is True

    def test_is_gzipped_false(self) -> None:
        """Test detection of non-gzipped data."""
        assert is_gzipped(b"hello world") is False
        assert is_gzipped(b"") is False
        assert is_gzipped(b"\x00\x00") is False

    def test_gzip_magic_bytes(self) -> None:
        """Test that gzip output starts with magic bytes."""
        compressed = compress_gzip(b"test")
        assert compressed[:2] == b"\x1f\x8b"

    def test_decompress_non_gzip_error(self) -> None:
        """Test that decompressing non-gzip data raises error."""
        with pytest.raises(ValueError, match="not gzip compressed"):
            decompress_gzip(b"not gzipped data")

    def test_compression_levels(self) -> None:
        """Test different compression levels."""
        data = b"test data " * 1000

        # Level 0 (no compression) should be larger than level 9
        level0 = compress_gzip(data, level=0)
        level9 = compress_gzip(data, level=9)

        assert len(level9) <= len(level0)

    def test_empty_data(self) -> None:
        """Test compression of empty data."""
        compressed = compress_gzip(b"")
        decompressed = decompress_gzip(compressed)
        assert decompressed == b""
