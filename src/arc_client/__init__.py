"""Arc Python SDK - High-performance client for Arc time-series database."""

# Defined before submodule imports so modules that read arc_client.__version__
# (e.g. the User-Agent header builder) can import it without a circular-import
# failure during package initialization.
__version__ = "0.2.0"

from arc_client.async_client import AsyncArcClient
from arc_client.client import ArcClient
from arc_client.compression import Compression
from arc_client.config import ClientConfig
from arc_client.exceptions import (
    ArcAuthenticationError,
    ArcConnectionError,
    ArcError,
    ArcIngestionError,
    ArcLicenseError,
    ArcNotFoundError,
    ArcNotSupportedError,
    ArcQueryError,
    ArcRateLimitError,
    ArcServerError,
    ArcValidationError,
)

__all__ = [
    # Clients
    "ArcClient",
    "AsyncArcClient",
    # Config
    "ClientConfig",
    "Compression",
    # Exceptions
    "ArcError",
    "ArcConnectionError",
    "ArcAuthenticationError",
    "ArcQueryError",
    "ArcIngestionError",
    "ArcValidationError",
    "ArcNotFoundError",
    "ArcRateLimitError",
    "ArcServerError",
    "ArcNotSupportedError",
    "ArcLicenseError",
    # Version
    "__version__",
]
