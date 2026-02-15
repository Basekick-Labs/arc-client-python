"""Arc Python SDK - High-performance client for Arc time-series database."""

from arc_client.async_client import AsyncArcClient
from arc_client.client import ArcClient
from arc_client.config import ClientConfig
from arc_client.exceptions import (
    ArcAuthenticationError,
    ArcConnectionError,
    ArcError,
    ArcIngestionError,
    ArcNotFoundError,
    ArcQueryError,
    ArcRateLimitError,
    ArcServerError,
    ArcValidationError,
)

__version__ = "0.1.2"

__all__ = [
    # Clients
    "ArcClient",
    "AsyncArcClient",
    # Config
    "ClientConfig",
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
    # Version
    "__version__",
]
