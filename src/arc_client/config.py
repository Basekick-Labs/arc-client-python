"""Arc client configuration."""

from __future__ import annotations

from typing import Optional, Union

from pydantic import BaseModel, Field, field_validator

from arc_client.compression import Compression


class ClientConfig(BaseModel):
    """Configuration for Arc client."""

    host: str = Field(default="localhost", description="Arc server hostname")
    port: int = Field(default=8000, description="Arc server port")
    token: Optional[str] = Field(default=None, description="API token for authentication")
    database: Optional[str] = Field(
        default=None,
        description="Database you chose for this client. Queries send it as "
        "x-arc-database only when set; writes and imports use 'default' when unset.",
    )
    timeout: float = Field(default=30.0, description="Request timeout in seconds")
    compression: Union[Compression, str, bool] = Field(
        default=Compression.ZSTD,
        description="Compression codec for writes (none/gzip/zstd). "
        "True is treated as gzip and False as none for backward compatibility.",
    )
    ssl: bool = Field(default=False, description="Use HTTPS")
    verify_ssl: bool = Field(default=True, description="Verify SSL certificates")
    max_retries: int = Field(
        default=3,
        ge=0,
        description="Max retries for read-only transient failures or mutation connection "
        "establishment failures. Data DELETE never retries. 0 disables retries.",
    )
    retry_backoff: float = Field(
        default=0.5,
        ge=0.0,
        description="Base backoff in seconds for exponential retry with jitter.",
    )

    send_installation_id: bool = Field(
        default=True,
        description="Send this installation's random id (Arc-Client-Installation-Id) so "
        "Arc's telemetry can count SDK installations. DO_NOT_TRACK=1 or "
        "ARC_CLIENT_SEND_INSTALLATION_ID=0 also disable it.",
    )

    @field_validator("compression")
    @classmethod
    def _coerce_compression(cls, value: Union[Compression, str, bool]) -> Compression:
        return Compression.coerce(value)

    @property
    def query_database(self) -> Optional[str]:
        """The database queries name in ``x-arc-database``, or None to send none.

        Only a database the caller chose. Arc rejects ``db.table`` names while
        the header is set, and without it resolves bare names to ``default``
        anyway, so sending the implicit default only broke ``db.table``
        queries (arc-client-python#35).
        """
        return self.database or None

    @property
    def default_database(self) -> str:
        """The database writes, imports and measurement reads use when none is given."""
        return self.database or "default"

    @property
    def base_url(self) -> str:
        """Get the base URL for the Arc server."""
        scheme = "https" if self.ssl else "http"
        return f"{scheme}://{self.host}:{self.port}"

    model_config = {"frozen": True}
