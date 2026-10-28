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
    database: str = Field(default="default", description="Default database name")
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

    @field_validator("compression")
    @classmethod
    def _coerce_compression(cls, value: Union[Compression, str, bool]) -> Compression:
        return Compression.coerce(value)

    @property
    def base_url(self) -> str:
        """Get the base URL for the Arc server."""
        scheme = "https" if self.ssl else "http"
        return f"{scheme}://{self.host}:{self.port}"

    model_config = {"frozen": True}
