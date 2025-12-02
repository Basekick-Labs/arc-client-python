"""Arc client configuration."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class ClientConfig(BaseModel):
    """Configuration for Arc client."""

    host: str = Field(default="localhost", description="Arc server hostname")
    port: int = Field(default=8000, description="Arc server port")
    token: Optional[str] = Field(default=None, description="API token for authentication")
    database: str = Field(default="default", description="Default database name")
    timeout: float = Field(default=30.0, description="Request timeout in seconds")
    compression: bool = Field(default=True, description="Enable gzip compression for writes")
    ssl: bool = Field(default=False, description="Use HTTPS")
    verify_ssl: bool = Field(default=True, description="Verify SSL certificates")

    @property
    def base_url(self) -> str:
        """Get the base URL for the Arc server."""
        scheme = "https" if self.ssl else "http"
        return f"{scheme}://{self.host}:{self.port}"

    model_config = {"frozen": True}
