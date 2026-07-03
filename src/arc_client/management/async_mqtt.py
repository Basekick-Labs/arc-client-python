"""Asynchronous MQTT ingestion-management client for Arc.

Wraps ``/api/v1/mqtt`` and ``/api/v1/mqtt/subscriptions``. Requires an admin
token. When the MQTT subsystem is disabled the server returns 503.
"""

from __future__ import annotations

from typing import Any

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError
from arc_client.http.async_http import AsyncHTTPClient


class AsyncMQTTClient:
    """Manage MQTT ingestion subscriptions."""

    def __init__(self, http: AsyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    async def _json(self, method: str, path: str, **kwargs: Any) -> Any:
        try:
            response = await getattr(self._http, method)(path, **kwargs)
            return response.json()
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"MQTT request failed ({method.upper()} {path}): {e}") from e

    async def stats(self) -> Any:
        """Get MQTT subscription statistics."""
        return await self._json("get", "/api/v1/mqtt/stats")

    async def health(self) -> Any:
        """Get MQTT subsystem health."""
        return await self._json("get", "/api/v1/mqtt/health")

    # --- Subscriptions ---
    async def list_subscriptions(self) -> Any:
        """List MQTT subscriptions."""
        return await self._json("get", "/api/v1/mqtt/subscriptions/")

    async def get_subscription(self, subscription_id: str) -> Any:
        """Get a subscription by id."""
        return await self._json("get", f"/api/v1/mqtt/subscriptions/{subscription_id}")

    async def create_subscription(self, config: dict[str, Any]) -> Any:
        """Create a subscription.

        Args:
            config: The subscription body (name, broker, client_id, topics, qos,
                database, tls_*, auto_start, topic_mapping, etc.). See the Arc
                server docs for the full field list.
        """
        return await self._json("post", "/api/v1/mqtt/subscriptions/", json=config)

    async def update_subscription(self, subscription_id: str, config: dict[str, Any]) -> Any:
        """Update a subscription (fields are optional; 409 if it is running)."""
        return await self._json("put", f"/api/v1/mqtt/subscriptions/{subscription_id}", json=config)

    async def delete_subscription(self, subscription_id: str) -> Any:
        """Delete a subscription."""
        return await self._json("delete", f"/api/v1/mqtt/subscriptions/{subscription_id}")

    async def subscription_stats(self, subscription_id: str) -> Any:
        """Get statistics for a single subscription."""
        return await self._json("get", f"/api/v1/mqtt/subscriptions/{subscription_id}/stats")

    async def start_subscription(self, subscription_id: str) -> Any:
        """Start a subscription."""
        return await self._json("post", f"/api/v1/mqtt/subscriptions/{subscription_id}/start")

    async def stop_subscription(self, subscription_id: str) -> Any:
        """Stop a subscription."""
        return await self._json("post", f"/api/v1/mqtt/subscriptions/{subscription_id}/stop")

    async def pause_subscription(self, subscription_id: str) -> Any:
        """Pause a subscription."""
        return await self._json("post", f"/api/v1/mqtt/subscriptions/{subscription_id}/pause")

    async def restart_subscription(self, subscription_id: str) -> Any:
        """Restart a subscription."""
        return await self._json("post", f"/api/v1/mqtt/subscriptions/{subscription_id}/restart")
