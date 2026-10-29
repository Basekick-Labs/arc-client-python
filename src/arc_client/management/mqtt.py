"""Synchronous MQTT ingestion-management client for Arc.

Wraps ``/api/v1/mqtt`` and ``/api/v1/mqtt/subscriptions``. Requires an admin
token. When the MQTT subsystem is disabled the server returns 503.
"""

from __future__ import annotations

from typing import Any

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcError
from arc_client.http.sync_http import SyncHTTPClient


class MQTTClient:
    """Manage MQTT ingestion subscriptions."""

    def __init__(self, http: SyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    def _json(self, method: str, path: str, **kwargs: Any) -> Any:
        try:
            response = getattr(self._http, method)(path, **kwargs)
            return response.json()
        except ArcError:
            raise
        except Exception as e:
            raise ArcError(f"MQTT request failed ({method.upper()} {path}): {e}") from e

    def stats(self) -> Any:
        """Get MQTT subscription statistics."""
        return self._json("get", "/api/v1/mqtt/stats")

    def health(self) -> Any:
        """Get MQTT subsystem health."""
        return self._json("get", "/api/v1/mqtt/health")

    # --- Subscriptions ---
    def list_subscriptions(self) -> Any:
        """List MQTT subscriptions."""
        return self._json("get", "/api/v1/mqtt/subscriptions/")

    def get_subscription(self, subscription_id: str) -> Any:
        """Get a subscription by id."""
        return self._json("get", f"/api/v1/mqtt/subscriptions/{subscription_id}")

    def create_subscription(self, config: dict[str, Any]) -> Any:
        """Create a subscription.

        Args:
            config: The subscription body (name, broker, client_id, topics, qos,
                database, tls_*, auto_start, topic_mapping, etc.). See the Arc
                server docs for the full field list.
        """
        return self._json("post", "/api/v1/mqtt/subscriptions/", json=config)

    def update_subscription(self, subscription_id: str, config: dict[str, Any]) -> Any:
        """Update a subscription (fields are optional; 409 if it is running)."""
        return self._json("put", f"/api/v1/mqtt/subscriptions/{subscription_id}", json=config)

    def delete_subscription(self, subscription_id: str) -> Any:
        """Delete a subscription."""
        return self._json("delete", f"/api/v1/mqtt/subscriptions/{subscription_id}")

    def subscription_stats(self, subscription_id: str) -> Any:
        """Get statistics for a single subscription."""
        return self._json("get", f"/api/v1/mqtt/subscriptions/{subscription_id}/stats")

    def start_subscription(self, subscription_id: str) -> Any:
        """Start a subscription."""
        return self._json("post", f"/api/v1/mqtt/subscriptions/{subscription_id}/start")

    def stop_subscription(self, subscription_id: str) -> Any:
        """Stop a subscription."""
        return self._json("post", f"/api/v1/mqtt/subscriptions/{subscription_id}/stop")

    def pause_subscription(self, subscription_id: str) -> Any:
        """Pause a subscription."""
        return self._json("post", f"/api/v1/mqtt/subscriptions/{subscription_id}/pause")

    def restart_subscription(self, subscription_id: str) -> Any:
        """Restart a subscription."""
        return self._json("post", f"/api/v1/mqtt/subscriptions/{subscription_id}/restart")
