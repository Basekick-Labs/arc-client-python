"""Arc client exceptions."""

from __future__ import annotations

from typing import Any, Optional


class ArcError(Exception):
    """Base exception; HTTP failures retain structured diagnostics when available."""

    _response_body: Optional[dict[str, Any]] = None

    @property
    def response_body(self) -> Optional[dict[str, Any]]:
        """Retain diagnostics through existing higher-level exception wrappers."""
        error: Optional[BaseException] = self
        seen: set[int] = set()
        while isinstance(error, ArcError) and id(error) not in seen:
            seen.add(id(error))
            if error._response_body is not None:
                return error._response_body
            error = error.__cause__
        return None

    @response_body.setter
    def response_body(self, value: Optional[dict[str, Any]]) -> None:
        self._response_body = value


class ArcConnectionError(ArcError):
    """Connection to Arc server failed."""

    pass


class ArcAuthenticationError(ArcError):
    """Authentication failed."""

    pass


class ArcQueryError(ArcError):
    """Query execution failed."""

    pass


class ArcIngestionError(ArcError):
    """Data ingestion failed."""

    pass


class ArcValidationError(ArcError):
    """Request validation failed."""

    pass


class ArcNotFoundError(ArcError):
    """Resource not found."""

    pass


class ArcRateLimitError(ArcError):
    """Rate limit exceeded."""

    pass


class ArcServerError(ArcError):
    """Server returned an error."""

    def __init__(self, message: str, status_code: Optional[int] = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class ArcNotSupportedError(ArcError):
    """The server does not support the requested operation.

    Raised, for example, when calling ``query_msgpack`` against an Arc server
    that was not built with the ``duckdb_arrow`` build tag (HTTP 501).
    """

    pass


class ArcLicenseError(ArcAuthenticationError):
    """The requested feature requires an enterprise license.

    Subclasses :class:`ArcAuthenticationError` so existing ``except`` blocks
    that catch permission failures keep working.
    """

    pass


class ArcPermissionError(ArcAuthenticationError):
    """HTTP 403: the token does not carry the permission the endpoint needs.

    Distinct from :class:`ArcAuthenticationError`, which now means only HTTP
    401 — the token is missing, invalid or expired. A 401 is fixed by
    re-authenticating; a 403 is not, and needs a token with different
    permissions.

    Subclasses :class:`ArcAuthenticationError` so existing ``except`` blocks
    that caught 403 as an authentication failure keep working.
    """

    pass


class ArcPermissionDataUnavailableError(ArcPermissionError):
    """HTTP 403 because Arc could not load the token's grants.

    Arc fails closed when its permission data cannot be read, which arrives
    as a 403 like a real refusal. Nothing about the token or the database
    caused it, so it is raised separately: retrying with a different database
    or a different token will not help, and the Arc server log is where the
    cause is.
    """

    pass


class ArcScopedAccessError(ArcPermissionError):
    """HTTP 403 from Arc's per-database permission check.

    The token may read, but not the database the call asked about. Raised by
    the database listing endpoints:

    * :meth:`arc_client.management.databases.DatabaseClient.list` — Arc
      refuses to list every database for a token restricted to particular
      ones rather than returning a filtered list, matching ``SHOW
      DATABASES``. ``database`` is then ``"*"``.
    * :meth:`~arc_client.management.databases.DatabaseClient.get` and
      :meth:`~arc_client.management.databases.DatabaseClient.list_measurements`
      — the token holds no grant for the database named, which ``database``
      carries.

    This is a normal state for a tenant-scoped token, not a transient
    failure: an identical retry is refused identically, and the way forward
    is to name a database the token is granted. It is deliberately separate
    from :class:`ArcPermissionError` so "you are scoped, name your database"
    and "this token cannot read at all" can be handled differently.

    Attributes:
        database: The database the call was refused for, or ``"*"`` when it
            asked for every database. ``None`` if Arc's message did not name
            one.
    """

    def __init__(self, message: str, database: Optional[str] = None) -> None:
        super().__init__(message)
        self.database = database
