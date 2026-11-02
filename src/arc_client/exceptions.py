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
    """Query execution failed.

    Attributes:
        status_code: The HTTP status Arc answered with when the failure was an
            HTTP error without a more specific exception (e.g. 400 for invalid
            SQL, 500, or a 503 that outlasted the retries). ``None`` otherwise.
    """

    def __init__(self, message: str, status_code: Optional[int] = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class ArcIngestionError(ArcError):
    """Data ingestion failed.

    Attributes:
        status_code: The HTTP status Arc answered with, when there was one.
    """

    def __init__(self, message: str, status_code: Optional[int] = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class ArcPartialWriteError(ArcIngestionError):
    """Arc dropped some points of a write (HTTP 400 "partial write").

    Arc 2027.01 drops individual points it cannot store (a value of the wrong
    type for its column, a tag or field named ``time``, a name Arc could not
    read back) and still writes the rest of the request, answering
    ``400 "partial write: N points dropped: ..."``.

    The request has been applied as far as Arc could apply it, so it must
    **not** be resent: resending would duplicate every point that was stored.
    The SDK never retries it, and a buffered writer discards the batch and
    reports this error once.

    Attributes:
        dropped: Number of points Arc dropped.
        others_written: ``True`` when Arc reported that the other points of the
            request were written; ``False`` when every point was dropped and
            nothing was stored. Arc does not report an accepted-point count.
    """

    def __init__(self, message: str, dropped: int, others_written: bool) -> None:
        super().__init__(message, status_code=400)
        self.dropped = dropped
        self.others_written = others_written


class ArcIngestRefusedError(ArcIngestionError):
    """Arc refused a write with HTTP 503 before (or while) applying it.

    Raised for a 503 that carries ``X-Arc-Ingest-Fenced`` (this node is not the
    available primary writer, e.g. during a failover) or
    ``X-Arc-Ingest-Refused: <reason>`` (a node-local, transient condition such
    as ``wal_disk``, WAL volume disk pressure). Arc documents these as "nothing
    was written, retry", and the SDK resends them automatically up to
    ``max_retries``; this error means the retries ran out.

    The exception is reason ``wal_not_durable`` (``wal.ack_mode = "durable"``
    could not make the WAL entry durable): earlier parts of the same request
    may already be stored, so a resend may duplicate rows. The SDK never
    retries it, and ``retry_safe`` is ``False``.

    Attributes:
        reason: Arc's refusal reason (``"fenced"`` for a fence, otherwise the
            ``X-Arc-Ingest-Refused`` value such as ``"wal_disk"``).
        retry_safe: Whether Arc guarantees nothing of the request was applied.
    """

    def __init__(self, message: str, reason: str, retry_safe: bool) -> None:
        super().__init__(message, status_code=503)
        self.reason = reason
        self.retry_safe = retry_safe


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
