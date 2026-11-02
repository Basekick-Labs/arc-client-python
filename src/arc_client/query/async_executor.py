"""Asynchronous query client for Arc."""

from __future__ import annotations

from typing import Any, Optional

import pyarrow as pa

from arc_client.config import ClientConfig
from arc_client.exceptions import (
    ArcConnectionError,
    ArcError,
    ArcQueryError,
    ArcServerError,
    ArcValidationError,
)
from arc_client.http.async_http import AsyncHTTPClient
from arc_client.models.query import EstimateResponse, MeasurementInfo, QueryResponse
from arc_client.query.arrow_response import QUERY_ID_HEADER, decode_arrow_response
from arc_client.query.msgpack_response import decode_msgpack_query_response
from arc_client.query.options import (
    arrow_headers,
    measurement_params,
    query_headers,
    query_response_from_json,
)
from arc_client.query.options import status_code as _status
from arc_client.validation import validate_name


class AsyncQueryClient:
    """Asynchronous client for querying data from Arc.

    This client provides async methods for executing SQL queries and retrieving
    results in various formats including raw JSON, pandas DataFrames,
    Polars DataFrames, and PyArrow Tables.

    Example:
        >>> async with AsyncArcClient(host="localhost", token="xxx") as client:
        ...     # JSON response
        ...     result = await client.query.query("SELECT * FROM cpu LIMIT 10")
        ...     print(result.columns, result.data)
        ...
        ...     # pandas DataFrame
        ...     df = await client.query.query_pandas("SELECT * FROM cpu LIMIT 10")
        ...
        ...     # Polars DataFrame (fastest for large results)
        ...     pl_df = await client.query.query_polars("SELECT * FROM cpu LIMIT 10")
        ...
        ...     # PyArrow Table (zero-copy)
        ...     table = await client.query.query_arrow("SELECT * FROM cpu LIMIT 10")
    """

    def __init__(self, http: AsyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    async def query(
        self, sql: str, database: Optional[str] = None, *, profile: bool = False
    ) -> QueryResponse:
        """Execute a SQL query and return results as JSON.

        This method returns structured JSON data. For large result sets,
        consider using query_arrow() or query_polars() for better performance.

        Queries only read, so a 429 or 503 (Arc's replication catch-up gate, a
        partition removed mid-query, an unavailable cold tier) is retried with
        backoff, honoring ``Retry-After``, up to ``max_retries``.

        Args:
            sql: SQL query to execute.
            database: Target database. Uses client default if not specified.
            profile: Ask Arc for server-side timing (``x-arc-profile``),
                returned in ``QueryResponse.profile``.

        Returns:
            QueryResponse with columns, data, row_count, metadata and
            ``query_id``.

        Raises:
            ArcQueryError: If the query fails. ``status_code`` carries the HTTP
                status when Arc answered with a generic error status (e.g. 400
                for invalid SQL).
            ArcValidationError: If the SQL is invalid.
            ArcError: Typed failures (ArcAuthenticationError,
                ArcPermissionError, ArcScopedAccessError, ArcRateLimitError,
                ArcNotSupportedError, ...) are raised as themselves. Transport
                failures (ArcConnectionError) are wrapped in ArcQueryError
                with the original as ``__cause__``.
        """
        if not sql or not sql.strip():
            raise ArcValidationError("SQL query cannot be empty")

        try:
            response = await self._http.post(
                "/api/v1/query",
                json={"sql": sql},
                headers=query_headers(database, profile) or None,
                idempotent=True,
            )
            return query_response_from_json(response)
        except (ArcServerError, ArcConnectionError) as e:
            raise ArcQueryError(f"Query failed: {e}", status_code=_status(e)) from e
        except ArcError:
            raise
        except Exception as e:
            raise ArcQueryError(f"Query failed: {e}") from e

    async def query_msgpack(
        self, sql: str, database: Optional[str] = None, *, profile: bool = False
    ) -> QueryResponse:
        """Execute a SQL query and decode the MessagePack response.

        Uses Arc's ``/api/v1/query/msgpack`` endpoint, which
        returns a compact columnar MessagePack envelope with native timestamp
        encoding. The result is transposed to row-major and returned as a
        :class:`QueryResponse` (same shape as :meth:`query`), with per-column
        Arrow types in ``QueryResponse.types``.

        Requires the Arc server to be built with the ``duckdb_arrow`` build tag;
        otherwise the server responds with HTTP 501.

        A stream Arc fails after sending the 200 (client disconnect, deadline)
        arrives as an incomplete MessagePack body and raises ArcQueryError;
        partial rows are never returned (Arc's msgpack writer has no
        ``truncated`` marker).

        Args:
            sql: SQL query to execute.
            database: Target database. Uses client default if not specified.
            profile: Ask Arc for server-side timing (``x-arc-profile``),
                returned in ``QueryResponse.profile``.

        Returns:
            QueryResponse with row-major ``data``, populated ``types`` and
            ``query_id``.

        Raises:
            ArcValidationError: If the SQL is empty.
            ArcNotSupportedError: If the server lacks msgpack query support (501).
            ArcQueryError: If the query fails or the response is incomplete.
        """
        if not sql or not sql.strip():
            raise ArcValidationError("SQL query cannot be empty")

        headers = query_headers(database, profile)
        headers["Accept"] = "application/msgpack"

        try:
            response = await self._http.post(
                "/api/v1/query/msgpack",
                json={"sql": sql},
                headers=headers,
                idempotent=True,
            )
            return decode_msgpack_query_response(
                response.content, query_id=response.headers.get(QUERY_ID_HEADER)
            )
        except (ArcServerError, ArcConnectionError) as e:
            raise ArcQueryError(f"Query failed: {e}", status_code=_status(e)) from e
        except ArcError:
            raise
        except Exception as e:
            raise ArcQueryError(f"Query failed: {e}") from e

    async def query_pandas(
        self,
        sql: str,
        database: Optional[str] = None,
        *,
        dictionary: bool = False,
        compression: Optional[str] = None,
    ) -> Any:
        """Execute a SQL query and return results as a pandas DataFrame.

        Uses Arrow IPC streaming for efficient data transfer. Requires
        pandas to be installed.

        Args:
            sql: SQL query to execute.
            database: Target database. Uses client default if not specified.
            dictionary: See :meth:`query_arrow`. Dictionary-encoded string
                columns become pandas ``Categorical`` columns.
            compression: See :meth:`query_arrow`.

        Returns:
            pandas.DataFrame with query results.

        Raises:
            ArcQueryError: If the query fails.
            ImportError: If pandas is not installed.
        """
        try:
            import pandas  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "pandas is required for query_pandas(). "
                "Install it with: pip install arc-client[pandas]"
            ) from e

        table = await self.query_arrow(
            sql, database, dictionary=dictionary, compression=compression
        )
        return table.to_pandas()

    async def query_polars(
        self,
        sql: str,
        database: Optional[str] = None,
        *,
        dictionary: bool = False,
        compression: Optional[str] = None,
    ) -> Any:
        """Execute a SQL query and return results as a Polars DataFrame.

        Uses Arrow IPC streaming for efficient zero-copy data transfer.
        This is the fastest method for large result sets. Requires
        polars to be installed.

        Args:
            sql: SQL query to execute.
            database: Target database. Uses client default if not specified.
            dictionary: See :meth:`query_arrow`. Dictionary-encoded string
                columns become polars ``Categorical`` columns.
            compression: See :meth:`query_arrow`.

        Returns:
            polars.DataFrame with query results.

        Raises:
            ArcQueryError: If the query fails.
            ImportError: If polars is not installed.
        """
        try:
            import polars as pl
        except ImportError as e:
            raise ImportError(
                "polars is required for query_polars(). "
                "Install it with: pip install arc-client[polars]"
            ) from e

        table = await self.query_arrow(
            sql, database, dictionary=dictionary, compression=compression
        )
        return pl.from_arrow(table)

    async def query_arrow(
        self,
        sql: str,
        database: Optional[str] = None,
        *,
        dictionary: bool = False,
        compression: Optional[str] = None,
    ) -> pa.Table:
        """Execute a SQL query and return results as a PyArrow Table.

        Uses Arrow IPC streaming for efficient zero-copy data transfer.
        This method provides the most efficient way to handle large
        result sets when you need raw Arrow data.

        Args:
            sql: SQL query to execute.
            database: Target database. Uses client default if not specified.
            dictionary: Ask Arc to dictionary-encode low-cardinality string
                columns (``x-arc-arrow-dictionary: true``); those columns
                arrive as ``dictionary<int32, utf8>``. Smaller on the wire for
                repetitive tags.
            compression: Arrow IPC buffer compression, ``"zstd"`` or ``"lz4"``
                (``x-arc-arrow-compression``), decoded transparently by
                pyarrow. ``None`` (default) sends uncompressed buffers.

        Returns:
            pyarrow.Table with query results and completion metadata in
            its schema: arc.rows_capped, arc.row_cap, arc.execution_time_ms,
            and arc.query_id (Arc's ``X-Arc-Query-ID``, for
            ``client.queries``). Missing cap metadata means unknown.
            Truncation raises ArcQueryError.

        Raises:
            ArcQueryError: If the query fails.
            ArcValidationError: If the SQL is empty, or ``compression`` is not
                ``"zstd"`` or ``"lz4"``.
        """
        if not sql or not sql.strip():
            raise ArcValidationError("SQL query cannot be empty")

        headers = arrow_headers(database, dictionary, compression)

        try:
            response = await self._http.post(
                "/api/v1/query/arrow",
                json={"sql": sql},
                headers=headers,
                idempotent=True,
            )

            return decode_arrow_response(response)

        except (ArcServerError, ArcConnectionError) as e:
            raise ArcQueryError(f"Query failed: {e}", status_code=_status(e)) from e
        except ArcError:
            raise
        except pa.ArrowInvalid as e:
            raise ArcQueryError(f"Failed to parse Arrow response: {e}") from e
        except Exception as e:
            raise ArcQueryError(f"Query failed: {e}") from e

    async def estimate(self, sql: str, database: Optional[str] = None) -> EstimateResponse:
        """Estimate the cost of a SQL query.

        Returns an estimate of how many rows the query will return and
        a warning level indicating if the query might be expensive.

        Args:
            sql: SQL query to estimate.
            database: Target database. Uses client default if not specified.

        Returns:
            EstimateResponse with estimated_rows and warning_level.

        Raises:
            ArcQueryError: If estimation fails.
            ArcValidationError: If the SQL is invalid.
        """
        if not sql or not sql.strip():
            raise ArcValidationError("SQL query cannot be empty")

        try:
            response = await self._http.post(
                "/api/v1/query/estimate",
                json={"sql": sql},
                headers=query_headers(database, False) or None,
                idempotent=True,
            )

            data = response.json()
            return EstimateResponse(**data)

        except (ArcServerError, ArcConnectionError) as e:
            raise ArcQueryError(f"Estimation failed: {e}", status_code=_status(e)) from e
        except ArcError:
            raise
        except Exception as e:
            raise ArcQueryError(f"Estimation failed: {e}") from e

    async def list_measurements(self, database: Optional[str] = None) -> list[MeasurementInfo]:
        """List all measurements in the database.

        Args:
            database: Database name. Lists measurements from all databases
                if not specified.

        Returns:
            List of MeasurementInfo objects with measurement details.

        Raises:
            ArcQueryError: If the request fails.
        """
        try:
            params = {}
            if database:
                params["database"] = database

            response = await self._http.get("/api/v1/measurements", params=params)
            data = response.json()

            if not data.get("success", True):
                raise ArcQueryError(data.get("error", "Failed to list measurements"))

            return [MeasurementInfo(**m) for m in data.get("measurements", [])]

        except (ArcServerError, ArcConnectionError) as e:
            raise ArcQueryError(f"Failed to list measurements: {e}", status_code=_status(e)) from e
        except ArcError:
            raise
        except Exception as e:
            raise ArcQueryError(f"Failed to list measurements: {e}") from e

    async def list_databases(self) -> list[str]:
        """List all databases.

        Returns:
            List of database names.

        Raises:
            ArcQueryError: If the request fails.
        """
        try:
            # Use SHOW DATABASES query
            result = await self.query("SHOW DATABASES")

            if not result.success:
                raise ArcQueryError(result.error or "Failed to list databases")

            # Extract database names from result
            return [row[0] for row in result.data if row]

        except ArcError:
            raise
        except Exception as e:
            raise ArcQueryError(f"Failed to list databases: {e}") from e

    async def query_measurement(
        self,
        measurement: str,
        database: Optional[str] = None,
        *,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
        where: Optional[str] = None,
    ) -> QueryResponse:
        """Read rows of one measurement with ``GET /api/v1/query/:measurement``.

        Arc builds ``SELECT * FROM <database>.<measurement> [WHERE <where>]
        ORDER BY <order_by> LIMIT <limit> OFFSET <offset>`` itself and
        validates every piece. Omitted arguments take Arc's defaults:
        ``limit`` 100, ``offset`` 0, ``order_by`` ``"time DESC"``.

        Args:
            measurement: Measurement (table) name.
            database: Database name. This endpoint takes the database as a
                query parameter, not the ``x-arc-database`` header; the client
                default is sent when not specified, else Arc uses
                ``"default"``.
            limit: Maximum rows, 0..1,000,000.
            offset: Rows to skip, >= 0.
            order_by: ORDER BY clause, e.g. ``"time ASC"``.
            where: WHERE clause without the keyword, e.g. ``"host = 'a'"``.

        Returns:
            QueryResponse, as from :meth:`query`.

        Raises:
            ArcValidationError: If a name or a numeric argument is invalid.
            ArcQueryError: If Arc rejects the request (``status_code`` set).
        """
        params = measurement_params(
            measurement, database or self._config.database, limit, offset, order_by, where
        )
        try:
            response = await self._http.get(f"/api/v1/query/{measurement}", params=params)
            return query_response_from_json(response)
        except (ArcServerError, ArcConnectionError) as e:
            raise ArcQueryError(f"Query failed: {e}", status_code=_status(e)) from e
        except ArcError:
            raise
        except Exception as e:
            raise ArcQueryError(f"Query failed: {e}") from e

    async def show_tables(self, database: Optional[str] = None) -> QueryResponse:
        """Show tables/measurements in a database.

        Args:
            database: Database name. Uses 'default' if not specified.

        Returns:
            QueryResponse with table information.
        """
        db = validate_name(database or "default", "database")
        return await self.query(f"SHOW TABLES FROM {db}")
