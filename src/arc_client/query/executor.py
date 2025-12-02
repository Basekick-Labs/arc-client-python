"""Synchronous query client for Arc."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.ipc as ipc

from arc_client.config import ClientConfig
from arc_client.exceptions import ArcQueryError, ArcValidationError
from arc_client.http.sync_http import SyncHTTPClient
from arc_client.models.query import EstimateResponse, MeasurementInfo, QueryResponse

if TYPE_CHECKING:
    pass


class QueryClient:
    """Synchronous client for querying data from Arc.

    This client provides methods for executing SQL queries and retrieving
    results in various formats including raw JSON, pandas DataFrames,
    Polars DataFrames, and PyArrow Tables.

    Example:
        >>> with ArcClient(host="localhost", token="xxx") as client:
        ...     # JSON response
        ...     result = client.query.query("SELECT * FROM cpu LIMIT 10")
        ...     print(result.columns, result.data)
        ...
        ...     # pandas DataFrame
        ...     df = client.query.query_pandas("SELECT * FROM cpu LIMIT 10")
        ...
        ...     # Polars DataFrame (fastest for large results)
        ...     pl_df = client.query.query_polars("SELECT * FROM cpu LIMIT 10")
        ...
        ...     # PyArrow Table (zero-copy)
        ...     table = client.query.query_arrow("SELECT * FROM cpu LIMIT 10")
    """

    def __init__(self, http: SyncHTTPClient, config: ClientConfig) -> None:
        self._http = http
        self._config = config

    def query(self, sql: str, database: str | None = None) -> QueryResponse:
        """Execute a SQL query and return results as JSON.

        This method returns structured JSON data. For large result sets,
        consider using query_arrow() or query_polars() for better performance.

        Args:
            sql: SQL query to execute.
            database: Target database. Uses client default if not specified.

        Returns:
            QueryResponse with columns, data, row_count, and metadata.

        Raises:
            ArcQueryError: If the query fails.
            ArcValidationError: If the SQL is invalid.

        Example:
            >>> result = client.query.query("SELECT * FROM cpu WHERE time > now() - '1h'")
            >>> for row in result.data:
            ...     print(dict(zip(result.columns, row)))
        """
        if not sql or not sql.strip():
            raise ArcValidationError("SQL query cannot be empty")

        headers = {}
        if database:
            headers["x-arc-database"] = database

        try:
            response = self._http.post(
                "/api/v1/query",
                json={"sql": sql},
                headers=headers if headers else None,
            )

            data = response.json()
            return QueryResponse(**data)

        except ArcQueryError:
            raise
        except Exception as e:
            raise ArcQueryError(f"Query failed: {e}") from e

    def query_pandas(self, sql: str, database: str | None = None) -> Any:
        """Execute a SQL query and return results as a pandas DataFrame.

        Uses Arrow IPC streaming for efficient data transfer. Requires
        pandas to be installed.

        Args:
            sql: SQL query to execute.
            database: Target database. Uses client default if not specified.

        Returns:
            pandas.DataFrame with query results.

        Raises:
            ArcQueryError: If the query fails.
            ImportError: If pandas is not installed.

        Example:
            >>> df = client.query.query_pandas("SELECT * FROM cpu LIMIT 1000")
            >>> print(df.describe())
        """
        try:
            import pandas  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "pandas is required for query_pandas(). "
                "Install it with: pip install arc-client[pandas]"
            ) from e

        table = self.query_arrow(sql, database)
        return table.to_pandas()

    def query_polars(self, sql: str, database: str | None = None) -> Any:
        """Execute a SQL query and return results as a Polars DataFrame.

        Uses Arrow IPC streaming for efficient zero-copy data transfer.
        This is the fastest method for large result sets. Requires
        polars to be installed.

        Args:
            sql: SQL query to execute.
            database: Target database. Uses client default if not specified.

        Returns:
            polars.DataFrame with query results.

        Raises:
            ArcQueryError: If the query fails.
            ImportError: If polars is not installed.

        Example:
            >>> df = client.query.query_polars("SELECT * FROM cpu LIMIT 1000000")
            >>> print(df.describe())
        """
        try:
            import polars as pl
        except ImportError as e:
            raise ImportError(
                "polars is required for query_polars(). "
                "Install it with: pip install arc-client[polars]"
            ) from e

        table = self.query_arrow(sql, database)
        return pl.from_arrow(table)

    def query_arrow(self, sql: str, database: str | None = None) -> pa.Table:
        """Execute a SQL query and return results as a PyArrow Table.

        Uses Arrow IPC streaming for efficient zero-copy data transfer.
        This method provides the most efficient way to handle large
        result sets when you need raw Arrow data.

        Args:
            sql: SQL query to execute.
            database: Target database. Uses client default if not specified.

        Returns:
            pyarrow.Table with query results.

        Raises:
            ArcQueryError: If the query fails.
            ArcValidationError: If the SQL is invalid.

        Example:
            >>> table = client.query.query_arrow("SELECT * FROM cpu LIMIT 1000")
            >>> print(table.schema)
            >>> print(table.num_rows)
        """
        if not sql or not sql.strip():
            raise ArcValidationError("SQL query cannot be empty")

        headers = {
            "Accept": "application/vnd.apache.arrow.stream",
        }
        if database:
            headers["x-arc-database"] = database

        try:
            response = self._http.post(
                "/api/v1/query/arrow",
                json={"sql": sql},
                headers=headers,
            )

            # Check if we got JSON error response instead of Arrow
            content_type = response.headers.get("content-type", "")
            if "application/json" in content_type:
                error_data = response.json()
                raise ArcQueryError(
                    error_data.get("error", "Query failed with unknown error")
                )

            # Parse Arrow IPC stream
            reader = ipc.open_stream(response.content)
            return reader.read_all()

        except ArcQueryError:
            raise
        except pa.ArrowInvalid as e:
            raise ArcQueryError(f"Failed to parse Arrow response: {e}") from e
        except Exception as e:
            raise ArcQueryError(f"Query failed: {e}") from e

    def estimate(self, sql: str, database: str | None = None) -> EstimateResponse:
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

        Example:
            >>> estimate = client.query.estimate("SELECT * FROM cpu")
            >>> if estimate.warning_level == "high":
            ...     print(f"Warning: {estimate.warning_message}")
            ...     print(f"Estimated rows: {estimate.estimated_rows:,}")
        """
        if not sql or not sql.strip():
            raise ArcValidationError("SQL query cannot be empty")

        headers = {}
        if database:
            headers["x-arc-database"] = database

        try:
            response = self._http.post(
                "/api/v1/query/estimate",
                json={"sql": sql},
                headers=headers if headers else None,
            )

            data = response.json()
            return EstimateResponse(**data)

        except ArcQueryError:
            raise
        except Exception as e:
            raise ArcQueryError(f"Estimation failed: {e}") from e

    def list_measurements(self, database: str | None = None) -> list[MeasurementInfo]:
        """List all measurements in the database.

        Args:
            database: Database name. Lists measurements from all databases
                if not specified.

        Returns:
            List of MeasurementInfo objects with measurement details.

        Raises:
            ArcQueryError: If the request fails.

        Example:
            >>> measurements = client.query.list_measurements()
            >>> for m in measurements:
            ...     print(f"{m.database}.{m.measurement}: {m.total_size_mb:.2f} MB")
        """
        try:
            params = {}
            if database:
                params["database"] = database

            response = self._http.get("/api/v1/measurements", params=params)
            data = response.json()

            if not data.get("success", True):
                raise ArcQueryError(data.get("error", "Failed to list measurements"))

            return [MeasurementInfo(**m) for m in data.get("measurements", [])]

        except ArcQueryError:
            raise
        except Exception as e:
            raise ArcQueryError(f"Failed to list measurements: {e}") from e

    def list_databases(self) -> list[str]:
        """List all databases.

        Returns:
            List of database names.

        Raises:
            ArcQueryError: If the request fails.

        Example:
            >>> databases = client.query.list_databases()
            >>> for db in databases:
            ...     print(db)
        """
        try:
            # Use SHOW DATABASES query
            result = self.query("SHOW DATABASES")

            if not result.success:
                raise ArcQueryError(result.error or "Failed to list databases")

            # Extract database names from result
            return [row[0] for row in result.data if row]

        except ArcQueryError:
            raise
        except Exception as e:
            raise ArcQueryError(f"Failed to list databases: {e}") from e

    def show_tables(self, database: str | None = None) -> QueryResponse:
        """Show tables/measurements in a database.

        Args:
            database: Database name. Uses 'default' if not specified.

        Returns:
            QueryResponse with table information.

        Example:
            >>> result = client.query.show_tables("default")
            >>> for row in result.data:
            ...     print(row)
        """
        db = database or "default"
        return self.query(f"SHOW TABLES FROM {db}")
