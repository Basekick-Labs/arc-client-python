"""DataFrame integration example - pandas and polars."""

from arc_client import ArcClient


def pandas_example(client: ArcClient):
    """Demonstrate pandas integration."""
    import pandas as pd

    # Create a DataFrame
    df = pd.DataFrame({
        "time": pd.date_range("2024-01-01", periods=100, freq="1min"),
        "host": ["server-01"] * 50 + ["server-02"] * 50,
        "region": ["us-east"] * 100,
        "cpu_usage": [50 + i * 0.1 for i in range(100)],
        "memory_mb": [1024 + i for i in range(100)],
    })

    # Write DataFrame to Arc
    client.write.write_dataframe(
        df,
        measurement="server_metrics",
        time_column="time",
        tag_columns=["host", "region"],
    )
    print(f"Wrote {len(df)} rows from pandas DataFrame")

    # Query back to DataFrame (use database.measurement syntax)
    result_df = client.query.query_pandas(
        "SELECT * FROM default.server_metrics WHERE host = 'server-01' ORDER BY time"
    )
    print(f"\nQueried {len(result_df)} rows back:")
    print(result_df.head())
    print(f"\nData types:\n{result_df.dtypes}")


def polars_example(client: ArcClient):
    """Demonstrate polars integration."""
    # Query to polars DataFrame (fastest for large results)
    df = client.query.query_polars(
        "SELECT host, avg(cpu_usage) as avg_cpu, max(memory_mb) as max_memory "
        "FROM default.server_metrics GROUP BY host"
    )
    print("\nPolars aggregation result:")
    print(df)


def arrow_example(client: ArcClient):
    """Demonstrate PyArrow integration."""
    # Query to Arrow table (zero-copy, interoperable)
    table = client.query.query_arrow("SELECT * FROM default.server_metrics LIMIT 1000")

    print(f"\nArrow table: {table.num_rows} rows")
    print(f"Schema: {table.schema}")

    # Convert to pandas with zero-copy where possible
    df = table.to_pandas()
    print(f"\nConverted to pandas: {df.shape}")


def main():
    with ArcClient(host="localhost", token="your-token") as client:
        print("=== Pandas Example ===")
        try:
            pandas_example(client)
        except ImportError:
            print("pandas not installed. Run: pip install arc-client[pandas]")

        print("\n=== Polars Example ===")
        try:
            polars_example(client)
        except ImportError:
            print("polars not installed. Run: pip install arc-client[polars]")

        print("\n=== Arrow Example ===")
        arrow_example(client)


if __name__ == "__main__":
    main()
