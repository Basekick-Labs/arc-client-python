"""Quick start example for arc-client."""

from arc_client import ArcClient

# Connect to Arc
with ArcClient(host="localhost", port=8000, token="your-token") as client:
    # Check server health
    health = client.health()
    print(f"Server status: {health.status}")

    # Write some CPU metrics (columnar format - fastest)
    client.write.write_columnar(
        measurement="cpu",
        columns={
            "time": [1704067200000000, 1704067260000000, 1704067320000000],
            "host": ["server01", "server01", "server01"],
            "region": ["us-east", "us-east", "us-east"],
            "usage_idle": [95.2, 94.8, 93.1],
            "usage_system": [2.1, 2.5, 3.2],
            "usage_user": [2.7, 2.7, 3.7],
        },
    )
    print("Data written successfully!")

    # Query the data back (use database.measurement syntax)
    result = client.query.query("SELECT * FROM default.cpu ORDER BY time DESC LIMIT 10")
    print(f"\nQuery returned {result.row_count} rows:")
    print(f"Columns: {result.columns}")
    for row in result.data:
        print(row)

    # Query to pandas DataFrame (requires pandas)
    try:
        df = client.query.query_pandas("SELECT * FROM default.cpu")
        print(f"\nDataFrame shape: {df.shape}")
        print(df.head())
    except ImportError:
        print("\nInstall pandas for DataFrame support: pip install arc-client[pandas]")
