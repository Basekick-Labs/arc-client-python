"""Async example for arc-client."""

import asyncio

from arc_client import AsyncArcClient


async def main():
    """Demonstrate async client usage."""
    async with AsyncArcClient(host="localhost", token="your-token") as client:
        # Check health
        health = await client.health()
        print(f"Server status: {health.status}")

        # Write data
        await client.write.write_columnar(
            measurement="temperature",
            columns={
                "time": [1704067200000000, 1704067260000000],
                "sensor_id": ["sensor-001", "sensor-001"],
                "location": ["warehouse-a", "warehouse-a"],
                "value": [22.5, 22.7],
            },
        )
        print("Data written!")

        # Query with Arrow (zero-copy, best for large results)
        # Use database.measurement syntax
        table = await client.query.query_arrow(
            "SELECT * FROM default.temperature ORDER BY time"
        )
        print(f"\nArrow table: {table.num_rows} rows, {table.num_columns} columns")
        print(table.to_pandas())


if __name__ == "__main__":
    asyncio.run(main())
