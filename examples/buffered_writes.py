"""Buffered writes example - for high-throughput ingestion."""

import random
import time

from arc_client import ArcClient


def generate_metrics(count: int):
    """Generate simulated metrics."""
    base_time = int(time.time() * 1_000_000)  # microseconds
    hosts = ["web-01", "web-02", "web-03", "db-01", "db-02"]

    for i in range(count):
        yield {
            "measurement": "system_metrics",
            "tags": {
                "host": random.choice(hosts),
                "datacenter": "us-east-1",
            },
            "fields": {
                "cpu_usage": random.uniform(10, 90),
                "memory_usage": random.uniform(30, 80),
                "disk_io": random.uniform(0, 100),
            },
            "timestamp": base_time + (i * 1000),  # 1ms apart
        }


def main():
    """Demonstrate buffered writes for high-throughput ingestion."""
    with ArcClient(host="localhost", token="your-token") as client:
        # Use buffered writer for automatic batching
        # - batch_size: flush after N records
        # - flush_interval: flush after N seconds (even if batch not full)
        with client.write.buffered(batch_size=5000, flush_interval=2.0) as buffer:
            start = time.time()
            count = 0

            for metric in generate_metrics(50_000):
                buffer.write(
                    measurement=metric["measurement"],
                    tags=metric["tags"],
                    fields=metric["fields"],
                    timestamp=metric["timestamp"],
                )
                count += 1

                if count % 10_000 == 0:
                    print(f"Queued {count} records...")

            # Buffer auto-flushes on exit

        elapsed = time.time() - start
        print(f"\nWrote {count} records in {elapsed:.2f}s")
        print(f"Throughput: {count / elapsed:,.0f} records/sec")


if __name__ == "__main__":
    main()
