"""Retention policies and continuous queries example."""

from arc_client import ArcClient


def retention_example(client: ArcClient):
    """Demonstrate retention policy management."""
    print("=== Retention Policies ===\n")

    # Create a retention policy
    policy = client.retention.create(
        name="logs-30d",
        database="default",
        retention_days=30,
        measurement="logs",  # Optional: applies to specific measurement
        buffer_days=7,  # Keep 7 extra days as buffer
    )
    print(f"Created policy: {policy.name} (id={policy.id})")

    # List all policies
    policies = client.retention.list()
    print(f"\nAll policies ({len(policies)}):")
    for p in policies:
        status = "active" if p.is_active else "inactive"
        print(f"  - {p.name}: {p.retention_days} days ({status})")

    # Dry-run execution to preview what would be deleted
    result = client.retention.execute(policy.id, dry_run=True)
    print(f"\nDry run: would delete {result.deleted_count} rows")

    # Actually execute (uncomment to run)
    # result = client.retention.execute(policy.id, dry_run=False, confirm=True)
    # print(f"Deleted {result.deleted_count} rows")

    # Clean up
    client.retention.delete(policy.id)
    print(f"\nDeleted policy: {policy.name}")


def continuous_query_example(client: ArcClient):
    """Demonstrate continuous query management."""
    print("\n=== Continuous Queries ===\n")

    # Create a CQ to downsample metrics
    cq = client.continuous_queries.create(
        name="cpu-hourly-avg",
        database="default",
        source_measurement="cpu",
        destination_measurement="cpu_1h",
        query=(
            "SELECT time_bucket('1 hour', time) as time, "
            "host, "
            "avg(usage_idle) as usage_idle, "
            "avg(usage_system) as usage_system "
            "FROM default.cpu "  # Use database.measurement syntax
            "GROUP BY 1, 2"
        ),
        interval="1h",
        description="Hourly CPU averages per host",
    )
    print(f"Created CQ: {cq.name} (id={cq.id})")
    print(f"  Source: {cq.source_measurement} -> {cq.destination_measurement}")
    print(f"  Interval: {cq.interval}")

    # List CQs
    cqs = client.continuous_queries.list(database="default")
    print(f"\nAll CQs ({len(cqs)}):")
    for q in cqs:
        status = "active" if q.is_active else "inactive"
        print(f"  - {q.name}: {q.interval} ({status})")

    # Manual execution with time range (dry run)
    result = client.continuous_queries.execute(
        cq.id,
        start_time="2024-01-01T00:00:00Z",
        end_time="2024-01-02T00:00:00Z",
        dry_run=True,
    )
    print(f"\nDry run: would process {result.records_read or 0} records")

    # Clean up
    client.continuous_queries.delete(cq.id)
    print(f"\nDeleted CQ: {cq.name}")


def main():
    with ArcClient(host="localhost", token="your-token") as client:
        retention_example(client)
        continuous_query_example(client)


if __name__ == "__main__":
    main()
