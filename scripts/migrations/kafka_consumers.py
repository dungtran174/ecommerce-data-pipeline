#!/usr/bin/env python3
"""Recreate typed ClickHouse Kafka consumers with a bounded parser-error skip."""

import json
import re
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "clickhouse/init/01_bronze_kafka_tables.sql"
TARGET_SKIP = 100
CLIENT = [
    "docker", "compose", "exec", "-T", "clickhouse", "sh", "-lc",
    'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery',
]


def clickhouse(sql: str) -> str:
    result = subprocess.run(CLIENT, input=sql, text=True, capture_output=True, check=False)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout


def source_definitions():
    without_comments = re.sub(r"(?m)^\s*--[^\n]*$", "", SOURCE.read_text())
    tables = {}
    views = {}
    for part in without_comments.split(";"):
        ddl = part.strip()
        if not ddl:
            continue
        table = re.match(r"CREATE TABLE IF NOT EXISTS bronze\.kafka_(\w+)\s*\(", ddl)
        view = re.match(r"CREATE MATERIALIZED VIEW IF NOT EXISTS bronze\.mv_(\w+)\s+TO", ddl)
        if table and table.group(1) != "raw_events":
            tables[table.group(1)] = ddl
        elif view:
            views[view.group(1)] = ddl
    if len(tables) != 22 or set(tables) != set(views) - {"raw_events"}:
        raise RuntimeError("Typed Kafka tables and materialized views disagree with the source schema")
    for suffix, ddl in tables.items():
        if f"kafka_skip_broken_messages = {TARGET_SKIP}" not in ddl:
            raise RuntimeError(f"Missing parser-error limit in kafka_{suffix}")
    return tables, views


def main() -> None:
    tables, views = source_definitions()
    # Stop consumers before replacing views; recover interrupted detaches.
    detached = clickhouse("SELECT table FROM system.detached_tables WHERE database='bronze' FORMAT TabSeparatedRaw")
    for table in detached.splitlines():
        if table.startswith('kafka_') and table[6:] in tables:
            clickhouse(f"ATTACH TABLE bronze.{table};")
    rows = clickhouse(
        "SELECT name, create_table_query FROM system.tables "
        "WHERE database='bronze' FORMAT JSONEachRow"
    )
    current = {row["name"]: row["create_table_query"]
               for line in rows.splitlines() if line.strip()
               for row in [json.loads(line)]}
    recreated = []
    repaired_views = []
    for suffix in sorted(tables):
        table_name = f"kafka_{suffix}"
        view_name = f"mv_{suffix}"
        existing = current.get(table_name, "")
        if (f"kafka_skip_broken_messages = {TARGET_SKIP}" in existing
                and "kafka_flush_interval_ms = 1000" in existing
                and "kafka_num_consumers = 4" not in existing
                and (suffix != 'orderdetails' or
                     ('unit_cost_at_order' in existing and
                      (view_name not in current or 'unit_cost_at_order' in current[view_name])))):
            if view_name not in current:
                clickhouse(views[suffix] + ";")
                repaired_views.append(suffix)
            continue
        clickhouse(
            (f"DETACH TABLE bronze.{table_name};\n" if existing else "")
            + f"DROP TABLE IF EXISTS bronze.{view_name};\n"
            + (f"ATTACH TABLE bronze.{table_name};\n" if existing else "")
            + f"DROP TABLE IF EXISTS bronze.{table_name};\n"
            + tables[suffix] + ";\n"
            + views[suffix] + ";\n"
        )
        recreated.append(suffix)
        print(f"Recreated Kafka consumer: {suffix}", flush=True)
    print(f"Kafka consumers ready: {len(tables)}; recreated={len(recreated)}; "
          f"repaired_views={len(repaired_views)}")


if __name__ == "__main__":
    main()
