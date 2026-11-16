"""Build, validate and publish a materialized warehouse generation.

Only the active-snapshot pointer is exchanged at publication. Tables retain the
previous generations, so a failed build never truncates the published reports.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import re
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import uuid

LOG = logging.getLogger(__name__)
SQL_DIR = Path(os.getenv("WAREHOUSE_SQL_DIR", "/opt/airflow/dags/sql/warehouse"))
LOCK_KEY = 741209620
LOCK_CONNECTION = ContextVar("warehouse_lock_connection", default=None)
SILVER_KEYS = {
    "users": "user_id", "locations": "province_id", "campaigns": "campaign_id",
    "order_status": "id", "orders": "order_id", "products": "product_id",
    "order_items": "order_item_id",
}
GOLD_KEYS = {
    "dim_products": "product_id", "dim_locations": "province_id",
    "dim_campaigns": "campaign_id", "dim_order_status": "id",
    "FACT_USER_REGISTRATION": "registration_date",
    "FACT_ORDER_OVERVIEW": "date_key, city_id, order_status_id, payment_method, shipping_method",
    "sales_line_measured": "order_id, order_item_id",
    "FACT_SALES_PRODUCT": "date_key, city_id, product_id, campaign_key",
}
VIEW_PATTERN = re.compile(
    r"CREATE (?:OR REPLACE )?VIEW (?:IF NOT EXISTS )?(\w+)\.(\w+) AS\s*(.*?);",
    re.S | re.I,
)


def definitions(filename: str, sql_dir: Path = SQL_DIR):
    text = re.sub(r"--[^\n]*", "", (sql_dir / filename).read_text())
    return [(db, name, query.strip()) for db, name, query in VIEW_PATTERN.findall(text)]


class ClickHouse:
    def __init__(self):
        host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
        port = os.getenv("CLICKHOUSE_INTERNAL_HTTP_PORT", "8123")
        self.url = f"http://{host}:{port}/?" + urlencode({
            "max_execution_time": 240, "max_threads": 2,
            "max_memory_usage": 1500000000, "join_use_nulls": 1, "wait_end_of_query": 1,
        })
        self.headers = {
            "X-ClickHouse-User": os.getenv("CLICKHOUSE_USER", "default"),
            "X-ClickHouse-Key": os.getenv("CLICKHOUSE_PASSWORD", ""),
        }

    def execute(self, sql: str) -> str:
        req = Request(self.url, data=sql.encode(), headers=self.headers, method="POST")
        try:
            with urlopen(req, timeout=270) as response:
                result = response.read().decode().strip()
                if result.startswith("Code:"):
                    raise RuntimeError(result[:4000])
                return result
        except HTTPError as exc:
            raise RuntimeError(exc.read().decode()[:4000]) from exc

    def scalar(self, sql: str) -> str:
        return self.execute(sql + " FORMAT TabSeparatedRaw")

    def rows(self, sql: str):
        result = self.execute(sql + " FORMAT JSONEachRow")
        return [json.loads(line) for line in result.splitlines() if line]


@contextmanager
def refresh_lock():
    # A PostgreSQL session lock covers both scheduled and manual invocations.
    # No application rows are modified. A killed process releases its session.
    import psycopg2
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres-main"),
        port=os.getenv("POSTGRES_INTERNAL_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "ecommerce_db"),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "secret"),
        connect_timeout=10,
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_KEY,))
            if not cur.fetchone()[0]:
                raise RuntimeError("Another warehouse refresh is running; retry later")
        token = LOCK_CONNECTION.set(conn)
        try:
            yield
        finally:
            LOCK_CONNECTION.reset(token)
    finally:
        conn.close()


def assert_lock():
    conn = LOCK_CONNECTION.get()
    if conn is None or conn.closed:
        raise RuntimeError("A live warehouse advisory lock is required")
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' "
                    "AND pid=pg_backend_pid() AND classid=0 AND objid=%s "
                    "AND objsubid=1 AND granted)", (LOCK_KEY,))
        if not cur.fetchone()[0]:
            raise RuntimeError("Warehouse advisory lock was lost; refusing publication")


class Refresher:
    def __init__(self, ch=None, sql_dir=SQL_DIR):
        self.ch = ch or ClickHouse()
        self.sql_dir = Path(sql_dir)
        self.silver = definitions("silver.sql", self.sql_dir)
        self.gold = definitions("gold.sql", self.sql_dir)
        self.stage = ""
        self.snapshot = ""

    def event(self, status, details=None):
        row = {"snapshot_id": self.snapshot, "at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
               "status": status, "details": json.dumps(details or {}, ensure_ascii=False)}
        self.ch.execute("INSERT INTO warehouse.refresh_runs FORMAT JSONEachRow\n" + json.dumps(row))

    def initialize_control(self):
        for db in ("warehouse", "silver", "gold", "report"):
            self.ch.execute(f"CREATE DATABASE IF NOT EXISTS {db} ENGINE = Atomic")
            engine = self.ch.scalar(f"SELECT engine FROM system.databases WHERE name='{db}'")
            if engine != "Atomic":
                raise RuntimeError(f"{db} must use the Atomic database engine")
        self.ch.execute("""CREATE TABLE IF NOT EXISTS warehouse.active_snapshot
            (snapshot_id UUID, published_at DateTime('UTC')) ENGINE=MergeTree ORDER BY tuple()""")
        self.ch.execute("""CREATE TABLE IF NOT EXISTS warehouse.refresh_runs
            (snapshot_id UUID, at DateTime('UTC'), status LowCardinality(String), details String)
            ENGINE=MergeTree ORDER BY (at, snapshot_id)""")
        self.ch.execute("""CREATE TABLE IF NOT EXISTS warehouse.legacy_definitions
            (database String, name String, ddl String, saved_at DateTime DEFAULT now())
            ENGINE=MergeTree ORDER BY (database, name)""")

    def active(self):
        return self.ch.scalar("SELECT snapshot_id FROM warehouse.active_snapshot LIMIT 1")

    def build_stage(self):
        self.ch.execute(f"CREATE DATABASE {self.stage} ENGINE=Atomic")
        # Resolve CDC versions exactly once, then freeze each source table for
        # all subsequent joins. This is not a cross-topic CDC transaction barrier.
        sources = sorted(set(re.findall(r"bronze\.(\w+)", " ".join(q for _, _, q in self.silver))))
        offsets = {}
        for name in sources:
            self.ch.execute(f"CREATE TABLE {self.stage}.raw_{name} ENGINE=MergeTree ORDER BY id "
                            f"AS SELECT * FROM bronze.{name} FINAL")
            offsets[name] = self.ch.rows(
                f"SELECT count() AS rows, max(_version) AS max_offset FROM {self.stage}.raw_{name}")[0]
        for db, name, query in self.silver + self.gold:
            query = re.sub(r"bronze\.(\w+)", lambda m: f"{self.stage}.raw_{m[1]}", query)
            query = re.sub(r"\bFINAL\b", "", query)
            query = re.sub(r"\b(silver|gold)\.(\w+)",
                           lambda m: f"{self.stage}.{'s' if m[1]=='silver' else 'g'}_{m[2]}", query)
            # Build physical Silver once; Gold helpers may be views within the
            # private stage, but public facts/dimensions are always stored tables.
            kind = "TABLE" if db == "silver" or name in GOLD_KEYS or name == "dim_date" else "VIEW"
            engine = " ENGINE=MergeTree ORDER BY tuple()" if kind == "TABLE" else ""
            self.ch.execute(f"CREATE {kind} {self.stage}.{'s' if db=='silver' else 'g'}_{name}{engine} AS {query}")
        return offsets

    def validate(self):
        s = self.stage
        checks = {
            "orphan_order_items": f"SELECT count() FROM {s}.s_order_items i LEFT JOIN {s}.s_orders o ON i.order_id=o.order_id WHERE o.order_id IS NULL",
            "missing_product": f"SELECT count() FROM {s}.s_order_items i LEFT JOIN {s}.s_products p ON i.product_id=p.product_id WHERE p.product_id IS NULL",
            "missing_status": f"SELECT count() FROM {s}.s_orders o LEFT JOIN {s}.s_order_status st ON o.order_status_id=st.id WHERE st.id IS NULL",
            "invalid_lines": f"SELECT count() FROM {s}.s_order_items WHERE quantity<=0 OR current_price<0 OR subtotal_amount<0",
            "incomplete_orders": f"""SELECT count() FROM {s}.s_orders o LEFT JOIN
                (SELECT order_id, sum(subtotal_amount) amount FROM {s}.s_order_items GROUP BY order_id) i
                ON o.order_id=i.order_id WHERE i.order_id IS NULL OR abs(o.order_amount-i.amount)>0.02
                OR o.discount_amount<0 OR o.discount_amount>o.order_amount
                OR abs(o.total_amount-(o.order_amount-o.discount_amount))>0.02""",
            "discount_allocation": f"""SELECT count() FROM (SELECT order_id, any(discount_amount) expected,
                sum(discount_gross) actual FROM {s}.g_sales_line_measured GROUP BY order_id HAVING abs(expected-actual)>0.001)""",
        }
        for name, key in SILVER_KEYS.items():
            checks[f"duplicate_{name}"] = f"SELECT count()-uniqExact({key}) FROM {s}.s_{name}"
        results = {name: int(self.ch.scalar(query)) for name, query in checks.items()}
        if any(results.values()):
            raise RuntimeError("Warehouse validation failed; CDC may not have caught up: " + json.dumps(results))
        return results

    def ensure_tables(self, migrate=False):
        legacy = self.ch.rows("SELECT database, name, create_table_query AS ddl FROM system.tables "
                              "WHERE database IN ('silver','gold') AND engine='View'")
        if legacy and not migrate:
            raise RuntimeError("Legacy views detected. Pause the old ETL DAG and run scripts/refresh_warehouse.sh --migrate")
        for row in legacy:
            self.ch.execute("INSERT INTO warehouse.legacy_definitions (database,name,ddl) FORMAT JSONEachRow\n" + json.dumps(row))
        # Migration affects derived layers only. Bronze, Kafka offsets and
        # PostgreSQL source data are never reset. A maintenance window is needed.
        for row in sorted(legacy, key=lambda r: r["database"]):
            self.ch.execute(f"DROP VIEW IF EXISTS {row['database']}.{row['name']}")
        for db, keys, prefix in (("silver", SILVER_KEYS, "s"), ("gold", GOLD_KEYS, "g")):
            for name, key in keys.items():
                self.ch.execute(f"CREATE TABLE IF NOT EXISTS {db}.{name} ENGINE=MergeTree "
                                f"PARTITION BY snapshot_id ORDER BY (snapshot_id, {key}) "
                                f"AS SELECT *, toUUID('{self.snapshot}') AS snapshot_id "
                                f"FROM {self.stage}.{prefix}_{name} LIMIT 0")
                engine = self.ch.scalar(f"SELECT engine FROM system.tables WHERE database='{db}' AND name='{name}'")
                if engine != "MergeTree":
                    raise RuntimeError(f"Unexpected engine for {db}.{name}: {engine}")
        self.ch.execute(f"CREATE TABLE IF NOT EXISTS gold.dim_date ENGINE=MergeTree ORDER BY date_key "
                        f"AS SELECT * FROM {self.stage}.g_dim_date")

    def store(self):
        counts = {}
        for db, keys, prefix in (("silver", SILVER_KEYS, "s"), ("gold", GOLD_KEYS, "g")):
            for name in keys:
                self.ch.execute(f"INSERT INTO {db}.{name} SELECT *, toUUID('{self.snapshot}') AS snapshot_id "
                                f"FROM {self.stage}.{prefix}_{name}")
                count = int(self.ch.scalar(f"SELECT count() FROM {db}.{name} WHERE snapshot_id=toUUID('{self.snapshot}')"))
                expected = int(self.ch.scalar(f"SELECT count() FROM {self.stage}.{prefix}_{name}"))
                if count != expected:
                    raise RuntimeError(f"Row reconciliation failed for {db}.{name}: {count} != {expected}")
                counts[f"{db}.{name}"] = count
        return counts

    def reporting(self):
        for _, name, query in definitions("reporting.sql", self.sql_dir):
            self.ch.execute(f"CREATE OR REPLACE VIEW report.{name} AS {query}")

    def publish(self):
        assert_lock()
        pointer = f"warehouse.pointer_{self.snapshot.replace('-', '')}"
        self.ch.execute(f"CREATE TABLE {pointer} AS warehouse.active_snapshot")
        self.ch.execute(f"INSERT INTO {pointer} VALUES (toUUID('{self.snapshot}'), now())")
        self.ch.execute(f"EXCHANGE TABLES warehouse.active_snapshot AND {pointer}")
        # Never exchange back automatically: a successful publish may outlive
        # an HTTP timeout. The active pointer is the authoritative commit marker.
        return pointer

    def refresh(self, migrate=False):
        assert_lock()
        started = time.monotonic()
        self.snapshot = str(uuid.uuid4())
        self.stage = "warehouse_stage_" + self.snapshot.replace("-", "")
        self.initialize_control()
        self.event("building")
        pointer = None
        try:
            offsets = self.build_stage()
            validations = self.validate()
            self.ensure_tables(migrate)
            counts = self.store()
            self.reporting()
            pointer = "warehouse.pointer_" + self.snapshot.replace("-", "")
            self.publish()
            result = {"snapshot_id": self.snapshot, "status": "published", "rows": counts,
                      "validation": validations, "source_versions": offsets,
                      "seconds": round(time.monotonic()-started, 3)}
            self.event("published", result)
            LOG.info("Warehouse published: %s", json.dumps(result))
            return result
        except Exception as exc:
            if self.active() == self.snapshot:
                # Publication succeeded but acknowledgement/audit may have failed.
                LOG.exception("Snapshot is published; follow-up operation failed")
                return {"snapshot_id": self.snapshot, "status": "published", "warning": str(exc)}
            try:
                self.event("failed", {"error": str(exc)[:4000]})
                self.discard(self.snapshot)
            except Exception:
                LOG.exception("Failed to clean unpublished generation; active reports remain unchanged")
            raise
        finally:
            try:
                self.ch.execute(f"DROP DATABASE IF EXISTS {self.stage} SYNC")
                if pointer:
                    self.ch.execute(f"DROP TABLE IF EXISTS {pointer} SYNC")
            except Exception:
                LOG.exception("Temporary objects could not be cleaned")

    def discard(self, snapshot):
        if snapshot == self.active():
            raise RuntimeError("Refusing to discard the active generation")
        for db, keys in (("silver", SILVER_KEYS), ("gold", GOLD_KEYS)):
            for name in keys:
                engine = self.ch.scalar(f"SELECT engine FROM system.tables WHERE database='{db}' AND name='{name}'")
                if engine == "MergeTree":
                    self.ch.execute(f"ALTER TABLE {db}.{name} DROP PARTITION '{snapshot}'")

    def cleanup(self, hours):
        assert_lock()
        if hours < 1:
            raise ValueError("Retention must be at least 1 hour to protect in-flight readers")
        self.initialize_control()
        # Query execution is capped at 240s; published generations live >=1h.
        rows = self.ch.rows(f"SELECT snapshot_id FROM warehouse.refresh_runs GROUP BY snapshot_id "
                            f"HAVING max(at) < now()-INTERVAL {int(hours)} HOUR")
        partitions = {row["partition"] for row in self.ch.rows(
            "SELECT DISTINCT partition FROM system.parts WHERE active "
            "AND database IN ('silver','gold') AND table != 'dim_date'")}
        # Audit history outlives data retention; do not issue DROP PARTITION
        # repeatedly for generations whose physical data is already gone.
        rows = [row for row in rows if row["snapshot_id"] in partitions]
        active = self.active()
        for row in rows:
            if row["snapshot_id"] != active:
                self.discard(row["snapshot_id"])
        # A killed process can leave private staging DBs and pointer tables.
        # The shared advisory lock guarantees no refresh is building right now.
        for row in self.ch.rows("SELECT name FROM system.databases WHERE startsWith(name,'warehouse_stage_')"):
            if re.fullmatch(r"warehouse_stage_[a-f0-9]{32}", row["name"]):
                self.ch.execute(f"DROP DATABASE {row['name']} SYNC")
        for row in self.ch.rows("SELECT name FROM system.tables WHERE database='warehouse' AND startsWith(name,'pointer_')"):
            if re.fullmatch(r"pointer_[a-f0-9]{32}", row["name"]):
                self.ch.execute(f"DROP TABLE warehouse.{row['name']} SYNC")
        return {"expired_generations": len(rows), "retention_hours": hours}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--migrate", action="store_true", help="Replace legacy derived views with physical tables")
    parser.add_argument("--cleanup-hours", type=int, help="Remove inactive generations older than this (minimum 1)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    with refresh_lock():
        job = Refresher()
        result = job.cleanup(args.cleanup_hours) if args.cleanup_hours is not None else job.refresh(args.migrate)
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
