"""Incremental Silver upserts and atomic publication of affected Gold dates."""
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
SQL_DIR = Path(os.getenv("WAREHOUSE_SQL_DIR", Path(__file__).resolve().parents[1] / "clickhouse/sql"))
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
            "max_partitions_per_insert_block": 1000, "max_query_size": 10485760,
            "max_bytes_before_external_group_by": 134217728, "max_bytes_before_external_sort": 134217728,
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
    # Serialize scheduled and manual refreshes with a session lock.
    import psycopg2
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres-main"),
        port=os.getenv("POSTGRES_INTERNAL_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
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


SENTINEL = "1970-01-01"
DIMENSIONS = {"dim_products", "dim_locations", "dim_campaigns", "dim_order_status"}
SOURCES = ["users", "orders", "orderdetails", "addresses", "discounts", "paymentmethods",
           "shippingmethods", "orderstatus", "products", "brands", "categories", "provinces",
           "regions", "adscampaigns"]


class Refresher:
    def __init__(self, ch=None, sql_dir=SQL_DIR):
        self.ch = ch or ClickHouse()
        self.sql_dir = Path(sql_dir)
        self.silver = definitions("silver.sql", self.sql_dir)
        self.gold = definitions("gold.sql", self.sql_dir)
        self.stage = ""
        self.snapshot = ""
        self.version = 0
        self.full = False
        self._key_cache = {}

    def ids(self, values):
        key = frozenset(int(v) for v in values)
        if not key: return '0'
        if key not in self._key_cache:
            table = f"{self.stage}.keys_{len(self._key_cache)}"
            self.ch.execute(f"CREATE TABLE {table} (id UInt32) ENGINE=MergeTree ORDER BY id")
            self.ch.execute(f"INSERT INTO {table} FORMAT TabSeparated\n"+'\n'.join(str(v) for v in sorted(key)))
            self._key_cache[key] = table
        return 'SELECT id FROM '+self._key_cache[key]

    def initialize_control(self):
        for db in ("warehouse", "silver", "gold", "report"):
            self.ch.execute(f"CREATE DATABASE IF NOT EXISTS {db} ENGINE=Atomic")
        self.ch.execute("""CREATE TABLE IF NOT EXISTS warehouse.active_snapshot
            (day Date, snapshot_id UUID, published_at DateTime64(3, 'UTC'), offsets String)
            ENGINE=MergeTree ORDER BY day""")
        self.ch.execute("""CREATE TABLE IF NOT EXISTS warehouse.refresh_runs
            (snapshot_id UUID, at DateTime64(3, 'UTC'), status LowCardinality(String), details String)
            ENGINE=MergeTree ORDER BY (at, snapshot_id)""")
        self.ch.execute("""CREATE TABLE IF NOT EXISTS warehouse.pending_orders
            (order_id UInt32, first_seen DateTime64(3, 'UTC'), active UInt8, version UInt64)
            ENGINE=ReplacingMergeTree(version) ORDER BY order_id""")
        # Preserve affected dates across failures after Silver updates.
        self.ch.execute("""CREATE TABLE IF NOT EXISTS warehouse.pending_days
            (day Date) ENGINE=MergeTree ORDER BY day""")
        self.ch.execute("""CREATE TABLE IF NOT EXISTS bronze.change_log
            (source LowCardinality(String), offset UInt64, id UInt32, order_id UInt32,
             archived_at DateTime64(3, 'UTC'))
            ENGINE=MergeTree ORDER BY (source, offset, id)""")
        self.ch.execute("""CREATE TABLE IF NOT EXISTS warehouse.control_flags
            (name String, value UInt8, version UInt64) ENGINE=ReplacingMergeTree(version) ORDER BY name""")
        # Track changes independently of typed-consumer progress.
        journal_select = """SELECT replaceAll(JSONExtractString(payload,'__table'),'_','') AS source,
            kafka_offset AS offset, toUInt32(JSONExtractUInt(payload,'id')) AS id,
            if(source='orders',id,toUInt32(JSONExtractUInt(payload,'order_id'))) AS order_id,
            now64(3) AS archived_at FROM bronze.raw_events
            WHERE isValidJSON(payload) AND JSONExtractString(payload,'__op') IN ('c','r','u','d')
            AND source IN (""" + ','.join("'"+v+"'" for v in SOURCES) + ')'
        ready = self.ch.scalar("SELECT ifNull(maxOrNull(value),0) FROM "
                               "(SELECT value FROM warehouse.control_flags FINAL WHERE name='journal_ready')") == '1'
        if not ready:
            self.ch.execute(f"INSERT INTO warehouse.control_flags VALUES ('bootstrap_required',1,{time.time_ns()})")
            self.ch.execute('CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.changes_raw TO bronze.change_log AS '+journal_select)
            self.ch.execute('INSERT INTO bronze.change_log '+journal_select)
            self.ch.execute(f"INSERT INTO warehouse.control_flags VALUES ('journal_ready',1,{time.time_ns()})")
        partitions = int(self.ch.scalar("SELECT count() FROM system.kafka_consumers "
                                        "WHERE database='bronze' AND arrayExists(p -> p != 0, `assignments.partition_id`)"))
        if partitions:
            raise RuntimeError('Multiple Kafka partitions detected: topic-offset versioning requires one partition')

    def event(self, status, details):
        row = dict(snapshot_id=self.snapshot, at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
                   status=status, details=json.dumps(details, ensure_ascii=False))
        self.ch.execute("INSERT INTO warehouse.refresh_runs FORMAT JSONEachRow\n" + json.dumps(row))

    def checkpoint(self):
        rows = self.ch.rows(f"SELECT offsets FROM warehouse.active_snapshot WHERE day='{SENTINEL}'")
        return json.loads(rows[0]["offsets"]) if rows else None

    def capture(self, settle=False):
        seconds = 5 if settle else 0
        predicate = f" AND archived_at<=now64(3)-INTERVAL {seconds} SECOND" if seconds else ''
        # Read the newest eligible offset using the journal sort key.
        result = {}
        for source in SOURCES:
            value = self.ch.scalar(f"SELECT offset FROM bronze.change_log WHERE source='{source}'{predicate} "
                                   "ORDER BY offset DESC LIMIT 1")
            result[source] = int(value) if value else -1
        return result

    def migrate(self):
        """Archive the old full-snapshot derived layers; never modify source/Bronze."""
        columns = self.ch.rows("SELECT name FROM system.columns WHERE database='warehouse' "
                               "AND table='active_snapshot'")
        legacy_views = int(self.ch.scalar("SELECT count() FROM system.tables WHERE database='silver' AND engine='View'"))
        old = (columns and "day" not in {r["name"] for r in columns}) or legacy_views
        if not old:
            return []
        suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        moved = []
        for db in ("silver", "gold", "warehouse"):
            rows = self.ch.rows(f"SELECT name, engine FROM system.tables WHERE database='{db}'")
            if not rows:
                continue
            backup = f"{db}_before_incremental_{suffix}"
            self.ch.execute(f"CREATE DATABASE {backup} ENGINE=Atomic")
            for row in rows:
                self.ch.execute(f"RENAME TABLE {db}.{row['name']} TO {backup}.{row['name']}")
            moved.append(backup)
        LOG.warning("Derived-layer backups retained: %s", moved)
        return moved

    def plan(self, checkpoint, target, full):
        # Keep full-load key selection inside ClickHouse.
        if full:
            self.pending = {int(r['order_id']): r['first_seen'] for r in self.ch.rows(
                "SELECT order_id,first_seen FROM warehouse.pending_orders FINAL WHERE active=1")}
            days = {r['day'] for table in ('orders', 'users') for r in self.ch.rows(
                f"SELECT DISTINCT toDate(toTimeZone(created_at,'Asia/Ho_Chi_Minh')) AS day "
                f"FROM bronze.{table} FINAL WHERE _deleted=0")}
            days.update(r['day'] for r in self.ch.rows(
                f"SELECT day FROM warehouse.active_snapshot WHERE day!='{SENTINEL}'"))
            return {source: set() for source in SOURCES}, set(), days
        changed = {}
        for source in SOURCES:
            rows = self.ch.rows(f"SELECT DISTINCT id FROM bronze.change_log WHERE source='{source}' "
                                f"AND offset>{checkpoint.get(source, -1)} AND offset<={target[source]}")
            changed[source] = {int(r["id"]) for r in rows}
        orders = set(changed["orders"])
        self.pending = {int(r['order_id']):r['first_seen'] for r in self.ch.rows(
            "SELECT order_id,first_seen FROM warehouse.pending_orders FINAL WHERE active=1")}
        orders.update(self.pending)
        line_ids = self.ids(changed["orderdetails"])
        if changed["orderdetails"]:
            orders.update(int(r["order_id"]) for r in self.ch.rows(
                f"SELECT order_id FROM bronze.orderdetails FINAL WHERE id IN ({line_ids})"))
            orders.update(int(r["order_id"]) for r in self.ch.rows(
                f"SELECT order_id FROM silver.order_items FINAL WHERE order_item_id IN ({line_ids})"))
        # Resolve mapping changes; dimension labels use Type 1 updates.
        for source, key in [("addresses", "address_id"), ("discounts", "discount_id"),
                            ("paymentmethods", "payment_method_id"), ("shippingmethods", "shipping_method_id"),
                            ("orderstatus", "order_status_id")]:
            if changed[source]:
                orders.update(int(r["id"]) for r in self.ch.rows(
                    f"SELECT id FROM bronze.orders FINAL WHERE {key} IN ({self.ids(changed[source])})"))
        days = {r["day"] for r in self.ch.rows("SELECT DISTINCT day FROM warehouse.pending_days")}
        for table, key, ids, expression in [
            ("orders", "order_id", orders, "toDate(toTimeZone(created_at,'Asia/Ho_Chi_Minh'))"),
            ("users", "user_id", changed["users"], "registration_date")]:
            if ids:
                days.update(r["day"] for r in self.ch.rows(
                    f"SELECT DISTINCT {expression} AS day FROM silver.{table} FINAL "
                    f"WHERE {key} IN ({self.ids(ids)}) AND _is_deleted=0"))
        for table, ids in [("orders", orders), ("users", changed["users"])]:
            if ids:
                days.update(r["day"] for r in self.ch.rows(
                    f"SELECT DISTINCT toDate(toTimeZone(created_at,'Asia/Ho_Chi_Minh')) AS day "
                    f"FROM bronze.{table} FINAL WHERE id IN ({self.ids(ids)}) AND _deleted=0"))
        return changed, orders, days

    def archive_barrier(self, checkpoint, target):
        missing = {}
        for source in SOURCES:
            if target[source] <= checkpoint.get(source,-1): continue
            required = f'{self.stage}.required_{source}'
            self.ch.execute(f"CREATE TABLE {required} ENGINE=MergeTree ORDER BY id AS "
                            f"SELECT id,max(offset) AS required_offset FROM bronze.change_log WHERE source='{source}' "
                            f"AND offset>{checkpoint.get(source,-1)} AND offset<={target[source]} GROUP BY id")
            count = int(self.ch.scalar(f"SELECT count() FROM {required} r LEFT JOIN "
                f"(SELECT id,_version FROM bronze.{source} FINAL WHERE id IN (SELECT id FROM {required})) b "
                f"ON r.id=b.id WHERE b.id IS NULL OR b._version<r.required_offset"))
            if count: missing[source]=count
        if missing:
            raise RuntimeError('CDC archive barrier waiting for typed consumers: '+json.dumps(missing))

    def build_silver_stage(self, changed, orders):
        for source in SOURCES:
            condition = ""
            if not self.full and source == "orders": condition = f" WHERE id IN ({self.ids(orders)})"
            elif not self.full and source == "orderdetails": condition = f" WHERE order_id IN ({self.ids(orders)})"
            elif not self.full and source == "users": condition = f" WHERE id IN ({self.ids(changed['users'])})"
            self.ch.execute(f"CREATE TABLE {self.stage}.raw_{source} ENGINE=MergeTree ORDER BY id "
                            f"AS SELECT * FROM bronze.{source} FINAL{condition}")
        for _, name, query in self.silver:
            query = re.sub(r"bronze\.(\w+)", lambda m: f"{self.stage}.raw_{m[1]}", query)
            query = re.sub(r"\bFINAL\b", "", query)
            self.ch.execute(f"CREATE TABLE {self.stage}.s_{name} ENGINE=MergeTree ORDER BY tuple() AS {query}")

    def ensure_silver(self):
        for name, key in SILVER_KEYS.items():
            self.ch.execute(f"CREATE TABLE IF NOT EXISTS silver.{name} "
                            f"ENGINE=ReplacingMergeTree(_batch_version) ORDER BY {key} "
                            f"AS SELECT *, toUInt64(0) AS _batch_version, toUInt8(0) AS _is_deleted "
                            f"FROM {self.stage}.s_{name} LIMIT 0")
        for table, column in [("orders", "created_at"), ("order_items", "order_id"), ("users", "registration_date")]:
            self.ch.execute(f"ALTER TABLE silver.{table} ADD INDEX IF NOT EXISTS ix_{column} {column} TYPE minmax GRANULARITY 1")

    def validate(self, defer_incomplete=False):
        s = self.stage
        conditions = {
            "orphan_order_items": f"SELECT DISTINCT i.order_id FROM {s}.s_order_items i LEFT JOIN {s}.s_orders o ON i.order_id=o.order_id WHERE o.order_id IS NULL",
            "missing_product": f"SELECT DISTINCT i.order_id FROM {s}.s_order_items i LEFT JOIN {s}.s_products p ON i.product_id=p.product_id WHERE p.product_id IS NULL",
            "missing_status": f"SELECT DISTINCT o.order_id FROM {s}.s_orders o LEFT JOIN {s}.s_order_status st ON o.order_status_id=st.id WHERE st.id IS NULL",
            "invalid_lines": f"SELECT DISTINCT order_id FROM {s}.s_order_items WHERE quantity<=0 OR current_price<0 OR subtotal_amount<0",
            "incomplete_orders": f"""SELECT DISTINCT o.order_id FROM {s}.s_orders o LEFT JOIN
                (SELECT order_id, sum(subtotal_amount) amount FROM {s}.s_order_items GROUP BY order_id) i
                ON o.order_id=i.order_id WHERE i.order_id IS NULL OR abs(o.order_amount-i.amount)>0.02
                OR o.discount_amount<0 OR o.discount_amount>o.order_amount
                OR abs(o.total_amount-(o.order_amount-o.discount_amount))>0.02""",
        }
        values = {}; bad = set()
        for name, query in conditions.items():
            ids = {int(r['order_id']) for r in self.ch.rows(query)}
            values[name] = len(ids);bad.update(ids)
        for name, key in SILVER_KEYS.items():
            values[f'duplicate_{name}'] = int(self.ch.scalar(f"SELECT count()-uniqExact({key}) FROM {s}.s_{name}"))
        if any(v for k,v in values.items() if k.startswith('duplicate_')):
            raise RuntimeError('Duplicate business keys: '+json.dumps(values))
        self.deferred = bad
        if bad:
            now = datetime.now(timezone.utc)
            oldest = min((datetime.fromisoformat(self.pending[i]).replace(tzinfo=timezone.utc)
                          for i in bad if i in self.pending),default=now)
            grace = 120
            if not defer_incomplete or (now-oldest).total_seconds()>grace:
                raise RuntimeError('CDC incomplete or invalid; reports/checkpoint retained: '+json.dumps(values))
            records=[dict(order_id=i,first_seen=self.pending.get(i,now.strftime('%Y-%m-%d %H:%M:%S.%f')),
                          active=1,version=self.version) for i in sorted(bad)]
            self.ch.execute('INSERT INTO warehouse.pending_orders FORMAT JSONEachRow\n'+'\n'.join(map(json.dumps,records)))
            selection = self.ids(bad)
            for name in ['orders','order_items']:
                self.ch.execute(f'RENAME TABLE {s}.s_{name} TO {s}.unready_{name}')
                self.ch.execute(f'CREATE TABLE {s}.s_{name} ENGINE=MergeTree ORDER BY tuple() AS '
                                f'SELECT * FROM {s}.unready_{name} WHERE order_id NOT IN ({selection})')
        return values

    def acknowledge_ready(self, orders):
        ready = (set(self.pending) & orders) - self.deferred
        if ready:
            records=[dict(order_id=i,first_seen=self.pending[i],active=0,version=self.version+1) for i in sorted(ready)]
            self.ch.execute('INSERT INTO warehouse.pending_orders FORMAT JSONEachRow\n'+'\n'.join(map(json.dumps,records)))

    def upsert_silver(self, changed, orders):
        for name, key in SILVER_KEYS.items():
            if name == "orders": condition = f"{key} IN ({self.ids(orders)})"
            elif name == "order_items": condition = f"order_id IN ({self.ids(orders)})"
            elif name == "users": condition = f"{key} IN ({self.ids(changed['users'])})"
            else: condition = "1"
            if self.full: condition = "1"
            columns = [r['name'] for r in self.ch.rows(f"DESCRIBE TABLE {self.stage}.s_{name}")]
            selected = ','.join(columns)
            # Tombstones remove deleted or moved keys; retries replace their state.
            self.ch.execute(f"INSERT INTO silver.{name} SELECT {selected}, toUInt64({self.version}), toUInt8(1) "
                            f"FROM silver.{name} FINAL WHERE ({condition}) AND _is_deleted=0 "
                            f"AND {key} NOT IN (SELECT {key} FROM {self.stage}.s_{name})")
            self.ch.execute(f"INSERT INTO silver.{name} SELECT *, toUInt64({self.version}), toUInt8(0) FROM {self.stage}.s_{name}")

    def build_gold(self, days):
        dates = ','.join("'"+d+"'" for d in sorted(days)) or "'1970-01-01'"
        # Freeze only affected dates, not the full transactional history.
        for name, predicate in [
            ("orders", f"toDate(toTimeZone(created_at,'Asia/Ho_Chi_Minh')) IN ({dates})"),
            ("users", f"registration_date IN ({dates})"),
            ("order_items", f"order_id IN (SELECT order_id FROM {self.stage}.s_orders)")]:
            self.ch.execute(f"DROP TABLE {self.stage}.s_{name} SYNC")
            self.ch.execute(f"CREATE TABLE {self.stage}.s_{name} ENGINE=MergeTree ORDER BY tuple() AS "
                            f"SELECT * EXCEPT (_batch_version, _is_deleted) FROM silver.{name} FINAL "
                            f"WHERE _is_deleted=0 AND {predicate}")
        for _, name, query in self.gold:
            query = re.sub(r"\b(silver|gold)\.(\w+)",
                           lambda m: f"{self.stage}.{'s' if m[1]=='silver' else 'g'}_{m[2]}", query)
            self.ch.execute(f"CREATE TABLE {self.stage}.g_{name} ENGINE=MergeTree ORDER BY tuple() AS {query}")
        mismatch = int(self.ch.scalar(f"""SELECT count() FROM
            (SELECT order_id, any(discount_amount) expected, sum(discount_gross) actual
             FROM {self.stage}.g_sales_line_measured GROUP BY order_id HAVING abs(expected-actual)>0.001)"""))
        if mismatch: raise RuntimeError("Discount allocation reconciliation failed")

    def store_gold(self):
        counts = {}
        for name, key in GOLD_KEYS.items():
            expression = "registration_date" if name == "FACT_USER_REGISTRATION" else (
                "toDate(toTimeZone(created_at,'Asia/Ho_Chi_Minh'))" if name == "sales_line_measured" else "date_key")
            partition = "snapshot_id" if name in DIMENSIONS else f"({expression}, snapshot_id)"
            self.ch.execute(f"CREATE TABLE IF NOT EXISTS gold.{name} ENGINE=MergeTree "
                            f"PARTITION BY {partition} ORDER BY (snapshot_id, {key}) "
                            f"AS SELECT *, toUUID('{self.snapshot}') AS snapshot_id FROM {self.stage}.g_{name} LIMIT 0")
            self.ch.execute(f"INSERT INTO gold.{name} SELECT *, toUUID('{self.snapshot}') FROM {self.stage}.g_{name}")
            count = int(self.ch.scalar(f"SELECT count() FROM gold.{name} WHERE snapshot_id=toUUID('{self.snapshot}')"))
            expected = int(self.ch.scalar(f"SELECT count() FROM {self.stage}.g_{name}"))
            if count != expected: raise RuntimeError(f"Gold reconciliation failed: {name}")
            counts[name] = count
        self.ch.execute(f"CREATE TABLE IF NOT EXISTS gold.dim_date ENGINE=MergeTree ORDER BY date_key "
                        f"AS SELECT * FROM {self.stage}.g_dim_date")
        return counts

    def reporting(self):
        for _, name, query in definitions("reporting.sql", self.sql_dir):
            self.ch.execute(f"CREATE OR REPLACE VIEW report.{name} AS {query}")

    def published(self):
        return self.ch.scalar(f"SELECT count() FROM warehouse.active_snapshot WHERE day='{SENTINEL}' "
                              f"AND snapshot_id=toUUID('{self.snapshot}')") == "1"

    def publish(self, days, target):
        assert_lock()
        pointer = 'warehouse.pointer_' + self.snapshot.replace('-', '')
        self.ch.execute(f"CREATE TABLE {pointer} AS warehouse.active_snapshot")
        excluded = ','.join("'"+d+"'" for d in sorted(days | {SENTINEL}))
        self.ch.execute(f"INSERT INTO {pointer} SELECT * FROM warehouse.active_snapshot WHERE day NOT IN ({excluded})")
        stamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
        payload = '\n'.join(json.dumps(dict(day=d,snapshot_id=self.snapshot,published_at=stamp,offsets=json.dumps(target)))
                            for d in sorted(days | {SENTINEL}))
        self.ch.execute(f"INSERT INTO {pointer} FORMAT JSONEachRow\n"+payload)
        self.ch.execute(f"EXCHANGE TABLES warehouse.active_snapshot AND {pointer}")
        self.ch.execute(f"DROP TABLE {pointer} SYNC")

    def refresh(self, migrate=False, full=False, defer_incomplete=True):
        assert_lock()
        backups = self.migrate() if migrate else []
        self.initialize_control()
        checkpoint = self.checkpoint()
        bootstrap = self.ch.scalar("SELECT ifNull(maxOrNull(value),0) FROM "
                                   "(SELECT value FROM warehouse.control_flags FINAL WHERE name='bootstrap_required')") == '1'
        full = full or checkpoint is None or bootstrap
        self.full = full
        target = self.capture(settle=not full)
        if checkpoint:
            target = {s:max(v,checkpoint.get(s,-1)) for s,v in target.items()}
        if (not full and target == checkpoint and self.ch.scalar('SELECT count() FROM warehouse.pending_days') == '0'
                and self.ch.scalar('SELECT count() FROM warehouse.pending_orders FINAL WHERE active=1') == '0'):
            return {"status": "idle", "mode": "incremental", "changed_orders": 0}
        self.snapshot = str(uuid.uuid4())
        self.stage = 'warehouse_stage_' + self.snapshot.replace('-', '')
        self.version = time.time_ns()
        started = time.monotonic()
        self.event('building', {'mode': 'full' if full else 'incremental'})
        try:
            self.ch.execute(f'CREATE DATABASE {self.stage} ENGINE=Atomic')
            changed, orders, days = self.plan(checkpoint or {}, target, full)
            if days:
                self.ch.execute('INSERT INTO warehouse.pending_days FORMAT JSONEachRow\n' +
                                '\n'.join(json.dumps({'day':d}) for d in sorted(days)))
            self.archive_barrier({} if full else checkpoint or {}, target)
            self.build_silver_stage(changed, orders)
            self.ensure_silver()
            checks = self.validate(defer_incomplete=defer_incomplete and not full)
            ready_orders = (set(self.pending) if full else orders) - self.deferred
            changed_count = (int(self.ch.scalar(f"SELECT count() FROM {self.stage}.s_orders"))
                             if full else len(ready_orders))
            self.upsert_silver(changed, ready_orders)
            self.build_gold(days)
            counts = self.store_gold()
            self.reporting()
            self.publish(days, target)
            if full:
                self.ch.execute(f"INSERT INTO warehouse.control_flags VALUES ('bootstrap_required',0,{time.time_ns()})")
            self.acknowledge_ready(ready_orders)
            self.ch.execute('TRUNCATE TABLE warehouse.pending_days')
            result = dict(status='published',mode='full' if full else 'incremental',snapshot_id=self.snapshot,
                          changed_orders=changed_count, deferred_orders=len(self.deferred), affected_days=sorted(days), rows=counts, validation=checks,
                          offsets=target, seconds=round(time.monotonic()-started,3), backups=backups)
            self.event('published', result)
            LOG.info('Warehouse published: %s',json.dumps(result))
            return result
        except Exception as exc:
            if self.published():
                return dict(status='published',snapshot_id=self.snapshot,warning=str(exc))
            self.event('failed',{'error':str(exc)[:4000]})
            raise
        finally:
            try: self.ch.execute(f'DROP DATABASE IF EXISTS {self.stage} SYNC')
            except Exception: LOG.exception('Stage cleanup failed')

    def cleanup(self, hours):
        assert_lock()
        if hours < 1: raise ValueError('Retention must be >= 1 hour')
        self.initialize_control()
        # Only remove partitions that are both old and no longer referenced.
        old = self.ch.rows(f"SELECT snapshot_id FROM warehouse.refresh_runs GROUP BY snapshot_id "
                           f"HAVING max(at)<now()-INTERVAL {int(hours)} HOUR")
        current = self.ch.rows('SELECT day,snapshot_id FROM warehouse.active_snapshot')
        active_pairs = {(r['day'],r['snapshot_id']) for r in current}
        dim_version = next((r['snapshot_id'] for r in current if r['day']==SENTINEL),None)
        old_versions = {r['snapshot_id'] for r in old}
        removed = 0
        for name in GOLD_KEYS:
            for part in self.ch.rows(f"SELECT DISTINCT partition_id,partition FROM system.parts WHERE active "
                                    f"AND database='gold' AND table='{name}'"):
                match = re.search(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}",part['partition'])
                if not match or match[0] not in old_versions: continue
                version = match[0]
                day = re.search(r"\d{4}-\d{2}-\d{2}",part['partition'])
                protected = version==dim_version if name in DIMENSIONS else day and (day[0],version) in active_pairs
                if not protected:
                    self.ch.execute(f"ALTER TABLE gold.{name} DROP PARTITION ID '{part['partition_id']}'")
                    removed += 1
        for row in self.ch.rows("SELECT name FROM system.databases WHERE startsWith(name,'warehouse_stage_')"):
            if re.fullmatch(r'warehouse_stage_[a-f0-9]{32}',row['name']):
                self.ch.execute(f"DROP DATABASE {row['name']} SYNC")
        for row in self.ch.rows("SELECT name FROM system.tables WHERE database='warehouse' AND startsWith(name,'pointer_')"):
            if re.fullmatch(r'pointer_[a-f0-9]{32}',row['name']):
                self.ch.execute(f"DROP TABLE warehouse.{row['name']} SYNC")
        return dict(removed_partitions=removed,retention_hours=hours)

    def status(self):
        self.initialize_control()
        rows=self.ch.rows("SELECT at,status,details FROM warehouse.refresh_runs ORDER BY at DESC LIMIT 1")
        consumers=self.ch.rows("SELECT table,`assignments.topic` AS topics,`assignments.current_offset` AS offsets,"
                               "last_poll_time,`exceptions.text` AS errors FROM system.kafka_consumers")
        checkpoint=self.checkpoint()
        archived=self.capture()
        return dict(checkpoint=checkpoint, archived_offsets=archived,
                    pending_events={k:max(0,v-(checkpoint or {}).get(k,-1)) for k,v in archived.items()},
                    latest_batch=rows,cdc_consumers=consumers,
                    pending_orders=self.ch.rows('SELECT order_id,first_seen FROM warehouse.pending_orders FINAL WHERE active=1 LIMIT 100'),
                    missing_consumers=sorted(({'kafka_raw_events'}|{'kafka_'+source for source in SOURCES})-{r['table'] for r in consumers}))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--migrate',action='store_true',help='Archive legacy derived layers during maintenance')
    parser.add_argument('--full',action='store_true',help='Explicit full rebuild (normal runs are incremental)')
    parser.add_argument('--status',action='store_true')
    parser.add_argument('--cleanup-hours',type=int)
    args=parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    with refresh_lock():
        job=Refresher()
        result=(job.status() if args.status else job.cleanup(args.cleanup_hours) if args.cleanup_hours is not None
                else job.refresh(migrate=args.migrate,full=args.full))
        print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__': main()
