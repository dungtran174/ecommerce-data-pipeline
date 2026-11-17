"""Compare the same sales report on OLTP and precomputed ClickHouse Gold."""
import argparse
import json
import statistics
import time
from decimal import Decimal
from generator.database import get_connection
from warehouse.refresh import ClickHouse

SOURCE_SQL="""WITH base AS (
 SELECT o.id AS order_id,i.id AS item_id,
   ((o.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS day,
   coalesce(a.province_id,0) AS province_id,i.product_id,i.quantity,
   i.quantity*i.product_price AS gmv,i.subtotal_amount,o.discount_amount,
   CASE WHEN o.order_amount>0 THEN round(o.discount_amount*i.subtotal_amount/o.order_amount,2) ELSE 0 END AS preliminary
 FROM orders o JOIN order_status s ON o.order_status_id=s.id
 JOIN orderdetails i ON i.order_id=o.id LEFT JOIN addresses a ON o.address_id=a.id
 WHERE s.order_status_name='delivered'
), allocated AS (
 SELECT *,preliminary+CASE WHEN row_number() OVER (PARTITION BY order_id ORDER BY item_id DESC)=1
 THEN discount_amount-sum(preliminary) OVER (PARTITION BY order_id) ELSE 0 END AS discount_gross FROM base
)
SELECT day,province_id,product_id,sum(gmv),sum(gmv-round(discount_gross*gmv/nullif(subtotal_amount,0),2)),
       sum(quantity),count(DISTINCT order_id)
FROM allocated GROUP BY day,province_id,product_id ORDER BY day,province_id,product_id"""
REPORT_SQL="SELECT date,province_id,product_id,total_gmv,total_net_revenue,total_quantity_sold,product_order_count FROM report.view_overview_sales ORDER BY date,province_id,product_id"


def verify_reports(expected, actual):
    if not expected:
        raise RuntimeError('No delivered orders to benchmark; generate data and wait for ETL first')
    if len(expected) != len(actual):
        raise RuntimeError('Report group counts differ; pause source generators and wait for ETL')
    for source, report in zip(expected, actual):
        key = (str(source[0]), source[1], source[2])
        report_key = (report['date'], report['province_id'], report['product_id'])
        matches = (
            key == report_key
            and abs(source[3] - Decimal(str(report['total_gmv']))) < Decimal('.01')
            and abs(source[4] - Decimal(str(report['total_net_revenue']))) < Decimal('.01')
            and source[5] == int(report['total_quantity_sold'])
            and source[6] == int(report['product_order_count'])
        )
        if not matches:
            raise RuntimeError(f'Report reconciliation failed for date/province/product {key}')


def run(runs=7):
    if runs < 3:
        raise ValueError('At least three runs are required (two warmups)')
    pg=get_connection();ch=ClickHouse()
    try:
        pg_times=[];ch_times=[]
        for _ in range(runs):
         t=time.perf_counter()
         with pg,pg.cursor() as c:c.execute(SOURCE_SQL);expected=c.fetchall()
         pg_times.append(time.perf_counter()-t)
         t=time.perf_counter();actual=ch.rows(REPORT_SQL);ch_times.append(time.perf_counter()-t)
        verify_reports(expected, actual)
        with pg,pg.cursor() as c:
         c.execute('SELECT count(*) FROM orders');orders=c.fetchone()[0]
         c.execute('SELECT count(*) FROM orderdetails');lines=c.fetchone()[0]
         c.execute("SELECT sum(n) FROM (SELECT count(*) n FROM users UNION ALL SELECT count(*) FROM addresses UNION ALL SELECT count(*) FROM products UNION ALL SELECT count(*) FROM orders UNION ALL SELECT count(*) FROM orderdetails UNION ALL SELECT count(*) FROM transactions UNION ALL SELECT count(*) FROM order_status_history) x");source_rows=int(c.fetchone()[0])
        compression=ch.rows("SELECT sum(data_uncompressed_bytes) AS uncompressed,sum(data_compressed_bytes) AS compressed,"
                            "sum(bytes_on_disk) AS on_disk FROM system.parts WHERE active AND database='bronze' AND table IN ('orders','orderdetails')")[0]
        result=dict(status='passed',groups=len(actual),source_orders=orders,source_lines=lines,source_rows_seven_tables=source_rows,
         postgres_seconds=pg_times,clickhouse_seconds=ch_times,
         postgres_median_seconds=statistics.median(pg_times[2:]),clickhouse_median_seconds=statistics.median(ch_times[2:]),
         compression=compression,source_sql=SOURCE_SQL,clickhouse_sql=REPORT_SQL,
         note='Same date/province/product metrics, including discounts and distinct order counts. PostgreSQL computes from source; ClickHouse reads precomputed Gold. Includes network/fetch time. First two runs excluded from medians.')
        return result
    finally:
        pg.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runs',type=int,default=7)
    args=parser.parse_args()
    if args.runs<3:parser.error('At least three runs are required (two warmups)')
    print(json.dumps(run(args.runs),ensure_ascii=False))


if __name__=='__main__':main()
