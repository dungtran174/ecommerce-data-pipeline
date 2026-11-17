"""Finite synthetic PostgreSQL workload; all generated orders use real CDC."""
import argparse
from datetime import date, datetime, timedelta, timezone
import json
import math
import time
import uuid

from generator.database import get_connection


def bootstrap(conn):
    with conn, conn.cursor() as cur:
        def one(sql, args=()):
            cur.execute(sql,args)
            return cur.fetchone()[0]
        user=one("INSERT INTO users(username,password,email) VALUES ('load_demo','synthetic','load_demo@example.invalid') "
                 "ON CONFLICT(username) DO UPDATE SET username=excluded.username RETURNING id")
        cur.execute("SELECT id FROM regions WHERE region_name='Demo region' ORDER BY id LIMIT 1")
        row=cur.fetchone()
        region=row[0] if row else one("INSERT INTO regions(region_name) VALUES ('Demo region') RETURNING id")
        city=one("INSERT INTO provinces(province_name,region_id) VALUES ('Demo province',%s) "
                 "ON CONFLICT(province_name) DO UPDATE SET region_id=excluded.region_id RETURNING id",(region,))
        cur.execute("SELECT id FROM addresses WHERE user_id=%s ORDER BY id LIMIT 1",(user,))
        row=cur.fetchone()
        address=row[0] if row else one("INSERT INTO addresses(user_id,province_id,region_id,title) VALUES (%s,%s,%s,'Synthetic') RETURNING id",(user,city,region))
        brand=one("INSERT INTO brands(brand_name) VALUES ('Demo brand') ON CONFLICT(brand_name) DO UPDATE SET brand_name=excluded.brand_name RETURNING id")
        cat=one("INSERT INTO categories(category_name,slug) VALUES ('Demo category','load-demo') ON CONFLICT(slug) DO UPDATE SET category_name=excluded.category_name RETURNING id")
        products=[]
        for name,price,cost in [('Demo product A',100,60),('Demo product B',50,30)]:
            cur.execute('SELECT id FROM products WHERE product_name=%s ORDER BY id LIMIT 1',(name,))
            row=cur.fetchone()
            products.append(row[0] if row else one('INSERT INTO products(product_name,category_id,brand_id,product_price,unit_cost) VALUES (%s,%s,%s,%s,%s) RETURNING id',(name,cat,brand,price,cost)))
        method=one("SELECT id FROM payment_methods WHERE payment_method_name='Credit Card'")
        shipping=one("SELECT id FROM shipping_methods ORDER BY id LIMIT 1")
        payment=one("SELECT id FROM payment_status WHERE payment_status_name='completed'")
        shipstatus=one("SELECT id FROM shipping_status WHERE shipping_status_name='delivered'")
        delivered=one("SELECT id FROM order_status WHERE order_status_name='delivered'")
        pending=one("SELECT id FROM order_status WHERE order_status_name='pending'")
        cur.execute("SELECT id FROM ads_campaigns WHERE campaign_title='Demo campaign' ORDER BY id LIMIT 1")
        row=cur.fetchone()
        campaign=row[0] if row else one("INSERT INTO ads_campaigns(campaign_title) VALUES ('Demo campaign') RETURNING id")
        cur.execute("SELECT id FROM discounts WHERE code='LOAD-DEMO' ORDER BY id LIMIT 1")
        row=cur.fetchone()
        discount=row[0] if row else one("INSERT INTO discounts(adscampaign_id,type,value,code) VALUES (%s,'percent',10,'LOAD-DEMO') RETURNING id",(campaign,))
        cur.execute('CREATE TEMP TABLE IF NOT EXISTS load_order_ids(id integer,created_at timestamp) ON COMMIT DELETE ROWS')
    return dict(user=user,address=address,products=products,method=method,shipping=shipping,payment=payment,
                shipstatus=shipstatus,delivered=delivered,pending=pending,discount=discount,brand=brand)


def catalog(conn, ref, customers=1000, products=200):
    """Give load tests independent customers and product keys, not one hot row."""
    with conn,conn.cursor() as cur:
        cur.execute("INSERT INTO users(username,password,email) SELECT 'load_user_'||g,'synthetic','load_user_'||g||'@example.invalid' "
                    "FROM generate_series(1,%s) g ON CONFLICT(username) DO NOTHING",(customers,))
        cur.execute("INSERT INTO addresses(user_id,province_id,region_id,title) "
                    "SELECT u.id,a.province_id,a.region_id,'Synthetic' FROM users u CROSS JOIN "
                    "(SELECT province_id,region_id FROM addresses WHERE id=%s) a "
                    "WHERE u.username LIKE 'load_user_%%' AND NOT EXISTS (SELECT 1 FROM addresses x WHERE x.user_id=u.id)",(ref['address'],))
        pairs=max(1,products//2)
        for kind,price,cost in [('A',100,60),('B',50,30)]:
            cur.execute("INSERT INTO products(product_name,category_id,brand_id,product_price,unit_cost) "
                        "SELECT 'Load SKU '||%s||' '||g,p.category_id,p.brand_id,%s,%s "
                        "FROM generate_series(1,%s) g CROSS JOIN (SELECT category_id,brand_id FROM products WHERE id=%s) p "
                        "WHERE NOT EXISTS (SELECT 1 FROM products x WHERE x.product_name='Load SKU '||%s||' '||g)",
                        (kind,price,cost,pairs,ref['products'][0],kind))
        cur.execute("SELECT u.id,min(a.id) FROM users u JOIN addresses a ON a.user_id=u.id "
                    "WHERE u.username LIKE 'load_user_%%' GROUP BY u.id ORDER BY u.id LIMIT %s",(customers,))
        rows=cur.fetchall();ref['users']=[r[0] for r in rows];ref['addresses']=[r[1] for r in rows]
        for kind in ['A','B']:
            cur.execute("SELECT id FROM products WHERE product_name LIKE %s ORDER BY product_name LIMIT %s",('Load SKU '+kind+' %',pairs))
            ref['products_'+kind]=[r[0] for r in cur.fetchall()]
    return ref


def insert_orders(conn, ref, count, *, run_id='demo', history=None):
    started=time.monotonic()
    with conn,conn.cursor() as cur:
        expression="timezone('UTC',clock_timestamp())"
        if history:
            # Use a global ordinal so dates stay stable across batch sizes.
            expression = "%s::timestamp + ((g + %s - 1)::bigint * %s::bigint / %s::bigint) * interval '1 second'"
        users=ref.get('users',[ref['user']]);addresses=ref.get('addresses',[ref['address']])
        products_a=ref.get('products_A',[ref['products'][0]]);products_b=ref.get('products_B',[ref['products'][1]])
        args=[count,users,len(users),addresses,len(addresses),ref['discount'],ref['method'],ref['payment'],ref['shipping'],ref['shipstatus'],ref['delivered'],ref['pending']]
        if history:args.extend([history['start_date'], history['written'], history['days']*86400, history['target']])
        cur.execute(f"""WITH generated AS (
            SELECT nextval(pg_get_serial_sequence('orders','id'))::integer AS new_id,g FROM generate_series(1,%s) g
        ), inserted AS (
            INSERT INTO orders(id,user_id,address_id,order_amount,discount_amount,total_amount,discount_id,
                payment_method_id,payment_status_id,shipping_method_id,shipping_status_id,order_status_id,created_at,updated_at)
            SELECT new_id,(%s::int[])[1+new_id %% %s],(%s::int[])[1+new_id %% %s],220,22,198,%s,%s,%s,%s,%s,
                CASE WHEN new_id %% 10<8 THEN %s ELSE %s END,
                {expression},timezone('UTC',now()) FROM generated RETURNING id,created_at)
            INSERT INTO load_order_ids SELECT * FROM inserted""",args)
        cur.execute("""INSERT INTO orderdetails(order_id,product_id,quantity,product_price,product_tax,subtotal_amount,created_at)
            SELECT id,(%s::int[])[1+id %% %s],1,100,10,110,created_at FROM load_order_ids
            UNION ALL SELECT id,(%s::int[])[1+id %% %s],2,50,5,110,created_at FROM load_order_ids""",
                    (products_a,len(products_a),products_b,len(products_b)))
        cur.execute("INSERT INTO transactions(order_id,transaction_type,status,description,created_at) "
                    "SELECT id,'payment',TRUE,%s,created_at FROM load_order_ids",('Synthetic '+run_id,))
        cur.execute("INSERT INTO order_status_history(order_id,order_status_id,comments,created_at) "
                    "SELECT o.id,o.order_status_id,%s,o.created_at FROM orders o JOIN load_order_ids l ON l.id=o.id",('Synthetic '+run_id,))
        cur.execute('SELECT min(id),max(id) FROM load_order_ids')
        first,last=cur.fetchone()
        if history:
            # Commit progress with the data so resume cannot duplicate a batch.
            cur.execute("UPDATE synthetic_workload.runs SET written=written+%s, first_id=coalesce(first_id,%s), "
                        "last_id=%s, updated_at=now() WHERE run_id=%s", (count,first,last,run_id))
    return dict(rows=count,first_id=first,last_id=last,transaction_seconds=time.monotonic()-started)


def history_run(conn, ref, args):
    """Durable source manifest. Only explicit --resume may reuse a run ID."""
    with conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS synthetic_workload")
        cur.execute("""CREATE TABLE IF NOT EXISTS synthetic_workload.runs (
            run_id text PRIMARY KEY, target bigint NOT NULL, written bigint NOT NULL DEFAULT 0,
            days integer NOT NULL, start_date date NOT NULL, customers integer NOT NULL,
            products integer NOT NULL, first_id integer, last_id integer,
            started_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
            completed_at timestamptz)""")
        cur.execute('SELECT target,written,days,start_date,customers,products FROM synthetic_workload.runs WHERE run_id=%s FOR UPDATE',(args.run_id,))
        row=cur.fetchone()
        anchor=args.start_date or (datetime.now(timezone.utc).date()-timedelta(days=args.days))
        if row:
            if not args.resume:raise RuntimeError('Run ID exists; use --resume with the same settings')
            if (row[0],row[2],row[4],row[5]) != (args.history,args.days,args.customers,args.products) or (args.start_date and row[3]!=args.start_date):
                raise RuntimeError('Resume settings differ from the recorded manifest')
            written=row[1];anchor=row[3]
        else:
            if args.resume:raise RuntimeError('Cannot resume an unknown run ID')
            cur.execute('INSERT INTO synthetic_workload.runs(run_id,target,days,start_date,customers,products) VALUES (%s,%s,%s,%s,%s,%s)',
                        (args.run_id,args.history,args.days,anchor,args.customers,args.products))
            # Date synthetic customers and products before their orders.
            for table, keys in [('users', ref.get('users', [ref['user']])),
                                ('addresses', ref.get('addresses', [ref['address']])),
                                ('products', ref.get('products_A', []) + ref.get('products_B', []))]:
                if keys:
                    cur.execute(f"UPDATE {table} SET created_at=least(created_at,%s::date-interval '1 day') WHERE id=ANY(%s)",
                                (anchor,keys))
            written=0
    started=time.monotonic();initial=written;last_print=started
    while written<args.history:
        batch=insert_orders(conn,ref,min(args.batch_size,args.history-written),run_id=args.run_id,
                            history=dict(start_date=anchor,days=args.days,written=written,target=args.history))
        written+=batch['rows']
        if time.monotonic()-last_print>=5 or written==args.history:
            elapsed=time.monotonic()-started
            print(json.dumps(dict(phase='history',status='running',run_id=args.run_id,orders=written,
                                  target=args.history,days=args.days,start_date=str(anchor),elapsed_seconds=elapsed,
                                  inserted_orders_per_second=(written-initial)/max(elapsed,.001))),flush=True)
            last_print=time.monotonic()
    with conn,conn.cursor() as cur:
        cur.execute('UPDATE synthetic_workload.runs SET completed_at=coalesce(completed_at,now()) WHERE run_id=%s',(args.run_id,))
        cur.execute('SELECT row_to_json(r) FROM synthetic_workload.runs r WHERE run_id=%s',(args.run_id,))
        result=cur.fetchone()[0]
    print(json.dumps(dict(phase='history',status='complete',manifest=result),default=str),flush=True)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--history',type=int,default=0)
    p.add_argument('--days',type=int,default=30)
    p.add_argument('--start-date',type=date.fromisoformat,help='First historical UTC date; default: today minus --days')
    p.add_argument('--resume',action='store_true',help='Continue history from its transactionally committed manifest')
    p.add_argument('--batch-size',type=int,default=1000)
    p.add_argument('--profile',default='')
    p.add_argument('--customers',type=int,default=1000)
    p.add_argument('--products',type=int,default=200)
    p.add_argument('--run-id',default='load-'+uuid.uuid4().hex[:10])
    args=p.parse_args()
    if args.history<0 or args.days<1 or args.batch_size<1 or args.customers<1 or args.products<2 or args.products%2:
        p.error('History must be nonnegative; days/batch/customers positive; products even and >=2')
    if args.resume and (not args.history or args.profile):p.error('--resume is for history only; run live profiles separately')
    phases=[]
    for item in args.profile.split(','):
        if not item:continue
        try:name,rate,seconds=item.split(':');rate=float(rate);seconds=float(seconds)
        except ValueError:p.error('Profile format: name:orders_per_minute:seconds')
        if not math.isfinite(rate) or not math.isfinite(seconds) or rate<=0 or seconds<=0:p.error('Rates and durations must be finite and positive')
        phases.append((name,rate,seconds))
    conn=get_connection();all_results=[]
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT pg_try_advisory_lock(741209621)')
            if not cur.fetchone()[0]: raise RuntimeError('Another synthetic workload is running')
        conn.commit()
        ref=catalog(conn,bootstrap(conn),args.customers,args.products)
        if args.history:history_run(conn,ref,args)
        for name,rate,seconds in phases:
            start=time.monotonic();written=0;total=math.floor(rate*seconds/60);durations=[];first=None;last=None
            while written<total:
                due=min(total,math.floor((time.monotonic()-start)*rate/60))
                count=min(args.batch_size,due-written)
                if count<=0:time.sleep(0.1);continue
                result=insert_orders(conn,ref,count,run_id=args.run_id)
                written+=count;first=first or result['first_id'];last=result['last_id'];durations.append(result['transaction_seconds'])
            elapsed=time.monotonic()-start
            durations.sort()
            record=dict(phase=name,orders=written,target_orders_per_minute=rate,actual_orders_per_minute=written*60/max(elapsed,.001),
                        elapsed_seconds=elapsed,transaction_p95_seconds=durations[min(len(durations)-1,int(.95*len(durations)))] if durations else 0,
                        first_id=first,last_id=last,completed_at=datetime.now(timezone.utc).isoformat())
            all_results.append(record);print(json.dumps(record),flush=True)
        print(json.dumps(dict(run_id=args.run_id,status='complete',history_orders=args.history,phases=all_results)),flush=True)
    finally: conn.close()


if __name__=='__main__':main()
