"""Database helper module for connecting to PostgreSQL OLTP."""

import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

logger = logging.getLogger(__name__)


def get_connection():
    """Create a connection to the main PostgreSQL (OLTP) database."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres-main"),
        port=int(os.getenv("POSTGRES_INTERNAL_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def execute_query(query, params=None, fetch=False):
    """Execute a single SQL query."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            if fetch:
                result = cur.fetchall()
                conn.commit()
                return result
            conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Query failed: {e}")
        raise
    finally:
        conn.close()


def execute_values_insert(query, data_list, template=None):
    """Execute a batch INSERT using psycopg2.extras.execute_values."""
    if not data_list:
        logger.warning("Empty data_list, skipping insert.")
        return

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, query, data_list, template=template)
            conn.commit()
            logger.info(f"Bulk inserted {len(data_list)} rows successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Bulk insert failed: {e}")
        raise
    finally:
        conn.close()


def fetch_all(query, params=None):
    """Fetch all rows as a list of dicts."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()
    finally:
        conn.close()


def create_complete_order(order_data, order_details, connection=None):
    """Commit an order, its lines, payment record and history atomically."""
    if not order_details:
        raise ValueError("An order must contain at least one line item")

    conn = connection or get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO orders (
                        user_id, staff_id, address_id,
                        order_amount, discount_amount, total_amount,
                        discount_id, payment_method_id, payment_status_id,
                        order_status_id, shipping_method_id, shipping_status_id
                    ) VALUES (
                        %(user_id)s, %(staff_id)s, %(address_id)s,
                        %(order_amount)s, %(discount_amount)s, %(total_amount)s,
                        %(discount_id)s, %(payment_method_id)s, %(payment_status_id)s,
                        %(order_status_id)s, %(shipping_method_id)s, %(shipping_status_id)s
                    ) RETURNING id""",
                    order_data,
                )
                order_id = cur.fetchone()[0]
                detail_rows = [
                    (order_id, d["product_id"], d["quantity"], d["product_price"],
                     d["product_tax"], d["subtotal_amount"])
                    for d in order_details
                ]
                execute_values(
                    cur,
                    """INSERT INTO orderdetails
                       (order_id, product_id, quantity, product_price,
                        product_tax, subtotal_amount) VALUES %s""",
                    detail_rows,
                )
                cur.execute(
                    """INSERT INTO transactions
                       (order_id, transaction_type, status, description)
                       VALUES (%s, 'payment', %s, %s)""",
                    (order_id, order_data.get("payment_completed", True),
                     f"Payment for order #{order_id}" if order_data.get("payment_completed", True)
                     else f"Payment pending for order #{order_id}"),
                )
                cur.execute(
                    """INSERT INTO order_status_history
                       (order_id, order_status_id, comments)
                       VALUES (%s, %s, %s)""",
                    (order_id, order_data["order_status_id"],
                     f"Order #{order_id} created"),
                )
        return order_id
    finally:
        if connection is None:
            conn.close()
