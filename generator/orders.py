import logging
import os
import random

from generator.database import create_complete_order, fetch_all, get_connection
from generator.faker_data import FakeDataGenerator

logger = logging.getLogger(__name__)


def advance_order_lifecycle(limit=300):
    """Advance a bounded set with set-based SQL in one transaction."""
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                WITH selected AS (
                    SELECT o.id, os.order_status_name, pm.payment_method_name
                    FROM orders o JOIN order_status os ON o.order_status_id=os.id
                    JOIN payment_methods pm ON o.payment_method_id=pm.id
                    WHERE os.order_status_name IN ('pending','confirmed','shipping')
                    ORDER BY o.id LIMIT %s FOR UPDATE OF o SKIP LOCKED
                ), mapped AS (
                    SELECT *, CASE order_status_name WHEN 'pending' THEN 'confirmed'
                        WHEN 'confirmed' THEN 'shipping' ELSE 'delivered' END AS next_status,
                        CASE order_status_name WHEN 'pending' THEN 'pending'
                        WHEN 'confirmed' THEN 'picked_up' ELSE 'delivered' END AS next_shipping
                    FROM selected
                ), updated AS (
                    UPDATE orders o SET order_status_id=os.id, shipping_status_id=ss.id,
                        payment_status_id=CASE WHEN m.payment_method_name='COD' AND m.next_status='delivered'
                            THEN (SELECT id FROM payment_status WHERE payment_status_name='completed')
                            ELSE o.payment_status_id END,
                        shipped_at=CASE WHEN m.next_status='shipping' THEN now() ELSE o.shipped_at END,
                        updated_at=now()
                    FROM mapped m JOIN order_status os ON os.order_status_name=m.next_status
                    JOIN shipping_status ss ON ss.shipping_status_name=m.next_shipping
                    WHERE o.id=m.id RETURNING o.id,o.order_status_id,m.next_status,m.payment_method_name
                ), payments AS (
                    UPDATE transactions t SET status=TRUE,description='COD payment received'
                    FROM updated u WHERE t.order_id=u.id AND t.transaction_type='payment'
                    AND u.next_status='delivered' AND u.payment_method_name='COD'
                ), history AS (
                    INSERT INTO order_status_history(order_id,order_status_id,comments)
                    SELECT id,order_status_id,'Order moved to '||next_status FROM updated
                ) SELECT count(*) FROM updated
            """, (limit,))
            return cur.fetchone()[0]
    finally:
        conn.close()


def generate_bulk_orders(count=100, **kwargs):
    """Generate multiple complete orders with details, transactions, and status history."""
    gen = FakeDataGenerator()
    seed = os.getenv("ECOMMERCE_RANDOM_SEED")
    if seed is not None:
        random.seed(int(seed))
    advanced = advance_order_lifecycle(limit=max(300, count * 3))
    logger.info("Advanced %s existing orders by one status", advanced)

    order_by = "u.id, a.id" if seed is not None else "RANDOM()"
    users_with_addresses = fetch_all(
        "SELECT u.id AS user_id, a.id AS address_id "
        "FROM users u "
        "INNER JOIN addresses a ON u.id = a.user_id "
        f"ORDER BY {order_by} LIMIT 50"
    )
    if not users_with_addresses:
        logger.warning("No users with addresses found. Run user_registration DAG first.")
        return "Skipped: no users with addresses"

    products = fetch_all(
        "SELECT id, product_price FROM products WHERE product_price > 0 ORDER BY id"
    )
    if not products:
        logger.warning("No products found. Run product DAG first.")
        return "Skipped: no products"

    payment_methods = fetch_all("SELECT id, payment_method_name FROM payment_methods")
    shipping_methods = fetch_all("SELECT id FROM shipping_methods")
    order_statuses = fetch_all("SELECT id, order_status_name FROM order_status")
    payment_statuses = fetch_all("SELECT id, payment_status_name FROM payment_status")
    shipping_statuses = fetch_all("SELECT id, shipping_status_name FROM shipping_status")

    discounts = fetch_all("SELECT id, type, value FROM discounts")

    pm_ids = [r["id"] for r in payment_methods]
    sm_ids = [r["id"] for r in shipping_methods]
    pending_order_id = next(r["id"] for r in order_statuses if r["order_status_name"] == "pending")
    pending_shipping_id = next(r["id"] for r in shipping_statuses if r["shipping_status_name"] == "pending")
    completed_payment_id = next(r["id"] for r in payment_statuses if r["payment_status_name"] == "completed")
    pending_payment_id = next(r["id"] for r in payment_statuses if r["payment_status_name"] == "pending")
    method_names = {r["id"]: r["payment_method_name"] for r in payment_methods}

    orders_created = 0

    connection = get_connection()
    try:
        for _ in range(count):
            user_addr = random.choice(users_with_addresses)
            user_id = user_addr["user_id"]
            address_id = user_addr["address_id"]

            discount = None
            if discounts and random.random() > 0.5:
                discount = random.choice(discounts)

            order_data, order_details = gen.generate_order(
                user_id=user_id,
                address_id=address_id,
                product_list=products,
                payment_method_ids=pm_ids,
                shipping_method_ids=sm_ids,
                order_status_ids=[pending_order_id],
                payment_status_ids=[completed_payment_id],
                shipping_status_ids=[pending_shipping_id],
                discount=discount,
            )

            if method_names[order_data["payment_method_id"]] == "COD":
                order_data["payment_status_id"] = pending_payment_id
                order_data["payment_completed"] = False
            else:
                order_data["payment_completed"] = True

            create_complete_order(order_data, order_details, connection=connection)

            orders_created += 1
    finally:
        connection.close()

    return f"Created {orders_created} orders with details, transactions, and status history"
