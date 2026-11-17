import random

from generator.database import execute_values_insert, fetch_all
from generator.faker_data import FakeDataGenerator


def generate_regions(**kwargs):
    """Insert 8 Vietnam regions."""
    gen = FakeDataGenerator()
    regions = gen.generate_regions()

    execute_values_insert(
        "INSERT INTO regions (region_name) "
        "SELECT v.region_name FROM (VALUES %s) AS v(region_name) "
        "WHERE NOT EXISTS (SELECT 1 FROM regions r WHERE r.region_name=v.region_name)",
        regions,
    )
    return f"Inserted {len(regions)} regions"


def generate_provinces(**kwargs):
    """Insert 63 provinces, linked to their region IDs."""
    rows = fetch_all("SELECT id, region_name FROM regions")
    region_id_map = {row["region_name"]: row["id"] for row in rows}

    gen = FakeDataGenerator()
    provinces = gen.generate_provinces(region_id_map)

    execute_values_insert(
        "INSERT INTO provinces (province_name, region_id, latitude, longitude) "
        "VALUES %s ON CONFLICT (province_name) DO NOTHING",
        provinces,
    )
    return f"Inserted {len(provinces)} provinces"


def generate_brands(**kwargs):
    """Generate 3-5 new brands per week."""
    gen = FakeDataGenerator()
    count = 5
    brands = gen.generate_brands(count=count)

    execute_values_insert(
        "INSERT INTO brands (brand_name) VALUES %s ON CONFLICT (brand_name) DO NOTHING",
        brands,
    )
    return f"Generated {count} brands"


def generate_parent_categories(**kwargs):
    """Insert 10 parent categories (top-level)."""
    gen = FakeDataGenerator()
    categories = gen.generate_parent_categories()

    execute_values_insert(
        "INSERT INTO categories (category_name, category_id, slug) "
        "VALUES %s ON CONFLICT (slug) DO NOTHING",
        categories,
    )
    return f"Inserted {len(categories)} parent categories"


def generate_sub_categories(**kwargs):
    """Insert sub-categories linked to their parent category IDs."""
    rows = fetch_all(
        "SELECT id, category_name FROM categories WHERE category_id IS NULL"
    )
    parent_id_map = {row["category_name"]: row["id"] for row in rows}

    gen = FakeDataGenerator()
    subs = gen.generate_sub_categories(parent_id_map)

    execute_values_insert(
        "INSERT INTO categories (category_name, category_id, slug) "
        "VALUES %s ON CONFLICT (slug) DO NOTHING",
        subs,
    )
    return f"Inserted {len(subs)} sub-categories"


def generate_tags(**kwargs):
    """Insert predefined product tags."""
    gen = FakeDataGenerator()
    tags = gen.generate_tags()

    execute_values_insert(
        "INSERT INTO tags (tag_name) VALUES %s ON CONFLICT (tag_name) DO NOTHING",
        tags,
    )
    return f"Inserted {len(tags)} tags"


def generate_campaigns(**kwargs):
    """Create 3 new marketing campaigns."""
    gen = FakeDataGenerator()
    campaigns = gen.generate_campaigns(count=3)

    execute_values_insert(
        "INSERT INTO ads_campaigns (campaign_title, started_at, expired_at) VALUES %s",
        campaigns,
    )
    return f"Created {len(campaigns)} campaigns"


def generate_discounts(**kwargs):
    """Create discount codes linked to the latest campaigns."""
    rows = fetch_all(
        "SELECT id FROM ads_campaigns ORDER BY id DESC LIMIT 3"
    )
    campaign_ids = [row["id"] for row in rows]

    if not campaign_ids:
        return "No campaigns found, skipping discounts"

    gen = FakeDataGenerator()
    discounts = gen.generate_discounts(campaign_ids, count_per_campaign=2)

    execute_values_insert(
        "INSERT INTO discounts "
        "(adscampaign_id, type, value, code, started_at, expired_at) VALUES %s",
        discounts,
    )
    return f"Created {len(discounts)} discount codes"


def generate_products(**kwargs):
    """Generate 10 new products linked to existing categories and brands."""
    categories = fetch_all(
        "SELECT id FROM categories WHERE category_id IS NOT NULL"
    )
    category_ids = [row["id"] for row in categories]

    brands = fetch_all("SELECT id FROM brands")
    brand_ids = [row["id"] for row in brands]

    if not category_ids or not brand_ids:
        return "No categories or brands found, skipping product generation"

    gen = FakeDataGenerator()
    products = gen.generate_products(category_ids, brand_ids, count=10)

    execute_values_insert(
        "INSERT INTO products "
        "(product_name, category_id, brand_id, product_price, unit_cost, product_quantity) "
        "VALUES %s",
        products,
    )
    return f"Generated {len(products)} products"


def assign_product_tags(**kwargs):
    """Assign 1-3 random tags to each newly created product."""
    products_without_tags = fetch_all(
        "SELECT p.id FROM products p "
        "LEFT JOIN product_tag pt ON p.id = pt.product_id "
        "WHERE pt.id IS NULL"
    )
    if not products_without_tags:
        return "All products already have tags"

    tags = fetch_all("SELECT id FROM tags")
    tag_ids = [row["id"] for row in tags]

    if not tag_ids:
        return "No tags found, skipping"

    product_tags = []
    for product in products_without_tags:
        num_tags = random.randint(1, min(3, len(tag_ids)))
        selected_tags = random.sample(tag_ids, num_tags)
        for tag_id in selected_tags:
            product_tags.append((product["id"], tag_id))

    execute_values_insert(
        "INSERT INTO product_tag (product_id, tag_id) VALUES %s",
        product_tags,
    )
    return f"Assigned tags to {len(products_without_tags)} products"
