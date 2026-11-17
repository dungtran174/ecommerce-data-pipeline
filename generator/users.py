import random

from generator.database import execute_values_insert, fetch_all, execute_query
from generator.faker_data import FakeDataGenerator


def register_users(**kwargs):
    """Generate and insert new user accounts."""
    gen = FakeDataGenerator()
    users = gen.generate_users(count=5)

    execute_values_insert(
        "INSERT INTO users (username, password, email, mobile) "
        "VALUES %s ON CONFLICT DO NOTHING",
        users,
    )
    return f"Registered {len(users)} new users"


def assign_roles(**kwargs):
    """Assign 'user' role to all users who don't have a role yet."""
    role = execute_query(
        "SELECT id FROM roles WHERE role_name = 'user'",
        fetch=True,
    )
    if not role:
        return "Role 'user' not found"

    user_role_id = role[0]["id"]

    users_without_role = fetch_all(
        "SELECT u.id FROM users u "
        "LEFT JOIN role_user ru ON u.id = ru.user_id "
        "WHERE ru.id IS NULL"
    )

    if not users_without_role:
        return "All users already have roles"

    role_assignments = [(u["id"], user_role_id) for u in users_without_role]

    execute_values_insert(
        "INSERT INTO role_user (user_id, role_id) VALUES %s",
        role_assignments,
    )
    return f"Assigned roles to {len(role_assignments)} users"


def create_addresses(**kwargs):
    """Create a delivery address for users who don't have one."""
    gen = FakeDataGenerator()

    users_without_addr = fetch_all(
        "SELECT u.id FROM users u "
        "LEFT JOIN addresses a ON u.id = a.user_id "
        "WHERE a.id IS NULL"
    )

    if not users_without_addr:
        return "All users already have addresses"

    provinces = fetch_all("SELECT id, region_id FROM provinces")
    if not provinces:
        return "No provinces found, run province DAG first"

    addresses = []
    for user in users_without_addr:
        province = random.choice(provinces)
        addresses.append((
            "Home",
            user["id"],
            province["id"],
            province["region_id"],
            gen.fake.address(),
        ))

    execute_values_insert(
        "INSERT INTO addresses (title, user_id, province_id, region_id, full_address) "
        "VALUES %s",
        addresses,
    )
    return f"Created {len(addresses)} addresses"
