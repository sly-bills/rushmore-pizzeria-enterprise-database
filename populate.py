# Import necessary modules
import os
import random
import logging
import time
import argparse
from datetime import datetime, timedelta, timezone

import faker
import yaml
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from faker import Faker
from tqdm import tqdm  # Progress bar


# ---------- Logging Setup ----------
def setup_logger(name, log_file, level=logging.INFO):
    """Helper to set up individual loggers for modular logging."""
    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


# Create category loggers
db_logger = setup_logger("db", "db_connection.log")
mask_logger = setup_logger("mask", "data_masking.log")
populate_logger = setup_logger("populate", "data_population.log")

# Root logger to console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)


# ---------- Configuration loading ----------
def load_config(yaml_path="dbconfig.yaml"):
    """
    Load database configuration from a YAML file only.
    The YAML must define: host, port, user, password, dbname.
    Example:
        host: localhost
        port: 5432
        user: myuser
        password: mypassword
        dbname: rushmore_db
    """
    if not os.path.exists(yaml_path):
        db_logger.error(f"Configuration file '{yaml_path}' not found.")
        raise FileNotFoundError(f"Configuration file '{yaml_path}' not found.")

    with open(yaml_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    required_keys = ["host", "port", "user", "password", "dbname"]
    missing = [k for k in required_keys if k not in cfg or cfg[k] in (None, "")]
    if missing:
        db_logger.error(f"Missing required keys in {yaml_path}: {missing}")   
        raise ValueError(f"Missing required keys in {yaml_path}: {missing}")
    
    db_logger.info(f"Configuration loaded successfully from '{yaml_path}'.")
    return cfg


# ---------- Database connection ----------
def get_conn(cfg):
    """
    Establish a PostgreSQL connection using psycopg2 and the provided config dictionary.
    """
    try:
        conn = psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["user"],
            password=cfg["password"],
            dbname=cfg["dbname"]
        )
        db_logger.info(
            f"Database connection established successfully to '{cfg['dbname']}' at host '{cfg['host']}'."
        )
        return conn
    except psycopg2.Error as e:
        db_logger.error(f"Database connection failed: {e}")
        raise


# ---------- Masking ----------
def mask_email(email: str) -> str:
    """
    Simple masking: keep first char of local part and domain intact,
    replace rest of local part with asterisks.
    Example: peter.asamoah@example.com -> p*******@example.com
    """
    if not email or '@' not in email:
        mask_logger.warning(f"Invalid or missing email provided for masking: {email}")
        return email
    
    local, domain = email.split('@', 1)
    if len(local) <= 2:
        masked_local = local[0] + "*"*(len(local)-1)
    else:
        masked_local = local[0] + "*"*(len(local)-1)

    masked_email = f"{masked_local}@{domain}"
    mask_logger.info(f"Masked email: {email} -> {masked_email}")
    return masked_email

def mask_phone(phone: str) -> str:
    """
    Keep last 4 digits, replace other characters with * (simple).
    Non-digits preserved for readability but masked in digits.
    """
    if not phone:
        mask_logger.warning("Empty or No phone number provided for masking.")
        return phone
    
    digits = [c for c in phone if c.isdigit()]
    if not digits:
        mask_logger.warning(f"No digits found in phone number: {phone}")
        return phone
    
    if len(digits) <= 4:
        masked_digits = "*" * len(digits)
    else:
        last4 = ''.join(digits[-4:])
        masked_digits = "*" * (len(digits) - 4) + last4

    mask_logger.info(f"Masked phone: {phone} -> {masked_digits}")
    return masked_digits


# ---------- Data creation functions ----------

def create_stores(cur, faker, num_stores=5):
    populate_logger.info(f"Starting to create {num_stores} stores...")
    stores = []
    for _ in range(num_stores):
        address = faker.address().replace('\n', ', ')
        city = f"{faker.city()} RushMore Pizzeria"
        phone_number = faker.unique.msisdn()[:20]
        opened_at = faker.date_time_this_decade(tzinfo=timezone.utc)
        stores.append((address, city, phone_number, opened_at))

    insert = "INSERT INTO stores (address, city, phone_number, opened_at) VALUES %s RETURNING store_id"
    try:
        rows = execute_values(cur, insert, stores, fetch=True)
        store_ids = [row[0] for row in rows]
        populate_logger.info(f"Inserted {len(store_ids)} stores successfully.")
        return store_ids
    except Exception as e:
        populate_logger.exception(f"Error inserting stores: {e}")
        raise

def create_customers(cur, faker, num_customers=1000):
    populate_logger.info(f"Starting to create {num_customers} customers...")
    customers = []
    for _ in range(num_customers):
        first_name = faker.first_name()
        last_name = faker.last_name()
        email = raw_email = faker.unique.email()
        masked_email = mask_email(raw_email)
        # ensure masked uniqueness by adding a short suffix if collision detected
        if any(c[2] == masked_email for c in customers):
            masked_email = masked_email.replace("@", f"+{random.randint(1000,9999)}@")
        email = masked_email
        raw_phone = faker.unique.msisdn()[:20]
        masked_phone = mask_phone(raw_phone)
        # ensure masked uniqueness within this batch
        if any(c[3] == masked_phone for c in customers):
            masked_phone = masked_phone + str(random.randint(1000, 9999))
        phone_number = masked_phone
        created_at = faker.date_time_this_year(tzinfo=timezone.utc)
        customers.append((first_name, last_name, email, phone_number, created_at))

    insert = ("INSERT INTO customers (first_name, last_name, email, phone_number, created_at) "
              "VALUES %s RETURNING customer_id")

    ids = []
    batch_size = 1000
    try:
        for i in range(0, len(customers), batch_size):
            chunk = customers[i:i+batch_size]
            rows = execute_values(cur, insert, chunk, fetch=True)
            ids.extend([row[0] for row in rows])
            populate_logger.info(f"Inserted customers: {len(ids)}/{len(customers)}")
        populate_logger.info(f"Inserted {len(ids)} customers successfully.")
        return ids
    except Exception as e:
        populate_logger.exception(f"Error inserting customers: {e}")
        raise


def create_ingredients(cur, faker, num_ingredients=50):
    populate_logger.info(f"Starting to create {num_ingredients} ingredients...")
    basic = ['Tomato', 'Cheese', 'Pepperoni', 'Mushroom', 'Basil',
             'Chicken', 'Onion', 'Peppers', 'Olive Oil', 'Garlic',
             'Dough', 'Sausage', 'Spinach', 'Feta', 'Pineapple',
             'Ham', 'Bacon', 'Jalapeno', 'Corn', 'BBQ Sauce']
    
    names = set()
    while len(names) < num_ingredients:
        base = random.choice(basic)
        suffix = f" {faker.word()}" if base in names else ""
        names.add(base + suffix)

    ing = []
    for name in names:
        stock_quantity = random.randint(10, 500)
        unit = random.choice(['grams', 'ml', 'pieces'])
        ing.append((name, stock_quantity, unit))

    insert = "INSERT INTO ingredients (name, stock_quantity, unit) VALUES %s RETURNING ingredient_id"
    try:
        rows = execute_values(cur, insert, ing, fetch=True)
        ids = [row[0] for row in rows]
        populate_logger.info(f"Inserted {len(ids)} ingredients successfully.")
        return ids
    except Exception as e:
        populate_logger.exception(f"Error inserting ingredients: {e}")
        raise

def create_menu_items(cur, faker, num_items=30):
    populate_logger.info(f"Starting to create {num_items} menu items...")
    items = []
    categories = ['Classic', 'Vegetarian/Vegan', 'Gourmet/Special', 'Meat Lovers', 'Seafood', 'Deluxe']
    sizes = ['Small', 'Medium', 'Large', 'Family']

    for _ in range(num_items):
        name = f"{faker.word().capitalize()} {random.choice(['Margherita', 'Pepperoni Feast', 'Hawaiian', 'Four Cheese', 'Spinach & Feta', 'BBQ Chicken', 'Veggie Delight', 'Meat Supreme', 'Seafood Special'])}"
        category = random.choice(categories)
        size = random.choice(sizes)
        price = round(random.uniform(5.0, 25.0), 2)
        items.append((name, category, size, price))

    insert = "INSERT INTO menu_items (name, category, size, price) VALUES %s RETURNING item_id"
    try:
        rows = execute_values(cur, insert, items, fetch=True)
        ids = [row[0] for row in rows]
        populate_logger.info(f"Inserted {len(ids)} menu items successfully.")
        return ids
    except Exception as e:
        populate_logger.exception(f"Error inserting menu items: {e}")
        raise

def create_item_ingredients(cur, menu_item_ids, ingredient_ids, min_ings=2, max_ings=6):
    populate_logger.info("Starting to create item_ingredients...")
    rows = []
    # For each menu item assign some ingredients and a quantity_required
    for item_id in menu_item_ids:
        num_ings = random.randint(min_ings, max_ings)
        chosen = random.sample(ingredient_ids, min(num_ings, len(ingredient_ids)))
        for ing_id in chosen:
            # quantity_required: realistic small decimal (e.g., grams or ml) — scale depends on unit
            quantity_required = round(random.uniform(5.0, 300.0), 2)
            rows.append((item_id, ing_id, quantity_required))

    insert = ("INSERT INTO item_ingredients (item_id, ingredient_id, quantity_required) "
              "VALUES %s RETURNING item_id, ingredient_id")
    try:
        result = execute_values(cur, insert, rows, fetch=True)
        populate_logger.info(f"Inserted {len(result)} item_ingredient rows.")
        return result
    except Exception as e:
        populate_logger.exception(f"Error inserting item_ingredients: {e}")
        raise

def create_orders(cur, customer_ids, store_ids, num_orders=5000, guest_rate=0.10):
    """
    Create orders entries. total_amount is set to 0 initially — it will be computed/updated
    after order_items are created (see create_order_items).
    - customer_ids: list of customer_id
    - store_ids: list of store_id
    - guest_rate: fraction of orders with no customer (NULL)
    Returns: list of order_id created (and order timestamps if needed).
    """
    populate_logger.info(f"Starting to create {num_orders} orders...")
    orders = []
    now = datetime.now(timezone.utc)
    # We'll generate order timestamps uniformly over the past 365 days
    days_back = 365
    for _ in range(num_orders):
        # pick a customer or NULL (guest)
        customer_id = random.choice(customer_ids) if customer_ids and random.random() > guest_rate else None
        store_id = random.choice(store_ids)
        # random timestamp in last year
        offset_days = random.randint(0, days_back)
        offset_seconds = random.randint(0, 86400 - 1)
        order_timestamp = now - timedelta(days=offset_days, seconds=offset_seconds)
        # placeholder total_amount = 0.0; will be updated after order_items inserted
        total_amount = 0.00
        status = random.choice(['Pending', 'Preparing', 'Completed', 'Cancelled'])
        orders.append((customer_id, store_id, order_timestamp, total_amount, status))

    insert = ("INSERT INTO orders (customer_id, store_id, order_timestamp, total_amount, status) "
              "VALUES %s RETURNING order_id, order_timestamp")
    try:
        # batch insert (chunks to avoid blowing memory)
        batch = 1000
        created_orders = []
        for i in range(0, len(orders), batch):
            chunk = orders[i:i+batch]
            rows = execute_values(cur, insert, chunk, fetch=True)
            created_orders.extend(rows)  # list of tuples (order_id, order_timestamp)
            populate_logger.info(f"Inserted orders: {len(created_orders)}/{len(orders)}")
        populate_logger.info(f"Inserted {len(created_orders)} orders successfully.")
        # return list of order_id (and timestamps if needed)
        return created_orders
    except Exception as e:
        populate_logger.exception(f"Error inserting orders: {e}")
        raise    

def create_order_items(cur, avg_items_per_order=3):
    """
    Creates order_items for all orders in the DB (or you can filter by recent orders).
    This function:
      1. Loads all existing orders (order_id)
      2. Loads menu_items (item_id and price)
      3. For each order, picks a Poisson-like number of items around avg_items_per_order
         (at least 1), creates order_items rows (quantity 1-3), and calculates per-order totals.
      4. Inserts all order_items in batches and then updates orders.total_amount with the computed totals.
    Returns: summary dict with counts and total revenue computed.
    """
    populate_logger.info("Starting to create order_items for existing orders...")

    # 1) Fetch orders
    cur.execute("SELECT order_id FROM orders")
    order_rows = cur.fetchall()
    if not order_rows:
        populate_logger.warning("No orders found in DB. Nothing to do.")
        return {"orders": 0, "order_items": 0, "revenue": 0.0}
    order_ids = [r[0] for r in order_rows]

    # 2) Fetch menu item prices
    cur.execute("SELECT item_id, price FROM menu_items")
    menu_rows = cur.fetchall()
    if not menu_rows:
        raise RuntimeError("No menu_items found — cannot create order_items.")
    menu_map = {r[0]: float(r[1]) for r in menu_rows}
    menu_item_ids = list(menu_map.keys())

    populate_logger.info(f"{len(order_ids)} orders and {len(menu_item_ids)} menu items loaded.")

    order_item_rows = []   # tuples to insert: (order_id, item_id, quantity, price_at_time_of_order)
    order_totals = {}      # order_id -> running total

    # For each order, choose how many items to attach
    for oid in order_ids:
        item_count = max(1, int(random.gauss(avg_items_per_order, 1)))
        # ensure at least 1
        chosen_items = random.choices(menu_item_ids, k=item_count)  # allow repeats (same item twice)
        total_for_order = 0.0
        for item_id in chosen_items:
            quantity = random.randint(1, 3)
            price = menu_map[item_id]
            price_at_time = round(price * (1 + random.uniform(-0.05, 0.10)), 2)  # small price variation
            total_for_order += price_at_time * quantity
            order_item_rows.append((oid, item_id, quantity, price_at_time))
        order_totals[oid] = round(total_for_order, 2)

    # 3) Insert order_items in batches
    insert = ("INSERT INTO order_items (order_id, item_id, quantity, price_at_time_of_order) "
              "VALUES %s RETURNING order_item_id")
    try:
        inserted_count = 0
        batch = 2000
        for i in range(0, len(order_item_rows), batch):
            chunk = order_item_rows[i:i+batch]
            rows = execute_values(cur, insert, chunk, fetch=True)
            inserted_count += len(rows)
            populate_logger.info(f"Inserted order_items: {inserted_count}/{len(order_item_rows)}")
    except Exception as e:
        populate_logger.exception(f"Error inserting order_items: {e}")
        raise

    # 4) Update orders.total_amount using the order_totals computed
    populate_logger.info("Updating orders.total_amount from computed totals...")
    try:
        # Prepare list of (total_amount, order_id) for update
        update_pairs = [(order_totals[oid], oid) for oid in order_totals.keys()]
        # Update in batches using CASE to avoid many separate UPDATEs
        # Build a query like: UPDATE orders SET total_amount = data.total_amount FROM (VALUES (...) ) AS data(total_amount, order_id) WHERE orders.order_id = data.order_id;
        batch = 500
        for i in range(0, len(update_pairs), batch):
            chunk = update_pairs[i:i+batch]
            # build the VALUES clause
            values_sql = ','.join(['(%s, %s)'] * len(chunk))
            flat = [val for pair in chunk for val in pair]  # flatten (amount, oid) pairs
            sql = ("UPDATE orders SET total_amount = data.total_amount FROM (VALUES " +
                   values_sql +
                   ") AS data(total_amount, order_id) WHERE orders.order_id = data.order_id")
            cur.execute(sql, flat)
        populate_logger.info("Orders updated successfully.")
    except Exception as e:
        populate_logger.exception(f"Error updating orders total_amount: {e}")
        raise

    # compute total revenue
    total_revenue = sum(order_totals.values())
    populate_logger.info(f"Inserted {inserted_count} order_items; total revenue ~ {total_revenue:.2f}")

    return {"orders": len(order_ids), "order_items": inserted_count, "revenue": round(total_revenue, 2)}


# ---------- Main Function ----------
def main():
    parser = argparse.ArgumentParser(description="Populate RushMore Pizzeria Enterprise Database.")
    parser.add_argument("--config", default="dbconfig.yaml", help="Path to YAML configuration file")
    parser.add_argument("--stores", type=int, default=5, help="Number of stores to create")
    parser.add_argument("--customers", type=int, default=1000, help="Number of customers to create")
    parser.add_argument("--ingredients", type=int, default=50, help="Number of ingredients to create")
    parser.add_argument("--menu", type=int, default=30, help="Number of menu items to create")
    parser.add_argument("--orders", type=int, default=5000, help="Number of orders to create")
    args = parser.parse_args()

    faker = Faker()
    faker.unique.clear()  # reset uniqueness between runs

    cfg = load_config(args.config)

    with get_conn(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute("SET search_path TO pizzeria;")
            populate_logger.info("------ Starting Data Population ------")

            # Create tables data sequentially
            store_ids = create_stores(cur, faker, args.stores)
            customer_ids = create_customers(cur, faker, args.customers)
            ingredient_ids = create_ingredients(cur, faker, args.ingredients)
            menu_item_ids = create_menu_items(cur, faker, args.menu)
            create_item_ingredients(cur, menu_item_ids, ingredient_ids)
            create_orders(cur, customer_ids, store_ids, args.orders)
            create_order_items(cur)

            conn.commit()
            populate_logger.info("------ Data Population Completed Successfully ------")


# ---------- Entry Point ----------
if __name__ == "__main__":
    start_time = datetime.now()
    populate_logger.info("Executing RushMore Pizzeria data generation script...")
    main()
    elapsed = datetime.now() - start_time
    populate_logger.info(f"Data generation completed in {elapsed.seconds // 60} min {elapsed.seconds % 60} sec.")
