# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Generate raw data
# MAGIC Simulates a TCG marketplace vendor dropping files into object storage:
# MAGIC 6 entities written as JSON to a Unity Catalog **Volume** (the lakehouse
# MAGIC answer to "an S3 landing bucket"). The domain is a Pokémon TCG
# MAGIC marketplace — collectors buying singles from card shops — sized to run on
# MAGIC Free Edition's daily quota many times over.
# MAGIC
# MAGIC The data is deliberately dirty, because vendor data always is:
# MAGIC - ~2% of collectors have a **null email** (severity: warn demo)
# MAGIC - ~1% of orders point at a **collector that doesn't exist** (RI threshold demo)
# MAGIC - 15 order_items point at **cards that don't exist** (RI threshold demo)
# MAGIC - (opt-in, commented out below) payments pointing at **orders that don't
# MAGIC   exist** — uncomment to trip the zero-tolerance RI test live

# COMMAND ----------

import json
import random
from datetime import datetime, timedelta

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "lakehouse_demo")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw")
raw_root = f"/Volumes/{catalog}/{schema}/raw"

random.seed(42)  # same data every run — demos should be reproducible

# COMMAND ----------

N_COLLECTORS, N_SHOPS, N_CARDS, N_ORDERS = 1_000, 50, 500, 10_000

STATES = ["CA", "NY", "TX", "FL", "WA", "IL", "MA", "CO"]
CITIES = ["Springfield", "Riverton", "Lakeside", "Fairview", "Georgetown"]

POKEMON = [
    "Charizard", "Pikachu", "Mewtwo", "Mew", "Umbreon", "Espeon", "Sylveon",
    "Eevee", "Gengar", "Dragonite", "Gyarados", "Blastoise", "Venusaur",
    "Snorlax", "Lugia", "Ho-Oh", "Rayquaza", "Garchomp", "Lucario",
    "Greninja", "Mimikyu", "Alakazam", "Machamp", "Tyranitar", "Salamence",
    "Metagross", "Gardevoir", "Zoroark", "Arcanine", "Lapras", "Ditto",
    "Jigglypuff", "Scizor", "Aerodactyl", "Dracaufeu", "Leafeon", "Glaceon",
    "Vaporeon", "Jolteon", "Flareon", "Absol", "Darkrai", "Giratina",
    "Dialga", "Palkia", "Arceus", "Zekrom", "Reshiram", "Xerneas", "Yveltal",
]
VARIANTS = ["", "", "", " ex", " V", " VMAX", " VSTAR", " GX"]
SETS = [
    "Base Set", "Jungle", "Fossil", "Team Rocket", "Neo Genesis",
    "Evolving Skies", "Lost Origin", "Crown Zenith", "Scarlet & Violet 151",
    "Obsidian Flames", "Paldea Evolved", "Paradox Rift", "Temporal Forces",
    "Twilight Masquerade", "Surging Sparks", "Prismatic Evolutions",
]
TYPES = ["Fire", "Water", "Grass", "Lightning", "Psychic", "Fighting",
         "Darkness", "Metal", "Dragon", "Colorless"]
# Rarity drives price — the pareto shape every card economy has: commons are
# pennies, chase cards carry the whole market.
RARITIES = [
    ("Common",                      (0.10, 1.00),   40),
    ("Uncommon",                    (0.25, 2.50),   25),
    ("Rare",                        (0.50, 6.00),   15),
    ("Holo Rare",                   (2.00, 25.00),  10),
    ("Ultra Rare",                  (10.00, 90.00),  5),
    ("Illustration Rare",           (15.00, 160.00), 3),
    ("Special Illustration Rare",   (60.00, 450.00), 1),
    ("Secret Rare",                 (30.00, 300.00), 1),
]
# The names that always command a premium regardless of set.
CHASE = {"Charizard", "Pikachu", "Umbreon", "Mewtwo", "Rayquaza", "Lugia"}

STATUSES = ["completed", "completed", "completed", "shipped", "returned", "cancelled"]
METHODS = ["card", "card", "card", "paypal", "store_credit"]
CONDITIONS = ["NM", "NM", "NM", "NM", "LP", "LP", "MP", "HP", "DMG"]
HANDLES = ["Shiny", "Holo", "Master", "Rogue", "Cosmic", "Retro", "Vintage",
           "Graded", "Binder", "Mint"]

base_date = datetime(2025, 1, 1)

collectors = [{
    "collector_id": cid,
    "handle": f"{random.choice(HANDLES)}{random.choice(POKEMON)}{cid}",
    # dirt #1: ~2% null emails — real vendors do this constantly
    "email": None if random.random() < 0.02 else f"collector{cid}@example.com",
    "state": random.choice(STATES),
    "signup_date": (base_date - timedelta(days=random.randint(0, 900))).strftime("%Y-%m-%d"),
} for cid in range(1, N_COLLECTORS + 1)]

shops = [{
    "shop_id": sid,
    "shop_name": f"{random.choice(CITIES)} {random.choice(['Cards', 'TCG', 'Collectibles', 'Game Haven', 'Card Vault'])}",
    "city": random.choice(CITIES),
    "state": random.choice(STATES),
} for sid in range(1, N_SHOPS + 1)]

rarity_names = [r[0] for r in RARITIES]
rarity_weights = [r[2] for r in RARITIES]
rarity_price = {r[0]: r[1] for r in RARITIES}

cards = []
for card_id in range(1, N_CARDS + 1):
    pokemon = random.choice(POKEMON)
    rarity = random.choices(rarity_names, weights=rarity_weights)[0]
    lo, hi = rarity_price[rarity]
    price = random.uniform(lo, hi)
    if pokemon in CHASE:
        price *= random.uniform(1.5, 4.0)  # chase-card premium
    cards.append({
        "card_id": card_id,
        "card_name": f"{pokemon}{random.choice(VARIANTS)}",
        "set_name": random.choice(SETS),
        "card_number": f"{random.randint(1, 250)}/{random.choice([102, 165, 198, 203, 250])}",
        "rarity": rarity,
        "card_type": random.choice(TYPES),
        "market_price": round(price, 2),
    })

orders = []
for oid in range(1, N_ORDERS + 1):
    # dirt #2: ~1% of orders reference a collector that was never sent to us
    collector_id = random.randint(9000, 9999) if random.random() < 0.01 else random.randint(1, N_COLLECTORS)
    orders.append({
        "order_id": oid,
        "collector_id": collector_id,
        "shop_id": random.randint(1, N_SHOPS),
        "order_ts": (base_date + timedelta(minutes=random.randint(0, 260_000))).strftime("%Y-%m-%d %H:%M:%S"),
        "amount": round(random.uniform(2, 500), 2),
        "status": random.choice(STATUSES),
    })

order_items, iid = [], 1
for o in orders:
    for _ in range(random.randint(1, 3)):
        order_items.append({
            "order_item_id": iid,
            "order_id": o["order_id"],
            "card_id": random.randint(1, N_CARDS),
            "quantity": random.randint(1, 4),
            "condition": random.choice(CONDITIONS),
        })
        iid += 1
# dirt #4: 15 items referencing cards that don't exist
for _ in range(15):
    order_items.append({
        "order_item_id": iid,
        "order_id": random.randint(1, N_ORDERS),
        "card_id": random.randint(8000, 8999),
        "quantity": 1,
        "condition": "NM",
    })
    iid += 1

payments, pid = [], 1
for o in orders:
    if o["status"] in ("completed", "shipped", "returned"):
        payments.append({
            "payment_id": pid,
            "order_id": o["order_id"],
            "method": random.choice(METHODS),
            "paid_amount": o["amount"],
        })
        pid += 1
# LIVE LEVER #2: payments for orders that don't exist — money we can't
# explain. The dbt test on payments.order_id is zero-tolerance (no threshold),
# so uncommenting this fails the gate on the very next run.
# for _ in range(30):
#     payments.append({
#         "payment_id": pid,
#         "order_id": random.randint(50_000, 59_999),
#         "method": random.choice(METHODS),
#         "paid_amount": round(random.uniform(2, 500), 2),
#     })
#     pid += 1

# COMMAND ----------

def write_jsonl(name, rows):
    path = f"{raw_root}/{name}/{name}.json"
    dbutils.fs.mkdirs(f"{raw_root}/{name}")
    dbutils.fs.put(path, "\n".join(json.dumps(r) for r in rows), overwrite=True)
    print(f"wrote {len(rows):>6} rows -> {path}")

for name, rows in [
    ("collectors", collectors), ("shops", shops), ("cards", cards),
    ("orders", orders), ("order_items", order_items), ("payments", payments),
]:
    write_jsonl(name, rows)
