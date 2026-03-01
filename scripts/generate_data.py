"""
FranchisePulse — Sales Data Generator
======================================
Generates 90 days of realistic POS sales CSVs for 12 franchise locations.
Output lands in data/raw/<YYYY-MM-DD>/<store_id>_sales_<YYYY-MM-DD>.csv

Intentional messiness (mirrors real-world pipeline problems):
  - Store S07 goes silent for 3 days (missing files)
  - Store S11 occasionally sends duplicate rows
  - Store S03 has some malformed unit prices (letters in numeric field)
  - Store S09 sends a file with UTF-16 encoding instead of UTF-8
  - Random ~1% of rows have null cashier_id
  - Random ~0.5% of rows have negative quantity (refunds, realistic)

Usage:
  python scripts/generate_data.py

  Optional args:
  --days        Number of days to generate (default: 90)
  --start-date  Start date in YYYY-MM-DD format (default: 90 days ago)
  --output-dir  Root data directory (default: data/raw)
  --seed        Random seed for reproducibility (default: 42)
"""

import argparse
import csv
import os
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker("en_IE")  # Irish locale for realistic names


# -----------------------------------------------------------
# Configuration
# -----------------------------------------------------------

STORES = [
    {"store_id": "S01", "name": "FranchisePulse Grafton St",      "city": "Dublin",     "region": "Leinster", "owner": "Murphy Catering Ltd"},
    {"store_id": "S02", "name": "FranchisePulse Dundrum",         "city": "Dublin",     "region": "Leinster", "owner": "Murphy Catering Ltd"},
    {"store_id": "S03", "name": "FranchisePulse Cork City",        "city": "Cork",       "region": "Munster",  "owner": "O'Brien Foods Ltd"},
    {"store_id": "S04", "name": "FranchisePulse Mahon Point",      "city": "Cork",       "region": "Munster",  "owner": "O'Brien Foods Ltd"},
    {"store_id": "S05", "name": "FranchisePulse Galway",           "city": "Galway",     "region": "Connacht", "owner": "Walsh Hospitality"},
    {"store_id": "S06", "name": "FranchisePulse Eyre Square",      "city": "Galway",     "region": "Connacht", "owner": "Walsh Hospitality"},
    {"store_id": "S07", "name": "FranchisePulse Limerick",         "city": "Limerick",   "region": "Munster",  "owner": "Ryan Group"},
    {"store_id": "S08", "name": "FranchisePulse Waterford",        "city": "Waterford",  "region": "Munster",  "owner": "Ryan Group"},
    {"store_id": "S09", "name": "FranchisePulse Kilkenny",         "city": "Kilkenny",   "region": "Leinster", "owner": "Brennan Retail"},
    {"store_id": "S10", "name": "FranchisePulse Drogheda",         "city": "Drogheda",   "region": "Leinster", "owner": "Brennan Retail"},
    {"store_id": "S11", "name": "FranchisePulse Sligo",            "city": "Sligo",      "region": "Connacht", "owner": "Kelly Ventures"},
    {"store_id": "S12", "name": "FranchisePulse Athlone",          "city": "Athlone",    "region": "Leinster", "owner": "Kelly Ventures"},
]

PRODUCTS = [
    {"sku": "HOT001", "name": "Espresso",            "category": "Hot Drinks",  "price": 2.50},
    {"sku": "HOT002", "name": "Americano",            "category": "Hot Drinks",  "price": 3.00},
    {"sku": "HOT003", "name": "Flat White",           "category": "Hot Drinks",  "price": 3.50},
    {"sku": "HOT004", "name": "Cappuccino",           "category": "Hot Drinks",  "price": 3.50},
    {"sku": "HOT005", "name": "Latte",                "category": "Hot Drinks",  "price": 3.80},
    {"sku": "HOT006", "name": "Hot Chocolate",        "category": "Hot Drinks",  "price": 3.80},
    {"sku": "COL001", "name": "Iced Latte",           "category": "Cold Drinks", "price": 4.20},
    {"sku": "COL002", "name": "Iced Americano",       "category": "Cold Drinks", "price": 3.80},
    {"sku": "COL003", "name": "Cold Brew",            "category": "Cold Drinks", "price": 4.50},
    {"sku": "COL004", "name": "Sparkling Water",      "category": "Cold Drinks", "price": 1.80},
    {"sku": "FOD001", "name": "Butter Croissant",     "category": "Food",        "price": 2.80},
    {"sku": "FOD002", "name": "Blueberry Muffin",     "category": "Food",        "price": 3.20},
    {"sku": "FOD003", "name": "Chicken Wrap",         "category": "Food",        "price": 6.50},
    {"sku": "FOD004", "name": "Ham & Cheese Toastie", "category": "Food",        "price": 5.80},
    {"sku": "FOD005", "name": "Banana Bread",         "category": "Food",        "price": 3.00},
    {"sku": "MER001", "name": "Branded Mug",          "category": "Merch",       "price": 12.00},
    {"sku": "MER002", "name": "Reusable Cup",         "category": "Merch",       "price": 8.00},
]

PAYMENT_METHODS = ["Card", "Cash", "Contactless", "Apple Pay", "Google Pay"]

# Product weights — drinks sell more than merch
PRODUCT_WEIGHTS = [8, 8, 7, 7, 6, 5, 5, 4, 3, 3, 6, 5, 4, 4, 4, 1, 2]

# Cashier IDs per store (5 cashiers each)
def get_cashiers(store_id):
    return [f"{store_id}_C{str(i).zfill(2)}" for i in range(1, 6)]


# -----------------------------------------------------------
# Messiness rules
# -----------------------------------------------------------

# S07 goes silent for 3 consecutive days somewhere in the middle
def get_silent_dates(start_date, num_days):
    silent = set()
    for offset in range(30, 33):
        silent.add(("S07", (start_date + timedelta(days=offset)).date()))
    return silent


def maybe_corrupt_price(store_id, price):
    """S03 occasionally has malformed prices."""
    if store_id == "S03" and random.random() < 0.02:
        return f"{price}X"  # e.g. "3.50X" — will fail numeric cast
    return f"{price:.2f}"


def maybe_null_cashier(cashier_id):
    """~1% of rows have null cashier_id across all stores."""
    if random.random() < 0.01:
        return ""
    return cashier_id


def generate_transaction_id(store_id, date, seq):
    return f"{store_id}-{date.strftime('%Y%m%d')}-{str(seq).zfill(5)}"


# -----------------------------------------------------------
# Row generation
# -----------------------------------------------------------

def generate_rows(store, date, num_transactions):
    """Generate transaction rows for one store on one date."""
    rows = []
    cashiers = get_cashiers(store["store_id"])

    for seq in range(1, num_transactions + 1):
        product = random.choices(PRODUCTS, weights=PRODUCT_WEIGHTS, k=1)[0]
        quantity = random.choices(
            [-1, 1, 1, 1, 1, 1, 2, 2, 3],  # mostly 1, occasional 2-3, rare refund
            k=1
        )[0]
        discount = round(random.choices(
            [0.00, 0.00, 0.00, 0.10, 0.15, 0.20],
            weights=[60, 15, 10, 8, 5, 2],
            k=1
        )[0], 2)

        # Random time during trading hours (7am - 8pm)
        hour = random.randint(7, 19)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        transaction_dt = datetime(date.year, date.month, date.day, hour, minute, second)

        row = {
            "transaction_id":  generate_transaction_id(store["store_id"], date, seq),
            "store_id":        store["store_id"],
            "transaction_date": transaction_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "product_sku":     product["sku"],
            "product_name":    product["name"],
            "category":        product["category"],
            "quantity":        quantity,
            "unit_price":      maybe_corrupt_price(store["store_id"], product["price"]),
            "discount_applied": f"{discount:.2f}",
            "payment_method":  random.choice(PAYMENT_METHODS),
            "cashier_id":      maybe_null_cashier(random.choice(cashiers)),
        }
        rows.append(row)

    return rows


# -----------------------------------------------------------
# Duplicate injection (S11)
# -----------------------------------------------------------

def inject_duplicates(rows, store_id):
    """S11 occasionally resends ~2% of rows as duplicates."""
    if store_id != "S11":
        return rows
    duplicates = random.sample(rows, max(1, int(len(rows) * 0.02)))
    rows.extend(duplicates)
    random.shuffle(rows)
    return rows


# -----------------------------------------------------------
# File writing
# -----------------------------------------------------------

FIELDNAMES = [
    "transaction_id", "store_id", "transaction_date",
    "product_sku", "product_name", "category",
    "quantity", "unit_price", "discount_applied",
    "payment_method", "cashier_id"
]


def write_csv(filepath, rows, encoding="utf-8"):
    with open(filepath, "w", newline="", encoding=encoding) as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


# -----------------------------------------------------------
# Main
# -----------------------------------------------------------

def generate(days, start_date, output_dir, seed):
    random.seed(seed)
    fake.seed_instance(seed)

    output_path = Path(output_dir)
    silent_dates = get_silent_dates(start_date, days)

    total_files = 0
    total_rows = 0
    skipped = 0

    print(f"\nFranchisePulse Data Generator")
    print(f"{'='*50}")
    print(f"Start date : {start_date.strftime('%Y-%m-%d')}")
    print(f"Days       : {days}")
    print(f"Stores     : {len(STORES)}")
    print(f"Output     : {output_path.resolve()}")
    print(f"Seed       : {seed}")
    print(f"{'='*50}\n")

    for day_offset in range(days):
        current_date = start_date + timedelta(days=day_offset)
        date_str = current_date.strftime("%Y-%m-%d")
        date_folder = output_path / date_str
        date_folder.mkdir(parents=True, exist_ok=True)

        for store in STORES:
            store_id = store["store_id"]

            # Skip silent dates
            if (store_id, current_date.date()) in silent_dates:
                print(f"  [SKIP] {store_id} — silent day ({date_str})")
                skipped += 1
                continue

            # Vary transaction volume — weekends busier
            is_weekend = current_date.weekday() >= 5
            base = random.randint(280, 380) if is_weekend else random.randint(180, 280)

            rows = generate_rows(store, current_date, base)
            rows = inject_duplicates(rows, store_id)

            filename = f"{store_id}_sales_{date_str}.csv"
            filepath = date_folder / filename

            # S09 writes UTF-16 instead of UTF-8
            encoding = "utf-16" if store_id == "S09" else "utf-8"
            write_csv(filepath, rows, encoding=encoding)

            total_files += 1
            total_rows += len(rows)

        # Progress every 10 days
        if (day_offset + 1) % 10 == 0:
            print(f"  [{date_str}] Day {day_offset + 1}/{days} complete")

    print(f"\n{'='*50}")
    print(f"Generation complete.")
    print(f"  Files generated : {total_files}")
    print(f"  Files skipped   : {skipped}  (intentional — S07 silent days)")
    print(f"  Total rows      : {total_rows:,}")
    print(f"  Output folder   : {output_path.resolve()}")
    print(f"{'='*50}\n")


def parse_args():
    parser = argparse.ArgumentParser(description="FranchisePulse sales data generator")
    parser.add_argument("--days",       type=int, default=90,   help="Number of days to generate")
    parser.add_argument("--start-date", type=str, default=None, help="Start date YYYY-MM-DD (default: 90 days ago)")
    parser.add_argument("--output-dir", type=str, default="data/raw", help="Output root directory")
    parser.add_argument("--seed",       type=int, default=42,   help="Random seed")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.start_date:
        start = datetime.strptime(args.start_date, "%Y-%m-%d")
    else:
        start = datetime.now() - timedelta(days=args.days)
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)

    generate(
        days=args.days,
        start_date=start,
        output_dir=args.output_dir,
        seed=args.seed,
    )