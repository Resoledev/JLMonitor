#!/usr/bin/env python3
"""
One-time script to deduplicate johnlewisv2.csv
Keeps only the most recent entry per Product ID.
"""

import csv
import shutil
from datetime import datetime
from collections import defaultdict
import os

CSV_FILE = 'johnlewisv2.csv'
BACKUP_FILE = f'johnlewisv2_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'


def dedupe_csv():
    if not os.path.exists(CSV_FILE):
        print(f"Error: {CSV_FILE} not found")
        return

    # Create backup
    shutil.copy(CSV_FILE, BACKUP_FILE)
    print(f"Backup created: {BACKUP_FILE}")

    # Read all rows and group by Product ID
    products_by_id = defaultdict(list)
    headers = None

    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        for row in reader:
            product_id = row.get('Product ID', '')
            timestamp_str = row.get('Timestamp', '')
            try:
                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            except:
                timestamp = datetime.min
            products_by_id[product_id].append((timestamp, row))

    # For each product ID, keep only the most recent entry
    deduplicated_rows = []
    for product_id, entries in products_by_id.items():
        # Sort by timestamp descending and keep the most recent
        entries.sort(key=lambda x: x[0], reverse=True)
        most_recent = entries[0][1]
        deduplicated_rows.append(most_recent)

    # Sort final output by timestamp (most recent first)
    def get_timestamp(row):
        try:
            return datetime.strptime(row.get('Timestamp', ''), '%Y-%m-%d %H:%M:%S')
        except:
            return datetime.min

    deduplicated_rows.sort(key=get_timestamp, reverse=True)

    # Write back
    with open(CSV_FILE, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(deduplicated_rows)

    original_count = sum(len(entries) for entries in products_by_id.values())
    final_count = len(deduplicated_rows)
    removed = original_count - final_count

    print(f"\nDeduplication complete!")
    print(f"  Original rows: {original_count}")
    print(f"  Unique Product IDs: {len(products_by_id)}")
    print(f"  Rows after dedup: {final_count}")
    print(f"  Duplicates removed: {removed}")

    # Also fix event type casing while we're at it
    fix_event_types()


def fix_event_types():
    """Normalize event types to consistent casing."""
    rows = []
    headers = None
    fixed_count = 0

    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        for row in reader:
            event_type = row.get('Event Type', '')
            # Normalize: "new" -> "New", "price_change" -> "Price_change"
            if event_type == 'new':
                row['Event Type'] = 'New'
                fixed_count += 1
            elif event_type == 'price_change':
                row['Event Type'] = 'Price_change'
                fixed_count += 1
            rows.append(row)

    if fixed_count > 0:
        with open(CSV_FILE, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
        print(f"  Event types normalized: {fixed_count}")


if __name__ == '__main__':
    dedupe_csv()
