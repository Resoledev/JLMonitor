import csv
import os
import shutil
from datetime import datetime

CSV_FILE = 'johnlewisv2.csv'
BACKUP_FILE = 'johnlewisv2.csv.bak'

NEW_FIELDNAMES = [
    'Product ID', 'Product Name', 'Current Price', 'Original Price', 
    'Discount', 'Stock Status', 'Sizes', 'URL', 'Event Type', 
    'Timestamp', 'Image', 'Category', 'Variants'
]

def extract_product_id(url):
    try:
        return url.split('/')[-1]
    except:
        return "unknown"

def migrate_csv():
    if not os.path.exists(CSV_FILE):
        print(f"File {CSV_FILE} not found.")
        return

    # Create backup
    shutil.copy2(CSV_FILE, BACKUP_FILE)
    print(f"Created backup at {BACKUP_FILE}")

    rows_to_write = []
    
    with open(CSV_FILE, 'r', encoding='utf-8', errors='replace') as f:
        # Read lines manually to handle mixed row lengths
        lines = f.readlines()

    # Skip header if it's the old one
    start_index = 0
    if lines[0].startswith('Name,Current Price'):
        print("Detected old header.")
        start_index = 1
    elif lines[0].startswith('Product ID,Product Name'):
        print("Detected new header.")
        start_index = 1
    
    for i in range(start_index, len(lines)):
        line = lines[i].strip()
        if not line:
            continue
            
        # Use csv.reader to parse the line correctly handling quotes
        reader = csv.reader([line])
        row_values = next(reader)
        
        new_row = {}
        
        if len(row_values) == 10:
            # Old format row
            # 0: Name, 1: Curr, 2: Orig, 3: Disc, 4: Stock, 5: Sizes, 6: URL, 7: Event, 8: Time, 9: Image
            new_row['Product ID'] = extract_product_id(row_values[6])
            new_row['Product Name'] = row_values[0]
            new_row['Current Price'] = row_values[1]
            new_row['Original Price'] = row_values[2]
            new_row['Discount'] = row_values[3]
            new_row['Stock Status'] = row_values[4]
            new_row['Sizes'] = row_values[5]
            new_row['URL'] = row_values[6]
            new_row['Event Type'] = row_values[7]
            new_row['Timestamp'] = row_values[8]
            new_row['Image'] = row_values[9]
            new_row['Category'] = "John Lewis" # Default
            new_row['Variants'] = ""
            
        elif len(row_values) == 13:
            # New format row
            # Map by index to be safe, assuming order matches NEW_FIELDNAMES
            for idx, field in enumerate(NEW_FIELDNAMES):
                new_row[field] = row_values[idx]
        
        elif len(row_values) == 9:
             # Old format row but missing image? Or just header?
             # If it's a data row with 9 cols, it's missing image
            new_row['Product ID'] = extract_product_id(row_values[6])
            new_row['Product Name'] = row_values[0]
            new_row['Current Price'] = row_values[1]
            new_row['Original Price'] = row_values[2]
            new_row['Discount'] = row_values[3]
            new_row['Stock Status'] = row_values[4]
            new_row['Sizes'] = row_values[5]
            new_row['URL'] = row_values[6]
            new_row['Event Type'] = row_values[7]
            new_row['Timestamp'] = row_values[8]
            new_row['Image'] = ""
            new_row['Category'] = "John Lewis"
            new_row['Variants'] = ""

        else:
            print(f"Skipping row {i+1} with unexpected column count: {len(row_values)}")
            continue

        rows_to_write.append(new_row)

    # Write new CSV
    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=NEW_FIELDNAMES, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows_to_write)
    
    print(f"Successfully migrated {len(rows_to_write)} rows to new format.")

if __name__ == "__main__":
    migrate_csv()
