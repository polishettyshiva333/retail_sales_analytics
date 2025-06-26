import os
import shutil
from datetime import datetime

SOURCE_DIR = "/Users/shivacharan/retail_sales_analytics/data/incoming"
DEST_DIR = "/Users/shivacharan/retail_sales_analytics/data/raw/sales"
ARCHIVE_DIR = "/Users/shivacharan/retail_sales_analytics/data/archive"

def move_and_organize_csv():
    files = [f for f in os.listdir(SOURCE_DIR) if f.endswith(".csv")]
    if not files:
        print("No new files to ingest")
        return
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    target_path = os.path.join(DEST_DIR, date_str)
    os.makedirs(target_path, exist_ok=True)
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    for file in files:
        src = os.path.join(SOURCE_DIR, file)
        dst = os.path.join(target_path, file)
        shutil.move(src, dst)
        shutil.copy(dst, os.path.join(ARCHIVE_DIR, file))
        print(f"Ingested: {file} to the path:")
        print(DEST_DIR)

if __name__ == "__main__":
    move_and_organize_csv()       