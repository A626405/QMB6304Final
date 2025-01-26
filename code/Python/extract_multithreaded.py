import sqlite3
import gzip
import io
from concurrent.futures import ThreadPoolExecutor

def process_row(raw_data, output_file):
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(raw_data)) as gz:
            decompressed_data = gz.read()
        
        with open(output_file, "wb") as f:
            f.write(gzip.compress(decompressed_data, compresslevel=9, mtime=0))
        
        print(f"Decompressed data saved as {output_file}.")
    except Exception as e:
        print(f"Error decompressing data: {e}")

def main():
    db_path = 'data/internal/datasets.db'
    output_file = "data/internal/temp/workingdata_restored.RDA"

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT data FROM main_datasets WHERE file_name = 'working_data.RDA';")
    rows = cursor.fetchall()

    with ThreadPoolExecutor() as executor:
        futures = []
        for row in rows:
            if row and row[0]:
                raw_data = row[0]
                futures.append(executor.submit(process_row, raw_data, output_file))
        
        for future in futures:
            future.result()  

    conn.close()

if __name__ == "__main__":
    main()
