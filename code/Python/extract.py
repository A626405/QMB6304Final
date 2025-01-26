import sqlite3
import gzip
import io

conn = sqlite3.connect('data/internal/databases.db')
cursor = conn.cursor()

cursor.execute("SELECT data FROM databases WHERE file_name = 'working_data.RDA';")
row = cursor.fetchone()

if row and row[0]:
    raw_data = row[0]

    try:
        with gzip.GzipFile(fileobj=io.BytesIO(raw_data)) as gz:
            decompressed_data = gz.read()
        
        with open("data/internal/workingdata_restored.RDA", "wb") as f:
            f.write(gzip.compress(decompressed_data,compresslevel=7,mtime=0))
        print("Decompressed data saved as output.rda.")
    
    except Exception as e:
        print("Error decompressing data:", e)
else:
    print("No data found or the data column is empty.")

conn.commit()
conn.close()
