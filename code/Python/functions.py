def create_db(db_path):
  import sqlite3
  try:
      if db_path:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS main_datasets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT,
            data BLOB
        );''')
        
        conn.commit()
        cursor.execute("CREATE INDEX file_name_idx1 ON main_datasets(file_name);")
        conn.commit()

        cursor.execute("VACUUM;")
        conn.commit()
        conn.close()
        return print("Database Successfully Created. Indexed by file_name.")
        
      else:
          return print("Path, file name, or database path is missing.")
  except sqlite3.Error as e:
        print(f"An error occurred with SQLite: '{e}'")
  except FileNotFoundError:
        print(f"The specified file '{db_path}' does not exist.")
  except Exception as e:
        print(f"An unexpected error occurred: '{e}'")
        
        
        
        
        





def write_db(rda_path,rda_name,db_path,tbl_name):
  import sqlite3
  
  try:
    if rda_path and rda_name and db_path and tbl_name:
      with open(rda_path, 'rb') as f:
        binary_data = f.read()

      conn = sqlite3.connect(db_path)
      cursor = conn.cursor()

      cursor.execute(f"INSERT INTO {tbl_name} (file_name, data) VALUES (?, ?)", (rda_name, binary_data))
      conn.commit()

      cursor.execute("VACUUM;")
      conn.commit()
      conn.close()
      print("Dataframe Successfully Uploaded Into The Database")
      
    else:
        print('Path or file name is missing.')
  except sqlite3.Error as e:
    print(f"An error occurred with SQLite: '{e}'")
  except FileNotFoundError:
    print(f"The specified file '{rda_path}' does not exist.")
  except FileNotFoundError:
    print(f"The specified file '{db_path}' does not exist.")
  except Exception as e:
    print(f"An unexpected error occurred: '{e}'")
    
    
    
    
    
    

'''def read_db(path_to_db_char,file_name):
    import sqlite3
    
    try:
        if path_to_db_char and file_name is not None:
            conn = sqlite3.connect(path_to_db_char)
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM databases WHERE file_name = ?", (file_name,))
            cursor.execute(".save data")
            conn.close()
            conn.commit()
        else:
            print("Path, file name, or database path is missing.")
            return None
    except sqlite3.Error as e:
        print(f"An error occurred with SQLite: '{e}'")
    except Exception as e:
        print(f"An unexpected error occurred: '{e}'")'''





import geoip2.database
import pandas as pd
def process_ip_file1(input_csv, output_csv, db_path='data/external/GeoLite2-Country.mmdb'):

    reader = geoip2.database.Reader(db_path)

    def get_country(ip):
        """Retrieves the country name for a given IP using the MaxMind database."""
        try:
            response = reader.country(ip)
            return response.country.name if response.country.name else None
        except geoip2.errors.AddressNotFoundError:
            return None  

    df = pd.read_csv(input_csv)

    df['country'] = df['x'].apply(get_country)

    df = df.dropna(subset=['country'])

    df.to_csv(output_csv, index=False)

    print(f"Saved the output to '{output_csv}'")







import requests
import pandas as pd
import time

def get_country(ip):
    """Fetch country for an IP using ip-api.com without proxies, handling failures."""
    url = f"http://ip-api.com/json/{ip}"
    
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        if data.get("status") == "success":
            return data.get("country")
        else:
            return None 
    except requests.RequestException:
        return None 

def process_ip_file(input_csv, output_csv):
    df = pd.read_csv(input_csv)
    
    results = []
    for index, ip in enumerate(df['x']):
        country = get_country(ip)
        results.append(country)

        if (index + 1) % 38 == 0:
            print(f"Processed {index + 1} IPs, waiting 60 seconds...")
            time.sleep(60)

    df['country'] = results

    df = df.dropna(subset=['country'])

    df.to_csv(output_csv, index=False)
    print(f"Saved output to {output_csv}")

