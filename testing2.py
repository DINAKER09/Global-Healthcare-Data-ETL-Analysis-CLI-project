import requests
import mysql.connector
from mysql.connector import Error

# ========== CONFIGURATION ==========
API_URL = "https://api.api-ninjas.com/v1/hospitals"
API_KEY = "Q9kYwJQzB7rgYdkLcVFjJA==qVlfx6pqzZsymX8T"  # 🔁 Replace this with your real API key
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',  # 🔁 Replace
    'password': 'Kdreddy09041999@',  # 🔁 Replace
    'database': 'healthcare'  # 🔁 Replace
}

# ========== FETCH DATA FROM API ==========
try:
    headers = {"X-Api-Key": API_KEY}
    params = {"state": "FL"}  # 🔁 Adjust this as needed (e.g., "city": "Miami")
    response = requests.get(API_URL, headers=headers, params=params)
    response.raise_for_status()
    hospitals = response.json()
    print(f"✅ Retrieved {len(hospitals)} records from API.")

except requests.exceptions.HTTPError as e:
    print(f"❌ HTTP Error: {e}")
    exit()

except requests.exceptions.RequestException as e:
    print(f"❌ Request failed: {e}")
    exit()

# ========== CONNECT TO MYSQL AND INSERT DATA ==========
try:
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS hospitals (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255),
        care_type VARCHAR(100),
        address VARCHAR(255),
        city VARCHAR(100),
        state VARCHAR(10),
        zipcode VARCHAR(20),
        county VARCHAR(100),
        location_area_code VARCHAR(10),
        fips_code VARCHAR(20),
        timezone VARCHAR(10),
        latitude DOUBLE,
        longitude DOUBLE,
        phone_number VARCHAR(50),
        website VARCHAR(255),
        ownership VARCHAR(100),
        bedcount INT
    )
    ''')

    # Insert query
    insert_query = '''
    INSERT INTO hospitals (
        name, care_type, address, city, state, zipcode, county,
        location_area_code, fips_code, timezone, latitude, longitude,
        phone_number, website, ownership, bedcount
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    for rec in hospitals:
        try:
            cursor.execute(insert_query, (
                rec.get('name'),
                rec.get('care_type'),
                rec.get('address'),
                rec.get('city'),
                rec.get('state'),
                rec.get('zipcode'),
                rec.get('county'),
                rec.get('location_area_code'),
                rec.get('fips_code'),
                rec.get('timezone'),
                float(rec['latitude']) if rec.get('latitude') else None,
                float(rec['longitude']) if rec.get('longitude') else None,
                rec.get('phone_number'),
                rec.get('website'),
                rec.get('ownership'),
                rec.get('bedcount')
            ))
        except Error as insert_error:
            print(f"⚠️ Failed to insert record {rec.get('name')}: {insert_error}")
            continue

    conn.commit()
    print("✅ Data successfully inserted into MySQL.")

except Error as db_error:
    print(f"❌ Database error: {db_error}")

finally:
    if 'conn' in locals() and conn.is_connected():
        cursor.close()
        conn.close()
        print("🔌 MySQL connection closed.")
