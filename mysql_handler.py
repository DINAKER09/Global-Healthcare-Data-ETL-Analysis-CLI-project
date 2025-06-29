import mysql.connector
import logging

class MySQLHandler:
    def __init__(self, host, user, password, database):
        self.conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )

    def create_tables(self):
        cursor = self.conn.cursor()
        with open("sql/create_tables.sql", "r") as f:
            cursor.execute(f.read(), multi=True)
        cursor.close()

    def insert_data(self, table_name, data_df):
        cursor = self.conn.cursor()

        insert_query = f"""
        INSERT INTO {table_name} (
            name, care_type, address, city, state, zipcode, county,
            location_area_code, fips_code, timezone, latitude, longitude,
            phone_number, website, ownership, bedcount
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            care_type = VALUES(care_type),
            address = VALUES(address),
            city = VALUES(city),
            state = VALUES(state),
            zipcode = VALUES(zipcode),
            county = VALUES(county),
            location_area_code = VALUES(location_area_code),
            fips_code = VALUES(fips_code),
            timezone = VALUES(timezone),
            latitude = VALUES(latitude),
            longitude = VALUES(longitude),
            phone_number = VALUES(phone_number),
            website = VALUES(website),
            ownership = VALUES(ownership),
            bedcount = VALUES(bedcount);
        """

        # Extract required fields from DataFrame
        columns = [
            'name', 'care_type', 'address', 'city', 'state', 'zipcode', 'county',
            'location_area_code', 'fips_code', 'timezone', 'latitude', 'longitude',
            'phone_number', 'website', 'ownership', 'bedcount'
        ]

        # Ensure numeric and null values are properly converted
        records = data_df[columns].values.tolist()
        print(records)
        for i, rec in enumerate(records):
            rec[10] = float(rec[10]) if rec[10] not in (None, '') else None  # latitude
            rec[11] = float(rec[11]) if rec[11] not in (None, '') else None  # longitude
            try:
                rec[15] = int(rec[15]) if rec[15] not in (None, '') else None
            except ValueError:
                rec[15] = None
            records[i] = rec
            

        try:
            cursor.executemany(insert_query, records)
            self.conn.commit()
            logging.info(f"✅ Inserted {cursor.rowcount} hospital records into '{table_name}'")
        except mysql.connector.Error as e:
            logging.error(f"❌ Failed to insert data: {e}")
            self.conn.rollback()
        finally:
            cursor.close()

    def query(self, sql, params=None):
        cursor = self.conn.cursor()
        cursor.execute(sql, params or ())
        results = cursor.fetchall()
        cursor.close()
        return results

    def list_tables(self):
        return self.query("SHOW TABLES")

    def drop_tables(self):
        cursor = self.conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS hospitals")
        self.conn.commit()
        cursor.close()

    def close(self):
        self.conn.close()
