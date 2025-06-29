import pandas as pd

class DataTransformer:
    def clean_and_transform(self, raw_data):
        df = pd.DataFrame(raw_data)

        # Convert numeric columns with nulls or incorrect formats
        for col in ['bedcount', 'latitude', 'longitude']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Fill missing bed counts with 0
        df['bedcount'] = df['bedcount'].fillna(0).astype(int)

        # Fill missing website with placeholder
        df['website'] = df['website'].fillna('Not Available')

        # Standardize column names (optional step)
        df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]

        # Drop duplicates if needed (based on name & address)
        df.drop_duplicates(subset=['name', 'address'], inplace=True)

        return df