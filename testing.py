import requests
import json

name = 'General Hospital'
api_key = 'Q9kYwJQzB7rgYdkLcVFjJA==qVlfx6pqzZsymX8T'  # Replace with your actual API key

# Construct the API URL
api_url = f'https://api.api-ninjas.com/v1/hospitals?name={name}'

# Make the API call
response = requests.get(api_url, headers={'X-Api-Key': api_key})

# Check the response
if response.status_code == requests.codes.ok:
    data = response.json()

    print(f"\n✅ Number of records returned: {len(data)}")

    if data:
        print("\n🗂️  Keys (columns) in each record:")
        print(list(data[0].keys()))

        print("\n📋 All hospital records:")
        for i, record in enumerate(data, start=1):
            print(f"\n--- Record {i} ---")
            print(json.dumps(record, indent=4))
    else:
        print("No data returned.")
else:
    print("❌ Error:", response.status_code, response.text)

