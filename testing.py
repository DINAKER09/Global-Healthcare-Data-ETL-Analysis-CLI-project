import requests

name = 'General Hospital'
api_key = 'Q9kYwJQzB7rgYdkLcVFjJA==qVlfx6pqzZsymX8T'  # Replace with your actual API key

# Construct the API URL
api_url = f'https://api.api-ninjas.com/v1/hospitals?name={name}'

# Make the API call
response = requests.get(api_url, headers={'X-Api-Key': api_key})

# Check the response
if response.status_code == requests.codes.ok:
    data = response.json()
    for hospital in data:
        print("Name:", hospital.get("name", "N/A"))
        print("Address:", hospital.get("address", "N/A"))
        print("City:", hospital.get("city", "N/A"))
        print("State:", hospital.get("state", "N/A"))
        print("Zip:", hospital.get("zip", "N/A"))
        print("-" * 40)
else:
    print("Error:", response.status_code, response.text)
