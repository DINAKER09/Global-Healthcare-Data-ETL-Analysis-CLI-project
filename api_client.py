import requests
import logging
import configparser

class APIClient:
    def __init__(self, base_url, api_key):
        self.base_url = base_url.rstrip('/')  # Remove trailing slash if any
        self.headers = {"X-Api-Key": api_key}

    def fetch_data(self, endpoint, params=None):
        """Fetch data from a specific API endpoint with optional parameters."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"  # Ensure correct URL format
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"API error for endpoint '{endpoint}': {e}")
            return []