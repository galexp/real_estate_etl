import requests
import pandas as pd
import os
import time
from config.settings import API_KEY, BASE_URL, RAW_DATA_DIR, CACHE_EXPIRY_HOURS

class RentCastAPIClient:
    """
    Handles communication with the RentCast API
    """

    def __init__(self):
        self.headers = {
            "Accept": "application/json",
            "X-Api-Key": API_KEY
        }
        os.makedirs(RAW_DATA_DIR, exist_ok=True)

    def _is_cache_valid(self, file_path: str) -> bool:
        if not os.path.exists(file_path):
            return False
        age_hours = (time.time() - os.path.getmtime(file_path)) / 3600
        return age_hours < CACHE_EXPIRY_HOURS

    def _fetch(self, endpoint: str, params: dict) -> list:
        try:
            response = requests.get(
                f"{BASE_URL}{endpoint}",
                headers=self.headers,
                params=params,
                timeout=15
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"❌ API request failed: {e}")
            exit()

        data = response.json()

        # Handle API response that may be dict
        if isinstance(data, dict) and 'properties' in data:
            data = data['properties']

        if not data:
            print("⚠️ No data returned from API. Check key, credits, or location.")
            exit()

        return data

    def get_property_listings(self, city: str, state: str, limit: int = 300) -> pd.DataFrame:
        """
        Pull property listings and cache locally
        """
        file_path = f"{RAW_DATA_DIR}/properties_{city.lower()}_{state.lower()}.csv"

        if self._is_cache_valid(file_path):
            print("📂 Using cached property listings")
            return pd.read_csv(file_path)

        print(f"🌐 Calling RentCast API (PAID) with limit={limit}")
        params = {
            "city": city,
            "state": state,
            "status": "Active",
            "limit": limit
        }

        data = self._fetch("/", params)
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)

        return df
