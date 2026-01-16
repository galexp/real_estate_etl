import os

# ------------------------
# Base Directory
# ------------------------
# This points to the root of your project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

API_KEY = "6c956b96d0bd4e028a3c1712655a58ff"
BASE_URL = "https://api.rentcast.io/v1/properties"

RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")

# Directory for processed / cleaned data
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, "data", "processed")

# Make sure the directories exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

CACHE_EXPIRY_HOURS = 8760   # refresh once per year (24 * 365)
RAW_DATA_PATH = "data/raw"
