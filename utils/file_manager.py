import os
import time

def is_cache_valid(file_path: str, expiry_hours: int) -> bool:
    if not os.path.exists(file_path):
        return False

    file_age_hours = (time.time() - os.path.getmtime(file_path)) / 3600
    return file_age_hours < expiry_hours
