import os
import json
from datetime import datetime, timedelta

CACHE_DIR = "results/cache/"

def load_cached_data(pair, interval):
    path = f"{CACHE_DIR}{pair}_{interval}.json"
    if os.path.exists(path):
        with open(path, "r") as file:
            cache = json.load(file)
            last_fetched = datetime.fromisoformat(cache["last_fetched"])
            if datetime.now() - last_fetched < timedelta(minutes=15):
                return cache["data"]
    return None

def save_to_cache(pair, interval, data):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = f"{CACHE_DIR}{pair}_{interval}.json"
    cache = {"last_fetched": datetime.now().isoformat(), "data": data}
    with open(path, "w") as file:
        json.dump(cache, file)
