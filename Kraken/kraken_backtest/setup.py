import os
import subprocess
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("setup")

# Define the directory structure and files
project_structure = {
    "": ["main.py", "config.json", "requirements.txt"],
    "utils": ["api.py", "backtest.py", "visualization.py", "cache.py", "logging_setup.py", "cli_args.py"],
    "results": []  # Directory for results, no initial files
}

# File content for each file
file_contents = {
    "main.py": """
import asyncio
import logging
from utils.api import fetch_ohlc_data, get_trading_pairs
from utils.backtest import perform_backtest
from utils.visualization import plot_backtest_results
from utils.cache import load_cached_data, save_to_cache
from utils.cli_args import parse_arguments
from utils.logging_setup import configure_logging
import json

async def main():
    configure_logging()
    logger = logging.getLogger("kraken_backtest")

    args = parse_arguments()
    with open("config.json") as config_file:
        config = json.load(config_file)

    asset_pairs = args.asset_pairs or config["asset_pairs"]
    interval = args.interval or config["interval"]
    output_path = args.output_path or "results/"

    logger.info(f"Starting backtesting for pairs: {asset_pairs} with interval {interval}")

    data = {}
    for pair in asset_pairs:
        cached_data = load_cached_data(pair, interval)
        if cached_data:
            logger.info(f"Using cached data for {pair}")
            data[pair] = cached_data
        else:
            logger.info(f"Fetching OHLC data for {pair}")
            ohlc_data = await fetch_ohlc_data(pair, interval)
            save_to_cache(pair, interval, ohlc_data)
            data[pair] = ohlc_data

    results = perform_backtest(data)
    plot_backtest_results(results, output_path)

    logger.info("Backtesting completed. Results saved to output directory.")

if __name__ == "__main__":
    asyncio.run(main())
""",
    "config.json": """
{
    "asset_pairs": ["BTC/USD", "ETH/USD"],
    "interval": 15
}
""",
    "requirements.txt": """
aiohttp
pandas
matplotlib
""",
    "utils/api.py": """
import aiohttp
import logging
from utils.cache import get_cached_trading_pairs, save_trading_pairs_to_cache

API_URL = "https://api.kraken.com/0/public/"

async def get_trading_pairs():
    cached_pairs = get_cached_trading_pairs()
    if cached_pairs:
        return cached_pairs

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{API_URL}AssetPairs") as response:
            if response.status == 200:
                data = await response.json()
                pairs = list(data["result"].keys())
                save_trading_pairs_to_cache(pairs)
                return pairs
            else:
                logging.error("Failed to fetch trading pairs")
                return []

async def fetch_ohlc_data(pair, interval):
    async with aiohttp.ClientSession() as session:
        url = f"{API_URL}OHLC?pair={pair}&interval={interval}"
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data["result"][pair]
            else:
                logging.error(f"Failed to fetch OHLC data for {pair}")
                return []
""",
    "utils/backtest.py": """
def perform_backtest(data):
    results = {}
    for pair, ohlc in data.items():
        total_profit = 0
        for i in range(1, len(ohlc)):
            if ohlc[i][4] > ohlc[i - 1][4]:
                profit = ohlc[i][4] - ohlc[i - 1][4]
                total_profit += profit
        results[pair] = total_profit
    return results
""",
    "utils/visualization.py": """
import matplotlib.pyplot as plt
import os

def plot_backtest_results(results, output_path):
    os.makedirs(output_path, exist_ok=True)
    for pair, profit in results.items():
        plt.figure()
        plt.bar(pair, profit)
        plt.title(f"Backtest Results for {pair}")
        plt.xlabel("Pair")
        plt.ylabel("Profit")
        plt.savefig(os.path.join(output_path, f"{pair}_results.png"))
        plt.close()
""",
    "utils/cache.py": """
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

def get_cached_trading_pairs():
    path = f"{CACHE_DIR}trading_pairs.json"
    if os.path.exists(path):
        with open(path, "r") as file:
            return json.load(file)
    return None

def save_trading_pairs_to_cache(pairs):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = f"{CACHE_DIR}trading_pairs.json"
    with open(path, "w") as file:
        json.dump(pairs, file)
""",
    "utils/logging_setup.py": """
import logging

def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
""",
    "utils/cli_args.py": """
import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description="Kraken OHLC Backtesting")
    parser.add_argument("--asset_pairs", nargs="+", help="List of asset pairs to backtest")
    parser.add_argument("--interval", type=int, help="OHLC interval (minutes)")
    parser.add_argument("--output_path", type=str, help="Path to save results")
    return parser.parse_args()
"""
}

# Create directories and files
for folder, files in project_structure.items():
    if folder:
        os.makedirs(folder, exist_ok=True)
    for file in files:
        path = os.path.join(folder, file)
        try:
            with open(path, "w") as f:
                f.write(file_contents.get(file, ""))
                logger.info(f"Created file: {path}")
        except Exception as e:
            logger.error(f"Failed to write to {path}: {e}")

# Install dependencies
try:
    subprocess.run(["pip", "install", "-r", "requirements.txt"], check=True)
    logger.info("Dependencies installed successfully.")
except subprocess.CalledProcessError as e:
    logger.error(f"Failed to install dependencies: {e}")

logger.info("Project setup complete.")
