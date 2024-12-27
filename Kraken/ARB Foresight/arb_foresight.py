import aiohttp
import asyncio
import pandas as pd
import logging
import json
import os
from datetime import datetime
from decimal import Decimal

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load configuration from config.json
with open("config.json", "r") as config_file:
    config = json.load(config_file)

class KrakenAPI:
    def __init__(self):
        self.base_url = "https://api.kraken.com"
        self.batch_size = config["batch_size"]

    async def fetch_asset_pairs(self):
        """
        Fetch all trading pairs available on Kraken.
        """
        endpoint = "/0/public/AssetPairs"
        url = f"{self.base_url}{endpoint}"

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    data = await response.json()
                    if "result" in data:
                        return list(data["result"].keys())
            except Exception as e:
                logger.error(f"Error fetching trading pairs: {e}")
                return []
        return []

    async def fetch_ohlc_batch(self, pairs, interval=15, since=None):
        """
        Fetch OHLC data for a batch of trading pairs.
        """
        results = {}
        async with aiohttp.ClientSession() as session:
            for pair in pairs:
                try:
                    endpoint = f"/0/public/OHLC"
                    url = f"{self.base_url}{endpoint}?pair={pair}&interval={interval}"
                    if since:
                        url += f"&since={since}"

                    async with session.get(url) as response:
                        data = await response.json()
                        if "result" in data:
                            key = list(data["result"].keys())[0]
                            results[pair] = data["result"][key]
                except Exception as e:
                    logger.error(f"Error fetching OHLC for {pair}: {e}")
        return results

    async def fetch_all_ohlc_parallel(self, all_pairs, interval=15, since=None):
        """
        Fetch OHLC data for all pairs using parallelized batching.
        """
        tasks = [
            self.fetch_ohlc_batch(all_pairs[i:i + self.batch_size], interval, since)
            for i in range(0, len(all_pairs), self.batch_size)
        ]
        results = await asyncio.gather(*tasks)
        ohlc_data = {}
        for batch in results:
            ohlc_data.update(batch)
        return ohlc_data

def process_ohlc_data(ohlc_data):
    """
    Process OHLC data into a pandas DataFrame.
    """
    if len(ohlc_data) < 2:
        logger.warning("Insufficient data, skipping pair.")
        return None

    df = pd.DataFrame(ohlc_data, columns=[
        "time", "open", "high", "low", "close", "vwap", "volume", "count"
    ])
    df["time"] = pd.to_datetime(df["time"], unit="s")
    df["close"] = df["close"].astype(float)
    return df

def calculate_required_usd(profit_margin, entry_price, exit_price, fees=0.0026):
    """
    Calculate the required starting USD balance for a profitable trade.
    """
    total_fees = fees * 2  # Entry and exit fees
    net_gain = (exit_price - entry_price) - (entry_price * total_fees)
    if net_gain <= 0:
        return None  # Not profitable
    return profit_margin / net_gain

def backtest_and_validate(df_a, df_b, entry_threshold=None, exit_threshold=None):
    """
    Backtest trading strategy and validate thresholds.
    """
    df = pd.merge(df_a, df_b, on="time", suffixes=("_a", "_b"))
    df["discrepancy"] = abs(df["close_a"] - df["close_b"])
    
    # Calculate dynamic thresholds if not provided
    if entry_threshold is None:
        entry_threshold = df["discrepancy"].mean() + 2 * df["discrepancy"].std()
    if exit_threshold is None:
        exit_threshold = df["discrepancy"].mean()

    results = []
    for _, row in df.iterrows():
        discrepancy = row["discrepancy"]
        price_a = row["close_a"]
        price_b = row["close_b"]

        if discrepancy > entry_threshold * price_a:
            exit_price = price_b if price_b < price_a else price_a
            required_usd = calculate_required_usd(
                profit_margin=config["profit_margin"],
                entry_price=price_a,
                exit_price=exit_price
            )
            if required_usd:
                results.append({
                    "time": row["time"],
                    "pair_a": df_a["pair"][0],  # Include trading pair names
                    "pair_b": df_b["pair"][0],
                    "entry_price": price_a,
                    "exit_price": exit_price,
                    "required_usd": required_usd,
                    "discrepancy": discrepancy
                })

    return pd.DataFrame(results)

async def main():
    kraken_api = KrakenAPI()

    # Fetch all trading pairs
    logger.info("Fetching all trading pairs...")
    all_pairs = await kraken_api.fetch_asset_pairs()
    
    # Limit to the first 50 trading pairs
    all_pairs = all_pairs[:50]
    logger.info(f"Selected first 50 pairs. Fetching OHLC data...")

    # Fetch OHLC data for selected pairs using parallelized batching
    ohlc_data = await kraken_api.fetch_all_ohlc_parallel(all_pairs, interval=config["interval"])

    # Process OHLC data into DataFrames
    logger.info("Processing OHLC data...")
    ohlc_frames = {
        pair: process_ohlc_data(data)
        for pair, data in ohlc_data.items()
        if process_ohlc_data(data) is not None and not process_ohlc_data(data).empty
    }

    # Analyze discrepancies and backtest
    logger.info("Running backtests...")
    all_results = []
    for i, pair_a in enumerate(all_pairs[:-1]):
        for pair_b in all_pairs[i+1:]:
            if pair_a in ohlc_frames and pair_b in ohlc_frames:
                logger.info(f"Backtesting {pair_a} vs {pair_b}...")
                df_a = ohlc_frames[pair_a]
                df_b = ohlc_frames[pair_b]
                results = backtest_and_validate(df_a, df_b)

                if not results.empty:
                    logger.info(f"Found profitable opportunities for {pair_a} vs {pair_b}.")
                    all_results.append(results)

    # Save combined results
    combined_results = pd.concat(all_results, ignore_index=True)
    combined_results.to_csv("combined_results.csv", index=False)
    logger.info("Backtesting complete. Results saved to 'combined_results.csv'.")

if __name__ == "__main__":
    asyncio.run(main())
