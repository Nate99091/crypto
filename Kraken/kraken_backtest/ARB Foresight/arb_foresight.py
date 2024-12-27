import aiohttp
import asyncio
import logging
import json
import os
import pandas as pd
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load configuration from config.json
config_path = "/Users/nathanhart/Desktop/Archive/Crypto/Kraken/kraken_backtest/ARB Foresight/config.json"
try:
    with open(config_path, "r") as config_file:
        config = json.load(config_file)
except Exception as e:
    logger.error(f"Failed to load configuration: {e}")
    exit(1)

# Load trade fees from CSV
trade_fees_path = os.path.expanduser("~/Desktop/Archive/Crypto/Kraken/kraken_backtest/trade_fees.csv")
try:
    trade_fees = pd.read_csv(trade_fees_path)
    trade_fees.rename(columns={"Pair": "pair", "TakerFee%": "taker_fee", "MakerFee%": "maker_fee"}, inplace=True)
    trade_fees["taker_fee"] /= 100  # Convert percentage to decimal
    trade_fees["maker_fee"] /= 100  # Convert percentage to decimal
    trade_fees.set_index("pair", inplace=True)
    logger.info("Trade fees DataFrame processed successfully.")
except Exception as e:
    logger.error(f"Failed to load or process trade fees file: {e}")
    exit(1)

# Debugging output for confirmation
print(trade_fees.head())

class KrakenAPI:
    def __init__(self):
        self.base_url = "https://api.kraken.com"
        self.batch_size = config.get("batch_size", 10)

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

def process_ohlc_data(ohlc_data, pair_name):
    """
    Process OHLC data into a pandas DataFrame.
    """
    if len(ohlc_data) < 2:
        logger.warning(f"Insufficient data for {pair_name}, skipping pair.")
        return None

    df = pd.DataFrame(ohlc_data, columns=[
        "time", "open", "high", "low", "close", "vwap", "volume", "count"
    ])
    df["time"] = pd.to_datetime(df["time"], unit="s")
    df["close"] = df["close"].astype(float)
    df["pair"] = pair_name  # Add pair name column
    return df

def backtest_and_compare(df_a, df_b, entry_threshold=None):
    """
    Compare two trading pairs based on discrepancy and incorporate trade fees.
    """
    df = pd.merge(df_a, df_b, on="time", suffixes=("_a", "_b"))
    df["discrepancy"] = abs(df["close_a"] - df["close_b"])

    # Adjust discrepancies for trade fees
    fee_a = trade_fees.at[df_a["pair"].iloc[0], "taker_fee"] if df_a["pair"].iloc[0] in trade_fees.index else 0.0026
    fee_b = trade_fees.at[df_b["pair"].iloc[0], "taker_fee"] if df_b["pair"].iloc[0] in trade_fees.index else 0.0026
    df["adjusted_discrepancy"] = df["discrepancy"] - (fee_a + fee_b)

    # Calculate dynamic entry threshold if not provided
    if entry_threshold is None:
        entry_threshold = df["adjusted_discrepancy"].mean() + 2 * df["adjusted_discrepancy"].std()

    results = []
    for _, row in df.iterrows():
        adjusted_discrepancy = row["adjusted_discrepancy"]
        if adjusted_discrepancy > entry_threshold:
            results.append({
                "time": row["time"],
                "pair_a": df_a["pair"].iloc[0],
                "pair_b": df_b["pair"].iloc[0],
                "adjusted_discrepancy": adjusted_discrepancy,
                "raw_discrepancy": row["discrepancy"],
                "fee_a": fee_a,
                "fee_b": fee_b
            })

    return pd.DataFrame(results)

async def main():
    kraken_api = KrakenAPI()

    # Fetch all trading pairs
    logger.info("Fetching all trading pairs...")
    all_pairs = await kraken_api.fetch_asset_pairs()
    if not all_pairs:
        logger.error("No trading pairs found. Exiting.")
        return
    
    # Print available pairs and prompt user for input
    logger.info(f"{len(all_pairs)} trading pairs found.")
    pair_count = int(input("How many trading pairs would you like to analyze? "))
    pair_count = min(pair_count, len(all_pairs))  # Ensure valid count
    selected_pairs = all_pairs[:pair_count]

    # Fetch OHLC data for selected pairs using parallelized batching
    logger.info(f"Fetching OHLC data for {pair_count} pairs...")
    ohlc_data = await kraken_api.fetch_all_ohlc_parallel(selected_pairs, interval=config.get("interval", 15))

    # Process OHLC data into DataFrames
    logger.info("Processing OHLC data...")
    ohlc_frames = {
        pair: process_ohlc_data(data, pair)
        for pair, data in ohlc_data.items()
        if process_ohlc_data(data, pair) is not None and not process_ohlc_data(data, pair).empty
    }

    # Analyze discrepancies and compare
    logger.info("Comparing pairs...")
    all_results = []
    processed_pairs = set()  # Caching mechanism
    for i, pair_a in enumerate(selected_pairs[:-1]):
        for pair_b in selected_pairs[i+1:]:
            if (pair_a, pair_b) in processed_pairs or (pair_b, pair_a) in processed_pairs:
                continue
            processed_pairs.add((pair_a, pair_b))
            if pair_a in ohlc_frames and pair_b in ohlc_frames:
                logger.info(f"Comparing {pair_a} vs {pair_b}...")
                df_a = ohlc_frames[pair_a]
                df_b = ohlc_frames[pair_b]
                results = backtest_and_compare(df_a, df_b)

                if not results.empty:
                    logger.info(f"Found discrepancies for {pair_a} vs {pair_b}.")
                    all_results.append(results)

    # Save combined results
    if all_results:
        combined_results = pd.concat(all_results, ignore_index=True)
        combined_results.to_csv("pair_comparison_results.csv", index=False)
        logger.info("Comparison complete. Results saved to 'pair_comparison_results.csv'.")
    else:
        logger.info("No significant discrepancies found.")

if __name__ == "__main__":
    asyncio.run(main())
