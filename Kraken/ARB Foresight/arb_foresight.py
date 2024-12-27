import aiohttp
import asyncio
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from decimal import Decimal
import math
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KrakenAPI:
    def __init__(self):
        self.base_url = "https://api.kraken.com"
        self.batch_size = 5  # Number of pairs per batch

    async def fetch_asset_pairs(self):
        """
        Fetch all trading pairs available on Kraken.
        """
        endpoint = "/0/public/AssetPairs"
        url = f"{self.base_url}{endpoint}"

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                data = await response.json()

        if "result" in data:
            return list(data["result"].keys())
        else:
            raise Exception(f"Error fetching trading pairs: {data.get('error')}")

    async def fetch_ohlc_batch(self, pairs, interval=15, since=None):
        """
        Fetch OHLC data for a batch of trading pairs.
        """
        results = {}
        async with aiohttp.ClientSession() as session:
            for pair in pairs:
                endpoint = f"/0/public/OHLC"
                url = f"{self.base_url}{endpoint}?pair={pair}&interval={interval}"
                if since:
                    url += f"&since={since}"

                async with session.get(url) as response:
                    data = await response.json()
                    if "result" in data:
                        key = list(data["result"].keys())[0]
                        results[pair] = data["result"][key]
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

    logger.info(f"Entry threshold: {entry_threshold}, Exit threshold: {exit_threshold}")

    results = []
    for _, row in df.iterrows():
        discrepancy = row["discrepancy"]
        price_a = row["close_a"]
        price_b = row["close_b"]

        if discrepancy > entry_threshold * price_a:
            exit_price = price_b if price_b < price_a else price_a
            required_usd = calculate_required_usd(
                profit_margin=1.0,  # Example profit margin in USD
                entry_price=price_a,
                exit_price=exit_price
            )
            if required_usd:
                results.append({
                    "time": row["time"],
                    "entry_price": price_a,
                    "exit_price": exit_price,
                    "required_usd": required_usd
                })

    return pd.DataFrame(results)

def plot_discrepancies(df, entry_threshold, exit_threshold, pair_a, pair_b):
    """
    Plot discrepancies and thresholds for analysis.
    """
    plt.figure(figsize=(10, 6))
    plt.plot(df["time"], df["discrepancy"], label="Discrepancy")
    plt.axhline(y=entry_threshold, color="r", linestyle="--", label="Entry Threshold")
    plt.axhline(y=exit_threshold, color="g", linestyle="--", label="Exit Threshold")
    plt.title(f"Discrepancy Analysis for {pair_a} vs {pair_b}")
    plt.xlabel("Time")
    plt.ylabel("Discrepancy")
    plt.legend()
    plt.show()

async def main():
    kraken_api = KrakenAPI()

    # Fetch all trading pairs
    logger.info("Fetching all trading pairs...")
    all_pairs = await kraken_api.fetch_asset_pairs()
    logger.info(f"Found {len(all_pairs)} pairs. Fetching OHLC data in batches...")

    # Fetch OHLC data for all pairs using parallelized batching
    ohlc_data = await kraken_api.fetch_all_ohlc_parallel(all_pairs, interval=15)

    # Process OHLC data into DataFrames
    logger.info("Processing OHLC data...")
    ohlc_frames = {pair: process_ohlc_data(data) for pair, data in ohlc_data.items()}

    # Analyze discrepancies and backtest
    logger.info("Running backtests...")
    for i, pair_a in enumerate(all_pairs[:-1]):
        for pair_b in all_pairs[i+1:]:
            if pair_a in ohlc_frames and pair_b in ohlc_frames:
                logger.info(f"Backtesting {pair_a} vs {pair_b}...")
                df_a = ohlc_frames[pair_a]
                df_b = ohlc_frames[pair_b]
                results = backtest_and_validate(df_a, df_b)

                if not results.empty:
                    logger.info(f"Results for {pair_a} vs {pair_b}:\n{results}")
                    # Save results for further analysis
                    results.to_csv(f"results_{pair_a}_{pair_b}.csv", index=False)

                    # Plot discrepancies
                    plot_discrepancies(df_a, entry_threshold=None, exit_threshold=None, pair_a=pair_a, pair_b=pair_b)

if __name__ == "__main__":
    asyncio.run(main())
