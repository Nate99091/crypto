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
config_path = os.path.expanduser("~/Desktop/Archive/Crypto/Kraken/kraken_backtest/ARB Foresight/config.json")
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

    async def fetch_ohlc_parallel(self, pairs, interval=15, since=None):
        """
        Fetch OHLC data for all pairs using asyncio.
        """
        async def fetch(pair):
            endpoint = f"/0/public/OHLC"
            url = f"{self.base_url}{endpoint}?pair={pair}&interval={interval}"
            if since:
                url += f"&since={since}"

            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(url) as response:
                        data = await response.json()
                        if "result" in data:
                            key = list(data["result"].keys())[0]
                            return pair, data["result"][key]
                except Exception as e:
                    logger.error(f"Error fetching OHLC for {pair}: {e}")
                    return pair, None
            return pair, None

        tasks = [fetch(pair) for pair in pairs]
        results = await asyncio.gather(*tasks)
        return {pair: data for pair, data in results if data}

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

def analyze_results(results, dynamic_threshold=True):
    """
    Analyze results to determine profitability and generate insights.
    """
    if not results:
        logger.info("No results to analyze.")
        return

    combined_results = pd.concat(results, ignore_index=True)
    combined_results["profit"] = combined_results["adjusted_discrepancy"] * combined_results["volume_a"]
    logger.info(f"Total Profit: {combined_results['profit'].sum():.2f}")

    # Generate insights
    if dynamic_threshold:
        threshold = combined_results["adjusted_discrepancy"].mean() + 2 * combined_results["adjusted_discrepancy"].std()
        filtered_results = combined_results[combined_results["adjusted_discrepancy"] > threshold]
        logger.info(f"Filtered Results Exceeding Dynamic Threshold ({threshold:.2f}):")
        logger.info(filtered_results)
    return combined_results

async def main():
    kraken_api = KrakenAPI()

    # Fetch trading pairs
    logger.info("Fetching all trading pairs...")
    pairs = await kraken_api.fetch_asset_pairs()
    if not pairs:
        logger.error("No trading pairs found.")
        return

    # Fetch OHLC data
    logger.info("Fetching OHLC data for selected pairs...")
    ohlc_data = await kraken_api.fetch_ohlc_parallel(pairs[:config["pair_limit"]])

    # Process data
    dataframes = {
        pair: process_ohlc_data(data, pair)
        for pair, data in ohlc_data.items()
        if process_ohlc_data(data, pair) is not None
    }

    # Compare pairs
    logger.info("Comparing pairs for arbitrage opportunities...")
    results = []
    seen = set()
    for i, pair_a in enumerate(dataframes):
        for pair_b in list(dataframes)[i+1:]:
            if (pair_a, pair_b) in seen or (pair_b, pair_a) in seen:
                continue
            seen.add((pair_a, pair_b))
            results.append(compare_pairs((dataframes[pair_a], dataframes[pair_b])))

    # Analyze results
    analyzed_results = analyze_results(results)

    # Save analyzed results
    if analyzed_results is not None:
        analyzed_results.to_csv("analyzed_pair_comparison_results.csv", index=False)
        logger.info("Analysis complete. Results saved to 'analyzed_pair_comparison_results.csv'.")

if __name__ == "__main__":
    asyncio.run(main())
