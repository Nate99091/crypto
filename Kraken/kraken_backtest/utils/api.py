import aiohttp
import logging

API_URL = "https://api.kraken.com/0/public/"

async def get_trading_pairs():
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{API_URL}AssetPairs") as response:
            if response.status == 200:
                data = await response.json()
                return list(data["result"].keys())
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
