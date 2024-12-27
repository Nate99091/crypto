# Import necessary functionality from the scripts
from kraken_api import KrakenAPI
from get_trading_pairs import get_trading_pairs
from get_ticker_values import get_ticker_values
import asyncio

async def main():
    # Initialize Kraken API
    api_key = '+MhCklHSN/ujlRpZN6tAo47QAJrMraves+q9f8C99Va9mRQXG83QMSrv'
    secret_key = 'wqb3B+4Qkj2fWaT1y5fiXzDlvcIX36kf5YXrdFZGwMkaXlvZ354btegIF/0FxwfFFElYF/+AzbolFvAQJGVM8g=='
    kraken_api = KrakenAPI(api_key, secret_key)

    # Get trading pairs
    trading_pairs = await get_trading_pairs()

    # Retrieve ticker values for trading pairs
    for pair in trading_pairs:
        await get_ticker_values(kraken_api, pair)

if __name__ == "__main__":
    asyncio.run(main())
