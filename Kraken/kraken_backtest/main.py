
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
