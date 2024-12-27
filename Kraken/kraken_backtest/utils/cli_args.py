import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description="Kraken OHLC Backtesting")
    parser.add_argument("--asset_pairs", nargs="+", help="List of asset pairs to backtest")
    parser.add_argument("--interval", type=int, help="OHLC interval (minutes)")
    parser.add_argument("--output_path", type=str, help="Path to save results")
    return parser.parse_args()
