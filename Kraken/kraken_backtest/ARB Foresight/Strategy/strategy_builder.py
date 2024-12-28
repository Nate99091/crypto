import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Paths
DATA_FILE = "/Users/nathanhart/Desktop/Archive/Crypto/Kraken/kraken_backtest/ARB Foresight/optimized_pair_comparison_results.csv"
TRADE_FEES_FILE = "/Users/nathanhart/Desktop/Archive/Crypto/Kraken/kraken_backtest/trade_fees.csv"
STRATEGY_RESULTS_FILE = "strategy_results.csv"
STRATEGY_CHART_FILE = "strategy_visualization.png"
CUMULATIVE_PROFIT_CHART = "cumulative_profit.png"

def load_data(file_path):
    """Load historical data for analysis."""
    try:
        df = pd.read_csv(file_path, parse_dates=["time"])
        logger.info(f"Loaded {len(df)} rows from {file_path}.")
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return None

def load_trade_fees(file_path):
    """Load trade fees from a CSV file."""
    try:
        fees = pd.read_csv(file_path)
        fees.rename(columns={"Pair": "pair", "TakerFee%": "taker_fee"}, inplace=True)
        fees["taker_fee"] /= 100  # Convert percentage to decimal
        fees.set_index("pair", inplace=True)
        logger.info(f"Trade fees loaded successfully from {file_path}.")
        return fees
    except Exception as e:
        logger.error(f"Error loading trade fees: {e}")
        return None

def validate_data(df):
    """Validate and clean data."""
    df = df[(df["adjusted_discrepancy"] > 0) & (df["volume_a"] > 0)]
    if df.empty:
        logger.error("No valid data found after cleaning. Exiting.")
        exit(1)
    return df

def calculate_profit(trades, fees):
    """Calculate profit based on discrepancies and fees."""
    try:
        # Ensure pairs exist in the fee table
        trades["profit"] = trades.apply(
            lambda row: (
                row["adjusted_discrepancy"] - fees.at[row["pair_a"], "taker_fee"]
            ) * row["volume_a"]
            if row["pair_a"] in fees.index
            else 0,
            axis=1,
        )
        trades = trades[trades["profit"] > 0]  # Remove unprofitable trades
        return trades
    except Exception as e:
        logger.error(f"Error calculating profit: {e}")
        return trades

def formulate_strategy(df, threshold_factor=2):
    """Formulate trading strategy based on thresholds."""
    mean_discrepancy = df["adjusted_discrepancy"].mean()
    std_discrepancy = df["adjusted_discrepancy"].std()
    threshold = mean_discrepancy + threshold_factor * std_discrepancy

    logger.info(f"Using trading threshold: {threshold:.2f}")
    strategy_trades = df[df["adjusted_discrepancy"] > threshold]
    return strategy_trades

def evaluate_strategy(trades):
    """Evaluate strategy with Sharpe Ratio."""
    returns = trades["profit"]
    mean_return = returns.mean()
    std_return = returns.std()

    sharpe_ratio = mean_return / std_return if std_return != 0 else 0
    logger.info(f"Sharpe Ratio: {sharpe_ratio:.2f}")
    return sharpe_ratio

def visualize_strategy(summary, trades, output_file):
    """Visualize top arbitrage opportunities."""
    try:
        plt.figure(figsize=(12, 6))

        # Top opportunities bar chart
        top_pairs = summary.head(10)
        plt.bar(top_pairs["pair_a"] + " - " + top_pairs["pair_b"], top_pairs["mean"], color="skyblue")
        plt.xticks(rotation=45, ha="right")
        plt.xlabel("Trading Pairs")
        plt.ylabel("Mean Adjusted Discrepancy")
        plt.title("Top Arbitrage Opportunities")
        plt.tight_layout()
        plt.savefig(output_file)
        logger.info(f"Strategy visualization saved to {output_file}")
        plt.close()

        # Cumulative profit plot
        trades = trades.sort_values("time")
        trades["cumulative_profit"] = trades["profit"].cumsum()
        plt.plot(trades["time"], trades["cumulative_profit"], label="Cumulative Profit")
        plt.xlabel("Time")
        plt.ylabel("Cumulative Profit")
        plt.title("Cumulative Profit Over Time")
        plt.legend()
        plt.tight_layout()
        plt.savefig(CUMULATIVE_PROFIT_CHART)
        logger.info(f"Cumulative profit chart saved to {CUMULATIVE_PROFIT_CHART}")
        plt.close()

    except Exception as e:
        logger.error(f"Error creating visualizations: {e}")

def save_strategy_results(trades, file_path):
    """Save results to a CSV file."""
    try:
        trades.to_csv(file_path, index=False)
        logger.info(f"Strategy results saved to {file_path}")
    except Exception as e:
        logger.error(f"Error saving results: {e}")

def main():
    """Main analysis function."""
    logger.info("Starting strategy analysis...")

    # Load data
    df = load_data(DATA_FILE)
    if df is None or df.empty:
        logger.error("No data available for analysis. Exiting.")
        return

    # Load trade fees
    trade_fees = load_trade_fees(TRADE_FEES_FILE)
    if trade_fees is None or trade_fees.empty:
        logger.error("No trade fee data available. Exiting.")
        return

    # Validate data
    df = validate_data(df)

    # Formulate strategy
    trades = formulate_strategy(df)

    # Calculate profits
    trades = calculate_profit(trades, trade_fees)

    # Evaluate strategy
    sharpe_ratio = evaluate_strategy(trades)

    # Analyze top opportunities
    summary = df.groupby(["pair_a", "pair_b"])["adjusted_discrepancy"].agg(["mean", "std"]).reset_index()

    # Visualize strategy
    visualize_strategy(summary, trades, STRATEGY_CHART_FILE)

    # Save results
    save_strategy_results(trades, STRATEGY_RESULTS_FILE)

    logger.info("Strategy analysis complete.")

if __name__ == "__main__":
    main()
