import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import numpy as np
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Paths to files
DB_FILE = "/Users/nathanhart/Desktop/Archive/Crypto/Kraken/kraken_backtest/ARB Foresight/arbitrage_data.db"
CHART_FILE = "/Users/nathanhart/Desktop/Archive/Crypto/Kraken/kraken_backtest/ARB Foresight/arbitrage_opportunities_backtest.png"
OUTLIER_FILE = "/Users/nathanhart/Desktop/Archive/Crypto/Kraken/kraken_backtest/ARB Foresight/outliers.csv"
TRADE_FEES_FILE = "/Users/nathanhart/Desktop/Archive/Crypto/Kraken/kraken_backtest/trade_fees.csv"

# Load data
def load_existing_data(db_file):
    """Load historical data from SQLite database."""
    try:
        conn = sqlite3.connect(db_file)
        df = pd.read_sql("SELECT * FROM arbitrage_opportunities", conn, parse_dates=['time'])
        conn.close()
        logger.info(f"Loaded {len(df)} rows of historical data from {db_file}.")
        return df
    except Exception as e:
        logger.warning(f"No historical data found: {e}")
        return pd.DataFrame()

def load_trade_fees(trade_fees_file):
    """Load trade fees from CSV file."""
    try:
        fees_df = pd.read_csv(trade_fees_file)
        logger.info("Trade fees loaded successfully.")
        return fees_df
    except Exception as e:
        logger.error(f"Failed to load trade fees: {e}")
        return pd.DataFrame()

# Real profit calculation
def calculate_profit(df, fees_df):
    """Calculate real profit or fallback to adjusted discrepancy as a proxy."""
    if 'volume_a' in df.columns and 'fee_a' in fees_df.columns:
        logger.info("Calculating real profit based on trading volumes and fees...")
        # Merge trade fees with the data
        df = df.merge(fees_df, on="pair", how="left")
        df['fee_a'].fillna(0, inplace=True)
        df['fee_b'].fillna(0, inplace=True)

        # Calculate profit after trade fees
        df['profit'] = (
            df['adjusted_discrepancy'] * df['volume_a'] - 
            (df['fee_a'] + df['fee_b']) * df['volume_a']
        )
    else:
        logger.warning("Volume and fee data unavailable. Using adjusted discrepancy as a proxy.")
        df['profit'] = df['adjusted_discrepancy']
    return df

# Backtesting with weights
def backtest_trades_with_weights(df, threshold, mean, std_dev):
    """Prioritize trades closer to the mean + 3*std_dev threshold."""
    threshold_value = mean + threshold * std_dev
    df['weight'] = np.exp(-abs(df['adjusted_discrepancy'] - threshold_value) / std_dev)  # Higher weight closer to the threshold
    filtered_trades = df[df['adjusted_discrepancy'] > threshold_value]
    
    total_profit = (filtered_trades['profit'] * filtered_trades['weight']).sum()
    logger.info(f"Weighted Profit: {total_profit:.2f} over {len(filtered_trades)} trades.")
    return total_profit, filtered_trades

# Sharpe Ratio and Sortino Ratio
def evaluate_risk_metrics(trade_summary):
    """Calculate Sharpe and Sortino Ratios."""
    if trade_summary.empty:
        logger.warning("No trades available for risk-adjusted return calculation.")
        return 0, 0

    returns = trade_summary['profit']
    mean_return = returns.mean()
    std_dev_return = returns.std()
    downside_dev = returns[returns < 0].std()

    sharpe_ratio = mean_return / std_dev_return if std_dev_return != 0 else 0
    sortino_ratio = mean_return / downside_dev if downside_dev != 0 else 0

    logger.info(f"Sharpe Ratio: {sharpe_ratio:.2f}")
    logger.info(f"Sortino Ratio: {sortino_ratio:.2f}")
    return sharpe_ratio, sortino_ratio

# Outlier analysis
def analyze_outliers(df, mean, std_dev):
    """Deep dive into outliers based on mean + 4*std_dev."""
    outlier_threshold = mean + 4 * std_dev
    outliers = df[df['adjusted_discrepancy'] > outlier_threshold]
    logger.info(f"Identified {len(outliers)} outliers exceeding threshold: {outlier_threshold:.2f}")
    outliers.to_csv(OUTLIER_FILE, index=False)
    logger.info(f"Outliers saved to: {OUTLIER_FILE}")
    return outliers

# Plot discrepancy distribution
def plot_discrepancy_distribution(df, mean, std_dev, chart_file):
    """Plot histograms of discrepancies and thresholds."""
    try:
        plt.figure(figsize=(10, 6))
        plt.hist(df['adjusted_discrepancy'], bins=50, alpha=0.6, label="Adjusted Discrepancy")
        plt.axvline(mean, color='r', linestyle='dashed', linewidth=1, label="Mean")
        plt.axvline(mean + 2 * std_dev, color='g', linestyle='dotted', linewidth=1, label="Mean + 2*Std Dev")
        plt.axvline(mean + 3 * std_dev, color='b', linestyle='dotted', linewidth=1, label="Mean + 3*Std Dev")
        plt.axvline(mean + 4 * std_dev, color='orange', linestyle='dotted', linewidth=1, label="Mean + 4*Std Dev")
        plt.title("Discrepancy Distribution")
        plt.xlabel("Adjusted Discrepancy")
        plt.ylabel("Frequency")
        plt.legend()
        plt.grid(alpha=0.3)
        plt.savefig(chart_file.replace(".png", "_distribution.png"))
        plt.close()
        logger.info(f"Discrepancy distribution chart saved to: {chart_file.replace('.png', '_distribution.png')}")
    except Exception as e:
        logger.error(f"Error creating discrepancy distribution chart: {e}")

# Main function
def main():
    logger.info("Starting enhanced analysis...")

    # Load data
    df = load_existing_data(DB_FILE)
    if df.empty:
        logger.error("No data available for analysis. Exiting.")
        return

    fees_df = load_trade_fees(TRADE_FEES_FILE)
    if fees_df.empty:
        logger.error("Trade fees data unavailable. Exiting.")
        return

    # Calculate mean and std dev
    mean = df['adjusted_discrepancy'].mean()
    std_dev = df['adjusted_discrepancy'].std()

    # Calculate profit
    df = calculate_profit(df, fees_df)

    # Backtest with weighted thresholds
    logger.info("Backtesting with weighted thresholds...")
    total_profit, filtered_trades = backtest_trades_with_weights(df, 3, mean, std_dev)

    # Risk metrics
    logger.info("Evaluating risk-adjusted returns...")
    sharpe, sortino = evaluate_risk_metrics(filtered_trades)

    # Analyze outliers
    logger.info("Analyzing outliers...")
    analyze_outliers(df, mean, std_dev)

    # Plot distribution
    logger.info("Plotting discrepancy distribution...")
    plot_discrepancy_distribution(df, mean, std_dev, CHART_FILE)

    logger.info("Enhanced analysis complete.")

if __name__ == "__main__":
    main()
