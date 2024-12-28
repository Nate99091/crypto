import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import numpy as np
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Paths to files
DIRECTORY = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(DIRECTORY, "arbitrage_data.db")
CHART_FILE = os.path.join(DIRECTORY, "arbitrage_opportunities_backtest.png")
OUTLIER_FILE = os.path.join(DIRECTORY, "outliers.csv")
THRESHOLD_OPT_FILE = os.path.join(DIRECTORY, "threshold_optimization.png")
TRADE_FEES_FILE = os.path.join(DIRECTORY, "trade_fees.csv")

# Load historical data
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

# Load trade fees
def load_trade_fees(file_path):
    """Load trade fees from CSV."""
    try:
        fees_df = pd.read_csv(file_path)
        fees_df['TakerFee'] = fees_df['TakerFee%'] / 100
        fees_df['MakerFee'] = fees_df['MakerFee%'] / 100
        logger.info(f"Loaded {len(fees_df)} trade fee records from {file_path}.")
        return fees_df[['Pair', 'TakerFee', 'MakerFee']]
    except Exception as e:
        logger.error(f"Error loading trade fees: {e}")
        return pd.DataFrame()

# Validate and normalize data
def validate_and_normalize_data(df):
    """Validate and normalize input data."""
    max_discrepancy = 50
    max_volume = 100000
    df['adjusted_discrepancy'] = np.clip(df['adjusted_discrepancy'], -max_discrepancy, max_discrepancy)
    df['volume_a'] = np.clip(df['volume_a'], 0, max_volume)
    df['TakerFee'] = np.clip(df['TakerFee'], 0.001, 0.005)
    df['MakerFee'] = np.clip(df['MakerFee'], 0.0005, 0.0025)
    return df

# Merge trade fees
def merge_trade_fees(df, fees_df):
    """Merge trade fees with the main dataset."""
    try:
        logger.info(f"Columns in historical data: {df.columns}")
        logger.info(f"Columns in trade fees data: {fees_df.columns}")
        merge_column = 'pair_a' if 'pair_a' in df.columns else 'pair_b'
        fees_df = fees_df.rename(columns={'Pair': 'pair'})
        merged_df = df.merge(fees_df, left_on=merge_column, right_on='pair', how='left')
        logger.info("Trade fees merged successfully.")
        return merged_df
    except Exception as e:
        logger.error(f"Error merging trade fees: {e}")
        return df

# Calculate profit
def calculate_profit(df):
    """Calculate real profit using volume, adjusted discrepancy, and fees."""
    if 'volume_a' in df.columns and 'TakerFee' in df.columns:
        logger.info("Calculating real profit based on trading volumes and fees...")
        df['profit'] = (
            df['adjusted_discrepancy'] * df['volume_a'] -
            df['volume_a'] * (df['TakerFee'] + df['MakerFee'])
        )
    else:
        logger.warning("Volume and/or fee data unavailable. Using adjusted discrepancy as a proxy.")
        df['profit'] = df['adjusted_discrepancy']
    return df

# Backtest trades with weights
def backtest_trades_with_weights(df, threshold, mean, std_dev):
    """Backtest trades using weighted thresholds."""
    threshold_value = mean + threshold * std_dev
    df['weight'] = np.exp(-np.clip(abs(df['adjusted_discrepancy'] - threshold_value) / std_dev, 0, 10))
    filtered_trades = df[df['adjusted_discrepancy'] > threshold_value]
    total_profit = (filtered_trades['profit'] * filtered_trades['weight']).sum()
    logger.info(f"Weighted Profit: {total_profit:.2f} over {len(filtered_trades)} trades.")
    return total_profit, filtered_trades

# Optimize thresholds
def optimize_thresholds(df, mean, std_dev, save_path):
    """Evaluate multiple thresholds and their impact on profit."""
    thresholds = np.arange(1, 5.1, 0.1)
    results = []
    for threshold in thresholds:
        threshold_value = mean + threshold * std_dev
        df['weight'] = np.exp(-np.clip(abs(df['adjusted_discrepancy'] - threshold_value) / std_dev, 0, 10))
        filtered_trades = df[df['adjusted_discrepancy'] > threshold_value]
        total_profit = (filtered_trades['profit'] * filtered_trades['weight']).sum()
        results.append({"threshold": threshold, "profit": total_profit, "num_trades": len(filtered_trades)})

    results_df = pd.DataFrame(results)

    # Plot threshold optimization
    plt.figure(figsize=(12, 6))
    plt.plot(results_df['threshold'], results_df['profit'], label="Profit", marker='o')
    plt.xlabel("Threshold (Standard Deviations)")
    plt.ylabel("Profit")
    plt.title("Threshold Optimization")
    plt.grid(alpha=0.3)
    plt.legend()
    plt.savefig(save_path)
    logger.info(f"Threshold optimization chart saved to: {save_path}")
    plt.close()

    optimal_row = results_df.loc[results_df['profit'].idxmax()]
    logger.info(f"Optimal Threshold: {optimal_row['threshold']} with Profit: {optimal_row['profit']:.2f}")
    return optimal_row['threshold'], results_df

# Analyze outliers
def analyze_outliers(df, method='IQR', multiplier=1.5, mean=None, std_dev=None):
    """Identify and save outliers based on the selected method."""
    if method == 'IQR':
        Q1 = df['adjusted_discrepancy'].quantile(0.25)
        Q3 = df['adjusted_discrepancy'].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - multiplier * IQR
        upper_bound = Q3 + multiplier * IQR
    elif method == 'std_dev' and mean is not None and std_dev is not None:
        lower_bound = mean - multiplier * std_dev
        upper_bound = mean + multiplier * std_dev
    else:
        raise ValueError("Invalid method or missing required parameters.")

    # Identify outliers
    outliers = df[(df['adjusted_discrepancy'] < lower_bound) | (df['adjusted_discrepancy'] > upper_bound)]
    logger.info(f"Identified {len(outliers)} outliers using {method} method with multiplier {multiplier}.")
    outliers.to_csv(OUTLIER_FILE, index=False)
    logger.info(f"Outliers saved to: {OUTLIER_FILE}")

    return outliers, lower_bound, upper_bound

# Plot discrepancy distribution
def plot_discrepancy_distribution(df, lower_bound, upper_bound):
    """Plot histograms of discrepancies with dynamic bounds."""
    plt.figure(figsize=(10, 6))
    plt.hist(df['adjusted_discrepancy'], bins=50, alpha=0.6, label="Adjusted Discrepancy")
    plt.axvline(lower_bound, color='r', linestyle='dashed', linewidth=1, label="Lower Bound")
    plt.axvline(upper_bound, color='g', linestyle='dashed', linewidth=1, label="Upper Bound")
    plt.title("Discrepancy Distribution with Outlier Bounds")
    plt.xlabel("Adjusted Discrepancy")
    plt.ylabel("Frequency")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.savefig(CHART_FILE)
    logger.info(f"Discrepancy distribution chart saved to: {CHART_FILE}")
    plt.close()

def calculate_outlier_percentiles(df):
    """Find bounds for 1% and 5% of total outliers."""
    # Calculate 1% bounds
    lower_bound_1 = df['adjusted_discrepancy'].quantile(0.01)  # 1st percentile
    upper_bound_1 = df['adjusted_discrepancy'].quantile(0.99)  # 99th percentile

    # Calculate 5% bounds
    lower_bound_5 = df['adjusted_discrepancy'].quantile(0.05)  # 5th percentile
    upper_bound_5 = df['adjusted_discrepancy'].quantile(0.95)  # 95th percentile

    logger.info(f"1% Outlier Bounds: Lower = {lower_bound_1}, Upper = {upper_bound_1}")
    logger.info(f"5% Outlier Bounds: Lower = {lower_bound_5}, Upper = {upper_bound_5}")

    return (lower_bound_1, upper_bound_1), (lower_bound_5, upper_bound_5)

def main():
    logger.info("Starting enhanced analysis...")

    # Load data
    df = load_existing_data(DB_FILE)
    if df.empty:
        logger.error("No data available for analysis. Exiting.")
        return

    fees_df = load_trade_fees(TRADE_FEES_FILE)
    if fees_df.empty:
        logger.error("Trade fee data unavailable. Exiting.")
        return

    df = merge_trade_fees(df, fees_df)
    df = validate_and_normalize_data(df)

    mean = df['adjusted_discrepancy'].mean()
    std_dev = df['adjusted_discrepancy'].std()

    df = calculate_profit(df)

    logger.info("Optimizing thresholds...")
    optimal_threshold, results_df = optimize_thresholds(df, mean, std_dev, THRESHOLD_OPT_FILE)

    logger.info(f"Running backtest with optimal threshold: {optimal_threshold}")
    total_profit, filtered_trades = backtest_trades_with_weights(df, optimal_threshold, mean, std_dev)

    # Find outlier bounds based on percentiles
    logger.info("Finding percentile-based outlier bounds...")
    bounds_1_percent, bounds_5_percent = calculate_outlier_percentiles(df)

    # Plot for 1% and 5% bounds
    logger.info("Plotting discrepancy distribution...")
    plot_discrepancy_distribution(df, bounds_1_percent[0], bounds_1_percent[1])  # For 1% bounds
    plot_discrepancy_distribution(df, bounds_5_percent[0], bounds_5_percent[1])  # For 5% bounds

    logger.info("Enhanced analysis complete.")

if __name__ == "__main__":
    main()
