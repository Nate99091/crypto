import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Paths to files
INPUT_FILE = "/Users/nathanhart/Desktop/Archive/Crypto/Kraken/kraken_backtest/ARB Foresight/optimized_pair_comparison_results.csv"
DB_FILE = "/Users/nathanhart/Desktop/Archive/Crypto/Kraken/kraken_backtest/ARB Foresight/arbitrage_data.db"
CHART_FILE = "/Users/nathanhart/Desktop/Archive/Crypto/Kraken/kraken_backtest/ARB Foresight/arbitrage_opportunities.png"


def load_csv(input_file):
    """
    Load new data from a CSV file.
    """
    try:
        df = pd.read_csv(input_file, parse_dates=['time'])
        logger.info(f"Loaded {len(df)} rows from {input_file}.")
        return df
    except Exception as e:
        logger.error(f"Error loading CSV file: {e}")
        return None


def load_existing_data(db_file):
    """
    Load historical data from SQLite database.
    """
    try:
        conn = sqlite3.connect(db_file)
        df = pd.read_sql("SELECT * FROM arbitrage_opportunities", conn, parse_dates=['time'])
        conn.close()
        logger.info(f"Loaded {len(df)} rows of historical data from {db_file}.")
        return df
    except Exception as e:
        logger.warning(f"No historical data found: {e}")
        return pd.DataFrame()


def save_to_database(df, db_file):
    """
    Save the DataFrame to an SQLite database, appending only new records.
    """
    try:
        conn = sqlite3.connect(db_file)
        historical_data = load_existing_data(db_file)

        if not historical_data.empty:
            # Append only new data
            new_data = df[~df['time'].isin(historical_data['time'])]
            logger.info(f"Appending {len(new_data)} new rows to the database.")
            new_data.to_sql("arbitrage_opportunities", conn, if_exists="append", index=False)
        else:
            logger.info(f"Saving {len(df)} rows to a new database.")
            df.to_sql("arbitrage_opportunities", conn, if_exists="replace", index=False)

        conn.close()
        logger.info(f"Data saved to database: {db_file}")
    except Exception as e:
        logger.error(f"Error saving data to database: {e}")


def get_combined_data(db_file):
    """
    Load and combine historical and new data.
    """
    historical_data = load_existing_data(db_file)
    return historical_data


def get_top_opportunities(df, n=10, method="percentile", value=95):
    """
    Extract the top N opportunities based on the chosen thresholding method.
    
    Parameters:
        df (DataFrame): The data containing discrepancies.
        n (int): The number of top opportunities to extract.
        method (str): The thresholding method ("percentile", "std_dev", "fixed").
        value (float): The threshold value (percentile, standard deviation multiplier, or fixed number).
        
    Returns:
        DataFrame: The top opportunities based on the chosen method.
    """
    if method == "percentile":
        threshold = df['adjusted_discrepancy'].quantile(value / 100)
        logger.info(f"Using percentile-based threshold: {value}th percentile = {threshold:.2f}")
    elif method == "std_dev":
        mean = df['adjusted_discrepancy'].mean()
        std_dev = df['adjusted_discrepancy'].std()
        threshold = mean + value * std_dev
        logger.info(f"Using std-dev-based threshold: mean + {value}*std_dev = {threshold:.2f}")
    elif method == "fixed":
        threshold = value
        logger.info(f"Using fixed numeric threshold: {threshold:.2f}")
    else:
        raise ValueError(f"Invalid method: {method}. Choose 'percentile', 'std_dev', or 'fixed'.")

    # Filter and select top N
    filtered_df = df[df['adjusted_discrepancy'] > threshold]
    logger.info(f"Filtered {len(filtered_df)} rows exceeding the threshold.")
    top_opportunities = filtered_df.nlargest(n, 'adjusted_discrepancy')
    logger.info(f"Top opportunities extracted:\n{top_opportunities}")
    return top_opportunities

def plot_opportunities(df, top_opportunities, chart_file):
    """
    Plot raw discrepancies and adjusted discrepancies with enhanced visualization.
    """
    try:
        plt.figure(figsize=(14, 8))

        # Plot raw discrepancies
        plt.subplot(2, 1, 1)
        plt.plot(df['time'], df['discrepancy'], label="Raw Discrepancy", alpha=0.4, color='blue')
        plt.scatter(top_opportunities['time'], top_opportunities['discrepancy'], color='red', label="Top Raw Opportunities", s=50)
        plt.xlabel("Time")
        plt.ylabel("Raw Discrepancy")
        plt.title("Raw Discrepancy Over Time")
        plt.legend()
        plt.grid(alpha=0.3)

        # Plot adjusted discrepancies
        plt.subplot(2, 1, 2)
        plt.plot(df['time'], df['adjusted_discrepancy'], label="Adjusted Discrepancy", alpha=0.4, color='orange')
        plt.scatter(top_opportunities['time'], top_opportunities['adjusted_discrepancy'], color='green', label="Top Adjusted Opportunities", s=50)
        plt.xlabel("Time")
        plt.ylabel("Adjusted Discrepancy")
        plt.title("Adjusted Discrepancy Over Time")
        plt.legend()
        plt.grid(alpha=0.3)

        # Rotate x-axis labels to handle dense time data
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(chart_file)
        plt.close()
        logger.info(f"Enhanced charts saved to: {chart_file}")
    except Exception as e:
        logger.error(f"Error creating chart: {e}")

def main():
    """
    Main function to orchestrate the analysis with dynamic thresholding.
    """
    logger.info("Starting analysis...")

    # Load new data
    logger.info("Loading new data...")
    new_data = load_csv(INPUT_FILE)
    if new_data is None:
        return

    # Save data to SQLite database
    logger.info("Saving data to database...")
    save_to_database(new_data, DB_FILE)

    # Load combined data
    logger.info("Loading combined data...")
    combined_data = get_combined_data(DB_FILE)

    # Analyze top opportunities
    logger.info("Analyzing top opportunities...")
    top_opportunities = get_top_opportunities(combined_data, n=10, method="percentile", value=95)

    # Generate and save chart
    logger.info("Generating charts...")
    plot_opportunities(combined_data, top_opportunities, CHART_FILE)

    logger.info("Analysis complete. Check the output for results.")


if __name__ == "__main__":
    main()
