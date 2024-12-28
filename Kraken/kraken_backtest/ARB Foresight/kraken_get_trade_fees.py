import requests
import csv
import os
from datetime import datetime

# Directory and file path for backtesting
directory = os.path.dirname(os.path.abspath(__file__))  # Set directory to current script location
file_path = os.path.join(directory, "trade_fees.csv")

# Ensure directory exists
os.makedirs(directory, exist_ok=True)

def fetch_trade_fees():
    """Fetch trade fees and pair information from Kraken API."""
    url = "https://api.kraken.com/0/public/AssetPairs"
    response = requests.get(url)

    # Check response
    if response.status_code != 200:
        print(f"Error fetching data from Kraken API: {response.status_code}")
        return None

    data = response.json()
    if "error" in data and data["error"]:
        print(f"Kraken API Error: {data['error']}")
        return None

    return data["result"]

def save_fees_to_csv(data):
    """Save fee information to a CSV file."""
    with open(file_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        # CSV header
        writer.writerow(["Pair", "AltName", "BaseCurrency", "QuoteCurrency", "TakerFee%", "MakerFee%"])
        
        for pair, details in data.items():
            taker_fee = details.get("fees", [[0, 0]])[0][1]  # Default to 0 if not found
            maker_fee = details.get("fees_maker", [[0, 0]])[0][1]  # Default to 0 if not found
            writer.writerow([
                pair,
                details.get("altname", ""),
                details.get("base", ""),
                details.get("quote", ""),
                taker_fee,
                maker_fee
            ])

def main():
    print("Fetching trade pair information from Kraken API...")
    trade_data = fetch_trade_fees()
    if not trade_data:
        return
    
    print(f"Saving trade fee data to {file_path}...")
    save_fees_to_csv(trade_data)
    print(f"Trade fees saved successfully at {datetime.now()}.")

if __name__ == "__main__":
    main()
