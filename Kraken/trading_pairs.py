import csv
import requests

def write_trading_pairs_to_csv(filename):
    # Make a request to the Kraken API to get the asset pairs
    resp = requests.get('https://api.kraken.com/0/public/AssetPairs')
    if resp.status_code == 200:
        data = resp.json()
        
        # Extract trading pair identifiers
        trading_pairs = [pair for pair in data['result']]

        # Write the trading pairs to a CSV file
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for pair in trading_pairs:
                writer.writerow([pair])
        
        print(f"Trading pairs written to {filename}")
    else:
        print("Failed to fetch asset pairs from Kraken API.")

if __name__ == "__main__":
    write_trading_pairs_to_csv('trading_pairs.csv')
