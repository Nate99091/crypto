import requests

def get_trading_pairs():
    asset_pairs_resp = requests.get('https://api.kraken.com/0/public/AssetPairs')
    asset_pairs_data = asset_pairs_resp.json()

    if 'error' not in asset_pairs_data:
        trading_pairs = list(asset_pairs_data['result'].keys())
        return trading_pairs
    else:
        print("Error fetching trading pairs:", asset_pairs_data['error'])
        return []
