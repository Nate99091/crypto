import requests
import hashlib
import hmac
import time
import base64
import json
import aiohttp
import asyncio

class KrakenAPI:
    def __init__(self, api_key, secret_key):
        self.api_key = api_key
        self.secret_key = secret_key
        self.base_url = "https://api.kraken.com"

    def _generate_signature(self, endpoint, data):
        # Serialize the data dictionary into a string
        data_str = json.dumps(data)

        # Calculate the nonce
        nonce = str(int(1000 * time.time()))

        # Concatenate the endpoint, serialized data, and nonce
        message = endpoint.encode() + data_str.encode() + nonce.encode()

        # Generate the signature using HMAC and other cryptographic functions
        signature = hmac.new(base64.b64decode(self.secret_key), message, hashlib.sha512)
        sigdigest = base64.b64encode(signature.digest()).decode()

        return sigdigest

    async def _request(self, method, endpoint, data=None):
        headers = {
            'API-Key': self.api_key,
            'API-Sign': self._generate_signature(endpoint, data)
        }
        url = self.base_url + endpoint

        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, headers=headers, data=data) as response:
                return await response.json()

    async def get_ticker(self, pair):
        endpoint = '/0/public/Ticker'
        data = {"pair": pair}
        response = await self._request('GET', endpoint, data=json.dumps(data))
        return response

async def main():
    # Credentials
    api_key = '+MhCklHSN/ujlRpZN6tAo47QAJrMraves+q9f8C99Va9mRQXG83QMSrv'
    secret_key = 'wqb3B+4Qkj2fWaT1y5fiXzDlvcIX36kf5YXrdFZGwMkaXlvZ354btegIF/0FxwfFFElYF/+AzbolFvAQJGVM8g=='

    # Instantiate KrakenAPI with your API key and secret key
    kraken_api = KrakenAPI(api_key, secret_key)

    # Fetch trading pairs from the Kraken API
    asset_pairs_resp = requests.get('https://api.kraken.com/0/public/AssetPairs')
    asset_pairs_data = asset_pairs_resp.json()

    # Check if the response contains no errors
    if 'error' not in asset_pairs_data:
        # Extract trading pairs from the 'result' dictionary keys
        trading_pairs = list(asset_pairs_data['result'].keys())

        # Define a list to store tasks
        tasks = []
        for trading_pair in trading_pairs:
            task = asyncio.create_task(kraken_api.get_ticker(trading_pair))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)

        # Process responses
        for response, pair in zip(responses, trading_pairs):
            try:
                # Extract data from the ticker response
                bid_price = response['result'][pair]['b'][0]
                ask_price = response['result'][pair]['a'][0]
                last_trade_price = response['result'][pair]['c'][0]

                # Print specific data for each trading pair
                print(f"Trading Pair: {pair}")
                print(f"Bid Price: {bid_price}")
                print(f"Ask Price: {ask_price}")
                print(f"Last Trade Price: {last_trade_price}")
                print("-" * 30)
            except KeyError:
                print(f"Error processing data for {pair}: {response['error']}")
    else:
        print("Error fetching trading pairs:", asset_pairs_data['error'])

if __name__ == "__main__":
    asyncio.run(main())
