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
