import requests
import hashlib
import hmac
import time

class KrakenAPI:
    def __init__(self, api_key, secret_key):
        self.api_key = api_key
        self.secret_key = secret_key
        self.base_url = "https://api.kraken.com"

    def _generate_signature(self, endpoint, data):
        postdata = data + '&nonce=' + str(int(1000 * time.time()))
        encoded_postdata = postdata.encode()
        message = endpoint.encode() + hashlib.sha256(encoded_postdata).digest()
        signature = hmac.new(base64.b64decode(self.secret_key), message, hashlib.sha512)
        sigdigest = base64.b64encode(signature.digest())
        return sigdigest.decode()

    def _request(self, method, endpoint, data=None):
        headers = {
            'API-Key': self.api_key,
            'API-Sign': self._generate_signature(endpoint, data)
        }
        url = self.base_url + endpoint
        response = requests.request(method, url, headers=headers, data=data)
        return response.json()

    def get_ticker(self, pair):
        endpoint = '/0/public/Ticker'
        data = f"pair={pair}"
        response = self._request('GET', endpoint, data)
        return response

    def get_order_book(self, pair):
        endpoint = '/0/public/Depth'
        data = f"pair={pair}"
        response = self._request('GET', endpoint, data)
        return response

    def place_order(self, pair, type, price, volume):
        endpoint = '/0/private/AddOrder'
        data = f"pair={pair}&type={type}&ordertype=market&price={price}&volume={volume}"
        response = self._request('POST', endpoint, data)
        return response

    def check_balance(self):
        endpoint = '/0/private/Balance'
        response = self._request('POST', endpoint)
        return response
