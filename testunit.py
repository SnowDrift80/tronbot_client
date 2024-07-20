# testunit.py

import requests
import json


class TestUnit():

    def __init__(self) -> None:
        self.base_url = "http://localhost:5000"

    async def send_request(self, endpoint, data):
        url = f"{self.base_url}/{endpoint}"
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, data=json.dumps(data))
        return response.json()

    def get_recent_deposits(self, asset, method):
        endpoint = 'get_recent_deposits'
        url = f"{self.base_url}/{endpoint}?asset={asset}&method={method}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            return {'error': response.status_code, 'message': response.text}


    async def make_payment(self, asset, method, eta, deposit_address):
        payment_data = {
            'asset': asset,
            'method': method,
            'eta': eta,
            'deposit_address': deposit_address
        }
        return await self.send_request('make_payment', payment_data)

