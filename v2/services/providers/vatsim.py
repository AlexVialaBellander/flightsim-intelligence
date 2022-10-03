import requests

API_URL = "https://data.vatsim.net/v3/vatsim-data.json"


def _get_raw():
    return requests.get(API_URL).json()
