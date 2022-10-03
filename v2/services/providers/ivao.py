import requests

API_URL = "https://api.ivao.aero/v2/tracker/whazzup"


def _get_raw():
    return requests.get(API_URL).json()
