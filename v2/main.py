import pandas as pd
import json
import logging
import os
import time


from datetime import datetime, timedelta
from schedule import every, repeat, run_pending

from services.manager import *
from services.providers import ivao, vatsim

# Load main.json config file
CONFIG = json.load(open("main.json"))
SERVICES = [ivao, vatsim]
PROVIDERS = [Manager(service) for service in SERVICES]


class Config:
    def __init__(self, **entries):
        self.__dict__.update(entries)


CONFIG = Config(**CONFIG)
# NOT USING ABOVE CONFIG YET

@repeat(every(10).seconds)
def fetch_raw() -> None:
    for provider in PROVIDERS:
        provider.fetch()
        provider.write()


@repeat(every().day.at("00:00:30"))
def run_consolidation(
    date_from=datetime.now().date() - timedelta(days=1),
    date_to=datetime.now().date(),
) -> None:
    consolidate(date_from, date_to)


while True:
    run_pending()
    time.sleep(1)
