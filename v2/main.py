import time
import json

from schedule import every, repeat, run_pending
from types import ModuleType
from typing import Any

from services.manager import Manager
from services.providers import ivao, vatsim

# Load main.json config file
CONFIG = json.load(open('main.json'))
SERVICES = [ivao, vatsim]
PROVIDERS = [Manager(service) for service in SERVICES]

@repeat(every(10).seconds)
def fetch_raw() -> None:
    for provider in PROVIDERS:
        provider.fetch()
        provider.write()

while True:
    run_pending()
    time.sleep(1)