from typing import Any
from schedule import every, repeat, run_pending
from types import ModuleType
import time

from services.providers import ivao, vatsim
from services.manager import Manager

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