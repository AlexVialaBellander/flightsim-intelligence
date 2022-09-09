import coloredlogs
import hashlib
import json
import logging
import os
import sys
import traceback

from datetime import datetime


# Load main.json config file
CONFIG = json.load(open('main.json'))

file_handler = logging.FileHandler(filename='logs.log')
stdout_handler = logging.StreamHandler(stream=sys.stdout)
handlers = [file_handler, stdout_handler]

logging.basicConfig(
    encoding='utf-8', 
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=handlers
)

logger = logging.getLogger('LOGGER_NAME')
coloredlogs.install()

class Data():
    def __init__(self, source, payload):
        self.hash = hashlib.md5(json.dumps(payload).encode('utf-8')).hexdigest()
        self.created_at = datetime.now().isoformat()
        self.source = source
        self.owner = dict(name=CONFIG["name"], version=CONFIG["version"])
        self.payload = payload

class Manager():
    def __init__(self, service):
        self.service = service
        self.name = service.__name__.split(".")[-1]
        self.current = Data(self.service.API_URL, None)
        self.prior = Data(self.service.API_URL, None)
        os.makedirs("datastore", exist_ok=True)
        
    # fetch from source
    def fetch(self):
        try:
            self.prior = self.current
            self.current = Data(self.service.API_URL, self.service._get_raw())
        except Exception as e:
            logging.exception(traceback.format_exc())
            
    def write(self):
        # make directory for each day
        path = "datastore/{}".format(datetime.now().strftime("%Y-%m-%d"))
        os.makedirs(path, exist_ok=True)
        # write data to file if content has changed
        if self.prior.hash != self.current.hash:
            with open(f"{path}/{self.name}_{datetime.now().isoformat()}.json", "w") as file:
                json.dump(self.current.__dict__, file)
            logging.info(f"Saved: {self.name}, {self.current.hash}")
        else:
            logging.info(f"No change: {self.name}, {self.current.hash}")
