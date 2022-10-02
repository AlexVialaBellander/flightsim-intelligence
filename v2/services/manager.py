import coloredlogs
import dateutil.parser as parser
import pandas as pd
import hashlib
import json
import logging
import os
import sys
import traceback

from datetime import datetime
from typing import Union

# Load main.json config file
CONFIG = json.load(open("main.json"))
columns = CONFIG['config']['consolidation']['columns']

file_handler = logging.FileHandler(filename="logs.log")
stdout_handler = logging.StreamHandler(stream=sys.stdout)
handlers = [file_handler, stdout_handler]

logging.basicConfig(
    encoding="utf-8",
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=handlers,
)

logger = logging.getLogger("LOGGER_NAME")
coloredlogs.install()

# Data Management
class Data:
    def __init__(self, source: str, payload: json) -> None:
        self.hash = hashlib.md5(json.dumps(payload).encode("utf-8")).hexdigest()
        self.created_at = datetime.now().isoformat()
        self.source = source
        self.owner = dict(name=CONFIG["name"], version=CONFIG["version"])
        self.payload = payload


# Service Management
def init() -> None:
    if not os.path.exists(".internal"):
        logging.info("Initialising local-data-management-service")
        with open(".internal", "w+") as file:
            file.write(
                json.dumps(
                    dict(
                        start_time=datetime.now().isoformat(),
                        latest_run="None",
                    ),
                    indent=4,
                )
            )
            logging.debug("Created .consolidation file")
    else:
        with open(".internal", "r+") as file:
            data = json.loads(file.read())
            data["start_time"] = datetime.now().isoformat()
            logging.debug(f"Latest run was at {data['latest_run']}")


# Service Manager
class Manager:
    def __init__(self, service) -> None:
        self.service = service
        self.name = service.__name__.split(".")[-1]
        self.current = Data(self.service.API_URL, None)
        self.prior = Data(self.service.API_URL, None)
        os.makedirs("datastore", exist_ok=True)

    # fetch from source
    def fetch(self) -> None:
        try:
            self.prior = self.current
            self.current = Data(self.service.API_URL, self.service._get_raw())
        except Exception as e:
            logging.exception(traceback.format_exc())

    def write(self) -> None:
        # make directory for each day
        path = "datastore/{}".format(datetime.now().strftime("%Y-%m-%d"))
        os.makedirs(path, exist_ok=True)
        # write data to file if content has changed
        if self.prior.hash != self.current.hash:
            with open(
                f"{path}/{self.name}_{datetime.now().isoformat()}.json", "w"
            ) as file:
                json.dump(self.current.__dict__, file)
            logging.info(f"Saved: {self.name}, {self.current.hash}")
        else:
            logging.info(f"No change: {self.name}, {self.current.hash}")


def consolidate(date_from: datetime, date_to: datetime) -> Union[str,None]:
    logging.info(f"Consolidating data...")
    # Create a timestamp for when consolidation begins
    consolidation_time = datetime.now().isoformat()

    general_tables = dict(ivao=[], vatsim=[])
    pilots_tables = dict(ivao=[], vatsim=[])
    controllers_tables = dict(ivao=[], vatsim=[])

    # In datastore for each date directory, load each file and create a dataframe
    days = [
        folder_name
        for folder_name in os.listdir("datastore")
        if not folder_name == "consolidated"
        and not folder_name.startswith(".")
        and parser.parse(folder_name).date() >= date_from
        and parser.parse(folder_name).date() < date_to
    ]
    logging.info(f"Found {len(days)} days of data to consolidate.")
    min_date = parser.parse("3200-01-01")
    max_date = parser.parse("1690-01-01")
    if len(days) != 0:
        for day in days:
            for file in os.listdir(f"datastore/{day}"):
                with open(f"datastore/{day}/{file}", "r") as data:
                    min_date = min(min_date, parser.parse(day))
                    max_date = max(max_date, parser.parse(day))
                    service = file.split("_")[0]

                    # Begin JSON Parsing
                    df_top = pd.json_normalize(json.load(data))

                    if service == "vatsim":
                        df_general = df_top[df_top.columns[:11]]
                    elif service == "ivao":
                        df_general = df_top[[*df_top.columns[:6], *df_top.columns[12:]]]
                    df_general.insert(0, "service", service)
                    df_general.insert(1, "consolidated_at", consolidation_time)

                    if service == "vatsim":
                        df_pilots = pd.json_normalize(df_top["payload.pilots"][0]).merge(
                            df_general[["hash"]], how="cross"
                        )
                        df_controllers = pd.json_normalize(
                            df_top["payload.controllers"][0]
                        ).merge(df_general[["hash"]], how="cross")
                    elif service == "ivao":
                        df_pilots = pd.json_normalize(
                            df_top["payload.clients.pilots"][0]
                        ).merge(df_general[["hash"]], how="cross")
                        df_controllers = pd.json_normalize(
                            df_top["payload.clients.atcs"][0]
                        ).merge(df_general[["hash"]], how="cross")

                    # Append to lists
                    general_tables[service].append(df_general)
                    pilots_tables[service].append(df_pilots)
                    controllers_tables[service].append(df_controllers)

        # Write CSV to consolidated directory
        if min_date == max_date:
            dir_name = min_date.strftime('%Y-%m-%d')
        else:
            dir_name = f"{min_date.strftime('%Y-%m-%d')}_{max_date.strftime('%Y-%m-%d')}"
        path = f"datastore/consolidated/{dir_name}"
        os.makedirs(path, exist_ok=True)
        logging.debug("Writing concolidated data to CSV")
        for service in ["ivao", "vatsim"]:
            for dfs, name in [
                (general_tables, "general"),
                (pilots_tables, "pilots"),
                (controllers_tables, "controllers"),
            ]:
                df = pd.concat(dfs[service])
                df.to_csv(f"{path}/{service}_{name}.csv")
        return path if path is not None else None
    logging.info(f"Consolidation finished.")
    return None

def process(path: str) -> None:
    # fetch data from source and create dataframes
    dfs = {}
    for file in os.listdir(path):
        with open(f"{path}/{file}", "r") as data:
            dfs[file] = pd.read_csv(f"{path}/{file}")
    dfs.keys()
    # for each dataframe, rename columns, add service column and concat
    for filename, df in dfs.items():
        service_name, content_type = filename[:-4].split("_")
        column_map = {value[service_name]:key for key, value in [content_type].items()}
        df = df.rename(columns=column_map)
        df["service"] = service_name
        dfs[filename] = df[list(column_map.values()) + ["service"]]
    # combine dfs
    pilots_data = pd.concat([dfs['ivao_pilots.csv'], dfs['vatsim_pilots.csv']])
    controller_data = pd.concat([dfs['ivao_controllers.csv'], dfs['vatsim_controllers.csv']])
    pilots_data.to_csv(f"{path}/pilots_data.csv")
    controller_data.to_csv(f"{path}/controller_data.csv")