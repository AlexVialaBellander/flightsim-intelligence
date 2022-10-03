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


def consolidate(date_from: datetime, date_to: datetime) -> Union[list,None]:
    logging.info(f"Consolidating data...")
    # Create a timestamp for when consolidation begins
    consolidation_time = datetime.now().isoformat()

    # services   
    services = ["ivao", "vatsim"]
    
    # data models
    model_names = ["pilots", "controllers", "general"]                
    
    # TODO: use os.path
    temp_consolidation_path = "datastore/consolidated"
   
    # create consolidated directory
    os.makedirs(temp_consolidation_path, exist_ok=True)

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
    
    # init variable that includes the header for the first iteration
    header = {service:True for service in services}
    
    if len(days) != 0:
        for day in days:
            logging.info(f"Processing {day}...")
            for file in os.listdir(f"datastore/{day}"):
                with open(f"datastore/{day}/{file}", "r") as data:
                    try:
                        # update min and max dates
                        min_date = min(min_date, parser.parse(day))
                        max_date = max(max_date, parser.parse(day))
                        # fetch service name
                        service = file.split("_")[0]
                        if service not in services:
                            logging.critical(f"Unknown service {service}")
                            raise Exception(f"Unknown service {service}")
                        # add empty list of dataframes to process
                        batch = dict()

                        # begin JSON Parsing
                        df_top = pd.json_normalize(json.load(data))

                        # fetch different columns depending on service
                        if service == "vatsim":
                            df_general = df_top[df_top.columns[:11]]
                        elif service == "ivao":
                            df_general = df_top[[*df_top.columns[:6], *df_top.columns[12:]]]
                        
                        # add service and consolidation time to dataframes
                        df_general.insert(0, "service", service)
                        df_general.insert(1, "consolidated_at", consolidation_time)
                            
                        # process columns depending on service
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
                        
                        # add dfs to batch
                        batch["pilots"] = df_pilots
                        batch["controllers"] = df_controllers
                        batch["general"] = df_general
                        
                        # check model names
                        if list(batch.keys()) != model_names:
                            logging.critical(f"Unknown model {batch.keys()}")
                            raise Exception(f"Unknown model {batch.keys()}")

                        # for each df in batch, append data to disk on a temporary file
                        for name in model_names:
                            df = batch.get(name)
                            df.to_csv(
                                f"{temp_consolidation_path}/.temp_{service}_{name}.csv",
                                mode="a",
                                header=header[service],
                                index=False,
                            )
                        # change flag to not include header on next iteration
                        header[service] = False
                    
                    except Exception:
                        logging.exception("Error parsing file: {}".format(file))
                        logging.exception(traceback.format_exc())

        # Write CSV to consolidated directory
        # create directory name from max and min date
        if min_date == max_date:
            dir_name = min_date.strftime('%Y-%m-%d')
        else:
            dir_name = f"{min_date.strftime('%Y-%m-%d')}_{max_date.strftime('%Y-%m-%d')}"
        # update path variable to include directory name
        path = f"datastore/consolidated/{dir_name}"
        # create directory
        os.makedirs(path, exist_ok=True)
        logging.debug(f"Writing concolidated data to {path}")
        # return paths
        paths = []
        # for each service and model name, read temporary file and write to consolidated directory
        for service in services:
            for model in model_names:
                os.replace(f"{temp_consolidation_path}/.temp_{service}_{model}.csv", f"{path}/{service}_{model}.csv")
                paths.append(f"{path}/{service}_{model}.csv")
        logging.info(f"Consolidation finished.")
        return paths
    logging.info(f"Exited consolidation.")
    return None

def process(paths: list) -> None:
    # fetch data from source and create dataframes
    dfs = {}
    for path in paths:
        print(path)
        filename = path.split("/")[-1][:-4]
        dfs[filename] = pd.read_csv(
            path, 
            on_bad_lines="warn")
    # for each dataframe, rename columns, add service column and concat
    for filename, df in dfs.items():
        service_name, model = filename.split("_")
        column_map = {value[service_name]:key for key, value in columns[model].items()}
        df = df.rename(columns=column_map)
        df["service"] = service_name
        dfs[filename] = df[list(column_map.values()) + ["service"]]
    # combine dfs
    path = "/".join(paths[0].split("/")[:-1])
    pilots_data = pd.concat([dfs['ivao_pilots'], dfs['vatsim_pilots']])
    controller_data = pd.concat([dfs['ivao_controllers'], dfs['vatsim_controllers']])
    pilots_data.to_csv(f"{path}/pilots_data.csv")
    controller_data.to_csv(f"{path}/controller_data.csv")