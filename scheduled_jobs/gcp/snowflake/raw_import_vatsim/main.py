import requests
import json
import os
from snowflake.connector import connect, Error
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

#test

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_GCP_USER =  os.getenv("SNOWFLAKE_GCP_USER")
SNOWFLAKE_GCP_USER_PASSWORD = os.getenv("SNOWFLAKE_GCP_USER_PASSWORD")

def postData(arg = None):
    response = requests.get('https://map.vatsim.net/livedata/live.json')

    if str(response.status_code)[0] in [4, 5]:
        print('API failed with status code 4XX or 5XX')
        return ('API failed')

    data = response.json()

    try:
        with connect(
            user = SNOWFLAKE_GCP_USER,
            password = SNOWFLAKE_GCP_USER_PASSWORD,
            account = SNOWFLAKE_ACCOUNT,
            role = 'developer',
            warehouse = 'compute_wh',
            database = 'lake',
            schema = 'dataflow',
            session_parameters = {'QUERY_TAG': 'Import raw data'}
        ) as connection:
            cursor = connection.cursor()
            add_intelligence = ('INSERT INTO vatsim (payload) VALUES (%s)')
            data_intelligence = ({
                    "type": "vatsim",
                    "payload": data
            })
            cursor.execute(add_intelligence, (json.dumps(data_intelligence), ))
            connection.commit()
            cursor.close()
            connection.close()
    except Error as e:
        print(e)
        return(e)

postData()