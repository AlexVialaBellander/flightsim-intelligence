import requests
import json
import os
from snowflake.connector import connect, Error
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_GCP_USER =  os.getenv("SNOWFLAKE_GCP_USER")
SNOWFLAKE_GCP_USER_PASSWORD = os.getenv("SNOWFLAKE_GCP_USER_PASSWORD")

def postAggregated(arg = None):

    response = requests.get('https://fs-api.grubse.com/data') 
    poscon_pilots = requests.get('https://atc.poscon.net/api/air/leases/countCurrent').json()
    poscon_atc = requests.get('https://atc.poscon.net/api/atc/controllers/countActive').json()
    poscon = poscon_atc + poscon_pilots

    if str(response.status_code)[0] in [4, 5]:
        print('API failed with status code 4XX or 5XX')
        return 'API failed'

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
            session_parameters = {'QUERY_TAG': 'Import aggregated data'}
        ) as connection:
            print(connection)
            cursor = connection.cursor()
            add_intelligence = ("INSERT INTO archive "
                "(ivao, vatsim, poscon) "
                "VALUES (%s, %s, %s)")
            data_intelligence = (data['data']['ivao'], data['data']['vatsim'], poscon)
            cursor.execute(add_intelligence, data_intelligence)
            connection.commit()
            cursor.close()
            connection.close()
    except Error as e:
        print(e)
        return e

postAggregated()
