import requests
from snowflake.connector import connect, Error
import json

with open("../secrets.json") as file:
    secret = json.load(file)

snowflake_account = secret['snowflake']['account']
db_user = secret['snowflake']['user']
db_password = secret['snowflake']['password']

url = 'https://map.vatsim.net/livedata/live.json'


def getData (res):

    response = requests.get(url)

    if str(response.status_code)[0] in [4, 5]:
        print('API failed with status code 4XX or 5XX')
        return ('API failed')

    data = response.json()
    try:
        with connect(
            user = db_user,
            password = db_password,
            account = snowflake_account,
            role = 'developer',
            warehouse = 'compute_wh',
            database = 'lake',
            schema = 'dataflow',
            session_parameters = {'QUERY_TAG': 'Import vatsim raw data'}
        ) as connection:
            cursor = connection.cursor()
            add_intelligence = ("INSERT INTO vatsim (payload) VALUES (%s)")
            data_intelligence = ({
                    "type": "vatsim_raw",
                    "payload": data
            })
            cursor.execute(add_intelligence, (json.dumps(data_intelligence), ))
            connection.commit()
            cursor.close()
            connection.close()
            return ('Success')
    except Error as e:
        return(e)
