import requests
from mysql.connector import connect, Error
import json

with open("../secrets.json") as file:
    secret = json.load(file)

db_host = secret['hostinger']['db_host']
db_username = secret['hostinger']['db_username']
db_password = secret['hostinger']['db_password']
db_name = secret['hostinger']['db_name']

url = 'https://map.vatsim.net/livedata/live.json'


def getData (res):

    response = requests.get(url)

    if str(response.status_code)[0] in [4, 5]:
        print('API failed with status code 4XX or 5XX')
        return ('API failed')

    data = response.json()
    try:
        with connect(
            host = db_host,
            user = db_username,
            password = db_password,
            database = db_name
        ) as connection:
            cursor = connection.cursor()
            add_intelligence = ("INSERT INTO lake (payload) VALUES (%s)")
            data_intelligence = ({
                    "type": "vatsim_raw",
                    "payload": json.dumps(data)
            })
            cursor.execute(add_intelligence, (json.dumps(data_intelligence), ))
            connection.commit()
            cursor.close()
            connection.close()
            return ('Success')
    except Error as e:
        return(e)
