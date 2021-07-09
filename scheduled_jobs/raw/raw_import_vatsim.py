import requests
from mysql.connector import connect, Error
import json

with open("../secrets.json") as file:
    secret = json.load(file)

db_host = secret['db_host']
db_username = secret['db_username']
db_password = secret['db_password']
db_name = secret['db_name']

url = 'https://map.vatsim.net/livedata/live.json'

response = requests.get(url)

if str(response.status_code)[0] in [4, 5]:
    print('API failed with status code 4XX or 5XX')
    print('API failed')

data = response.json()
try:
    with connect(
        host = db_host,
        user = db_username,
        password = db_password,
        database = db_name
    ) as connection:
        print(connection)
        cursor = connection.cursor()
        add_intelligence = ("INSERT INTO lake (payload) VALUES (%s)")
        data_intelligence = ({
                "type": "vatsim_raw",
                "payload": json.dumps(data)
        })
        print(data_intelligence)
        cursor.execute(add_intelligence, (json.dumps(data_intelligence), ))
        connection.commit()
        cursor.close()
        connection.close()
        print ('Success')
except Error as e:
    print(e)
