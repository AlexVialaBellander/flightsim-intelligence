import requests
from snowflake.connector import connect, Error
import json 

with open("../secrets.json") as file:
    secret = json.load(file)

db_host = secret['db_host']
db_username = "fsgcp"
db_password = "cyA;'9fK^;z4x<{5"
db_name = secret['db_name']

url = 'https://fs-api.grubse.com/data'

response = requests.get(url) 

if str(response.status_code)[0] in [4, 5]:
    print('API failed with status code 4XX or 5XX')
    exit()

data = response.json()
poscon_pilots = requests.get('https://atc.poscon.net/api/air/leases/countCurrent').json()
poscon_atc = requests.get('https://atc.poscon.net/api/atc/controllers/countActive').json()
poscon = poscon_atc + poscon_pilots

try:
    with connect(
        host = db_host,
        user = db_username,
        password = db_password,
        database = db_table
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