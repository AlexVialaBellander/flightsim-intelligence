import requests
from mysql.connector import connect, Error
import json 

with open("../secrets.json") as file:
    secret = json.load(file)

db_host = secret['db_host']
db_username = secret['db_username']
db_password = secret['db_password']
db_name = secret['db_name']

def getData(req):

    response = requests.get(url) 
    poscon_pilots = requests.get('https://atc.poscon.net/api/air/leases/countCurrent').json()
    poscon_atc = requests.get('https://atc.poscon.net/api/atc/controllers/countActive').json()
    poscon = poscon_atc + poscon_pilots

    if str(response.status_code)[0] in [4, 5]:
        print('API failed with status code 4XX or 5XX')
        return 'API failed'

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
            add_intelligence = ("INSERT INTO archive "
                "(ivao, vatsim, poscon) "
                "VALUES (%s, %s, %s)")
            data_intelligence = (data['data']['ivao'], data['data']['vatsim'], poscon)
            cursor.execute(add_intelligence, data_intelligence)
            connection.commit()
            cursor.close()
            connection.close()
            return 'Success'
    except Error as e:
        return e

