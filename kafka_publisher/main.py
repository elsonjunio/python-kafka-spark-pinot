from kafka import KafkaProducer
import mysql.connector
from json import dumps
from time import sleep


def run():

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:
                             dumps(x).encode('utf-8'))

    cnx = mysql.connector.connect(user='root', password='',
                                  host='localhost',
                                  database='protector_v1_ex')
    cursor = cnx.cursor()

    query = 'SELECT id, description, login, farm_id, farm_name, processed, submitted_at, created_at, latitude, longitude, real_latitude, \
        real_longitude, forced_location, area_id, issue_id, deleted, synced_at FROM collection;'
    cursor.execute(query)
    for (id, description, login, farm_id, farm_name, processed, submitted_at, created_at, latitude,
         longitude, real_latitude, real_longitude, forced_location, area_id, issue_id, deleted, synced_at) in cursor:

        item = {
            'id': id,
            'description': description,
            'login': login,
            'farm_id': farm_id,
            'farm_name': farm_name,
            'processed': processed,
            'submitted_at': submitted_at.strftime('%d/%m/%y %H:%M:%S'),
            'created_at': created_at.strftime('%d/%m/%y %H:%M:%S'),
            'latitude': latitude,
            'longitude': longitude,
            'real_latitude': real_latitude,
            'real_longitude': real_longitude,
            'forced_location': forced_location,
            'area_id': area_id,
            'issue_id': issue_id,
            'deleted': deleted,
            'synced_at': synced_at.strftime('%d/%m/%y %H:%M:%S')}

        # sleep(3)
        producer.send('numtest', value=item)

    # producer.flush()


if __name__ == "__main__":
    while True:
        sleep(30)
        run()
