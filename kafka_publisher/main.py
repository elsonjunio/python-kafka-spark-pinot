from kafka import KafkaProducer
from json import dumps
from time import sleep
import csv


def run():

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:
                             dumps(x).encode('utf-8'))

    with open('PublicReleaseArrestDataUPDATE.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')

        for row in reader:
            producer.send('prad', value=row)

    # producer.flush()


if __name__ == "__main__":
    while True:
        run()
        sleep(10)
