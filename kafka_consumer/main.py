from kafka import KafkaConsumer
from json import loads


if __name__ == "__main__":

    consumer = KafkaConsumer(
        'asdf',
        bootstrap_servers=['localhost:20092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    for message in consumer:
        message = message.value
        print('{}'.format(message))
