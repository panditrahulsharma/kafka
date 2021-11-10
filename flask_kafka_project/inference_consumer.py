# inference_consumer.py

from kafka import KafkaConsumer, KafkaProducer
import os
import json
import uuid
from concurrent.futures import ThreadPoolExecutor

TOPIC_NAME = "INFERENCE"

KAFKA_SERVER = "localhost:9092"

NOTIFICATION_TOPIC = "NOTIFICATION"
EMAIL_TOPIC = "EMAIL"

consumer = KafkaConsumer(
    TOPIC_NAME,
    # to deserialize kafka.producer.object into dict
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
)

producer = KafkaProducer(
    bootstrap_servers = KAFKA_SERVER,
    api_version = (0, 11, 15)
)

def inferencProcessFunction(data):
    # process steps

    notification_data =data['notification']
    email_data = data['email']
    # convert list to json object
    email_data=json.dumps(email_data)
    notification_data=json.dumps(notification_data)

    # convert json to byte like string
    email_data = str.encode(email_data)
    notification_data = str.encode(notification_data)

    producer.send(NOTIFICATION_TOPIC, notification_data)
    producer.flush()
    producer.send(EMAIL_TOPIC, email_data)
    producer.flush()

for inf in consumer:
	
    inf_data = inf.value
    print(inf_data)
    inferencProcessFunction(inf_data)