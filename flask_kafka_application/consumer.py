
from kafka import KafkaConsumer
# from pymongo import MongoClient
from json import loads

consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest')
consumer.subscribe(['test'])
for msg in consumer:
    print(msg)