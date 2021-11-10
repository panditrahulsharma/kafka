
from kafka import KafkaConsumer
# from pymongo import MongoClient
from json import loads


TOPIC_NAME = "wikichange"
consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest')
# consumer.subscribe(['test','EMAIL'])
consumer.subscribe([TOPIC_NAME])

for msg in consumer:
    print(msg)