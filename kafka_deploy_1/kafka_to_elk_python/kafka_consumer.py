
from kafka import KafkaConsumer
# from pymongo import MongoClient
from json import loads


TOPIC_NAME = "sparkproduce"
consumer = KafkaConsumer(bootstrap_servers=['3.14.27.172:9092'], auto_offset_reset='earliest') #begining
# consumer.subscribe(['test','EMAIL'])
consumer.subscribe([TOPIC_NAME])

for msg in consumer:
    print(msg)