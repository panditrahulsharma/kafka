import json

from sseclient import SSEClient as EventSource
from kafka import KafkaProducer

TOPIC_NAME = "wiki_changes"
KAFKA_SERVER = "localhost:9092"

# Create producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER, #Kafka server
    value_serializer=lambda v: json.dumps(v).encode('utf-8') #json serializer
    )

# Read streaming event
url = 'https://stream.wikimedia.org/v2/stream/recentchange'
try:
    for event in EventSource(url):
        if event.event == 'message':
            try:
                change = json.loads(event.data)
            except ValueError:
                pass
            else:
                #Send msg to topic wiki-changes
                print(change)
                producer.send(TOPIC_NAME, change)
                print("--------------------------")

except KeyboardInterrupt:
    print("process interrupted")