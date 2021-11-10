import json
import pprint
import sseclient
from kafka import KafkaConsumer, KafkaProducer


TOPIC_NAME = "wikichange"
KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers = KAFKA_SERVER,
    api_version = (0, 11, 15)
)

def with_urllib3(url, headers):
    """Get a streaming response for the given event feed using urllib3."""
    import urllib3
    http = urllib3.PoolManager()
    return http.request('GET', url, preload_content=False, headers=headers)

def with_requests(url, headers):
    """Get a streaming response for the given event feed using requests."""
    import requests
    return requests.get(url, stream=True, headers=headers)

url = 'https://stream.wikimedia.org/v2/stream/recentchange'
headers = {'Accept': 'text/event-stream'}
response = with_urllib3(url, headers)  # or with_requests(url, headers)
client = sseclient.SSEClient(response)
for event in client.events():
    json_payload=event.data
    # json_payload=json.dumps(event.data)
    # with open('data.json', 'w', encoding='utf-8') as f:
    #     json.dump(json.loads(event.data), f, ensure_ascii=False, indent=4)
    print(event)

    # send stream data to kafka
    json_payload = str.encode(json_payload)
    producer.send(TOPIC_NAME, json_payload)
    producer.flush()