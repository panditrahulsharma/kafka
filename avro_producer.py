==========================EXM-1===========================================

from kafka import KafkaProducer
from avro.schema import SchemaFromJSONData
from avro.io import BinaryEncoder
import io

# Define the Avro schema
schema_str = '''
{
    "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"}
    ]
}
'''

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Define the message payload
payload = {
    "name": "Alice",
    "age": 25
}

# Create an Avro encoder
schema = SchemaFromJSONData(json.loads(schema_str))
bytes_writer = io.BytesIO()
encoder = BinaryEncoder(bytes_writer)
writer = avro.io.DatumWriter(schema)

# Encode the payload as Avro binary data
writer.write(payload, encoder)
raw_bytes = bytes_writer.getvalue()

# Send the Avro binary data to Kafka
producer.send('my-topic', raw_bytes)
producer.flush()


==========================EXM-2===========================================

from typing import List
from fastapi import FastAPI
from confluent_kafka import Consumer, KafkaError

app = FastAPI()

@app.get("/messages/{topic}")
async def read_messages(topic: str):
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my-group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([topic])

    messages = []

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('End of partition reached')
            else:
                print('Error while consuming message: {}'.format(msg.error()))
        else:
            messages.append(msg.value().decode('utf-8'))

        if len(messages) >= 10: # Return up to 10 messages
            break

    consumer.close()

    return messages







