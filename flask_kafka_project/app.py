# app.py

from flask import Flask, request, jsonify
import json
from flask_cors import CORS
from kafka import KafkaConsumer, KafkaProducer
app = Flask(__name__)
TOPIC_NAME = "INFERENCE"
KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers = KAFKA_SERVER,
    api_version = (0, 11, 15)
)

@app.route('/kafka/pushToConsumers', methods=['POST'])
def kafkaProducer():
		
    req = request.get_json()
    json_payload = json.dumps(req)
    json_payload = str.encode(json_payload)
    print(json_payload)
    print(type(json_payload))
    # push data into INFERENCE TOPIC
    producer.send(TOPIC_NAME, json_payload)
    producer.flush()
    print("Sent to consumer")
    return jsonify({
        "message": "You will receive an email in a short while with the plot", 
        "status": "Pass"})

if __name__ == "__main__":
    app.run(debug=True, port = 5000)