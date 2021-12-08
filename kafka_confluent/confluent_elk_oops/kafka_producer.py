from confluent_kafka import Producer
import uuid
import json
import time
import random
# !pip install elasticsearch
# !pip install random-address
# !pip install confluent-kafka
import random_address
from time import sleep


def get_fake_add():
    address=random_address.real_random_address_by_postal_code('32409')
    address=json.dumps(address)
    # address=str.encode(address)
    return address

class KafkaProducer:

    def __init__(self):
        self.bootstrap_servers = "localhost:9092"
        self.topic = "data-address"
        self.p = Producer({'bootstrap.servers': self.bootstrap_servers})

    def produce(self, msg):

        # serialized_message = json.dumps(msg)
        self.p.produce(self.topic,msg)
        self.p.poll(0)
        time.sleep(1)
        # self.p.flush()


if __name__ == '__main__':
    producer = KafkaProducer()
    while True:
        try:
            data =get_fake_add()
            print("Message is :: {}".format(data))
            producer.produce(data)
        except StopIteration:
            exit()
        except KeyboardInterrupt:
            print("Printing last message before exiting :: {}".format(serialized_data))
            exit()

