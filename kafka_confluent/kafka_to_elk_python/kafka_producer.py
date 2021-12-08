from kafka import KafkaProducer
from time import sleep
from json import dumps
import pandas as pd
# producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

producer = KafkaProducer(bootstrap_servers=['3.14.27.172:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

df=pd.read_csv("dataset/data.csv")

"""
#let's iterate over swapi people documents and index them
# https://tryolabs.com/blog/2015/02/17/python-elasticsearch-first-steps
import json
i = 1
while 1<10:
    r = requests.get('https://swapi.dev/api/people/'+ str(i))
    i=i+1
    print(r.content)

"""

for index,row in df.iterrows():
    print(type(row))
    data = {'row' : list(row)}
    producer.send('sinkawsss', value=data)
    sleep(1)
    print(index)