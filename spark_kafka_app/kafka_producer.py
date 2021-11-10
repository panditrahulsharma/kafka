from kafka import KafkaProducer
from time import sleep
from json import dumps
import pandas as pd
# producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

df=pd.read_csv("dataset/data.csv")

for index,row in df.iterrows():
    print(type(row))
    data = {'row' : list(row)}
    producer.send('NM', value=data)
    sleep(1)
    print(index)