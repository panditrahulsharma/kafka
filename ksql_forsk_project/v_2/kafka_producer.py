from confluent_kafka import Producer
import json
from time import sleep
from faker import Faker
import psycopg2
from time import sleep
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
p = Producer({'bootstrap.servers': 'localhost:9092,localhost:9093'})
conn = create_engine('mysql://mysqluser:mysqlpw@0.0.0.0:3000/inventory') # connect to server

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def get_fake_add():
    faker = Faker() 
    fields = ['job','company','residence','username','name','sex','address','mail','ssn']

    data = faker.profile(fields)
    print(data)
    # df=pd.DataFrame(data)
    df=pd.DataFrame(data,index = [1])
    df.to_sql('raw_address',conn, if_exists='append')
    # telecom_data=json.dumps(data)
    # telecom_data=str.encode(telecom_data)
    # return telecom_data
dataset_name = "data/raw_cdr_data_header.csv"

def random_cdr_data():
    raw_cdr_data = pd.read_csv(dataset_name,low_memory=False)
    # print(raw_cdr_data.columns)
    # load random rows
    data=list(raw_cdr_data.sample(n=1).iloc[0,:])
    # data={i:data[i] for i in range(0,len(data))}
    header=list(raw_cdr_data.columns)
    data_range=len(data)
    # data_range=15
    data={header[i]:int(data[i]) if type(data[i])==np.int64 else data[i] for i in range(0,data_range)}
    telecom_data=json.dumps(data)
    telecom_data=str.encode(telecom_data)
    return telecom_data

while True:
    get_fake_add()
    sleep(15)

# for i in range(0,10000):
#     # Trigger any available delivery report callbacks from previous produce() calls
#     p.poll(0)

#     # Asynchronously produce a message, the delivery report callback
#     # will be triggered from poll() above, or flush() below, when the message has
#     # been successfully delivered or failed permanently.
#     p.produce('raw-telecom2', get_fake_add(), callback=delivery_report)
#     sleep(10)
# # Wait for any outstanding messages to be delivered and delivery report
# # callbacks to be triggered.
# p.flush()
