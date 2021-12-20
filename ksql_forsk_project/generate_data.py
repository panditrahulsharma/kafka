from faker import Faker
import psycopg2
from time import sleep
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import pandas as pd
if __name__ == '__main__':
    # conn = create_engine("postgresql://TEST:password@localhost:5432/TEST") 
    conn = create_engine('mysql://mysqluser:mysqlpw@0.0.0.0:3000/inventory') # connect to server
    dataset_name = "data/raw_cdr_data_header.csv"

    while True: 
        # data = faker.profile(fields)
        dataset_name = "data/raw_cdr_data_header.csv"
        raw_cdr_data = pd.read_csv(dataset_name,low_memory=False)
        df=raw_cdr_data.sample(n=1)
        print(f"Inserting data {df.index}")
        df.to_sql('raw_telecom',conn, if_exists='append')
        sleep(10)




