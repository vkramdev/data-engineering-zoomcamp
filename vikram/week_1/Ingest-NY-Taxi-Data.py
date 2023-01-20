import argparse

from time import time
import os

import pandas as pd
import pyarrow.parquet as pa
from sqlalchemy import create_engine

def main(params):
    user=params.user
    password=params.password
    db=params.db
    host=params.host
    port=params.port
    table_name=params.table_name
    url=params.url
    csv_name='output.csv'

    #data= pd.read_parquet("yellow_tripdata_2021-01.parquet", engine='fastparquet')
    os.system(f'wget {url} -o {csv_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{url}:{port}/{db}')

    data_p = pa.ParquetFile(csv_name)
    for i in data_p.iter_batches(batch_size=100000):
        t1=time()
        print("records")
        df = i.to_pandas()
        df.to_sql(name='yellow_taxi_data', con=engine, if_exists="append")
        t2=time()
        
        print(f"time to insert the batch = {t2-t1}")

parser = argparse.ArgumentParser(description='Ingest the NY Taxi dat') 

# username ,
# password,
# host,
# port,
# database name
# table name
# url of the csv

parser.add_argument('--user', help='user name for postgres')
parser.add_argument('--password', help='user name for postgres')
parser.add_argument('--host', help='hostname for the postgres')
parser.add_argument('--port', help='port to connect')
parser.add_argument('--db',help='database name to connect')
parser.add_argument('--table_name',help='table to connect to')
parser.add_argument('--url', help='url to connect')

args=parser.parse_args()
print(args.accumulate(args.integers))
