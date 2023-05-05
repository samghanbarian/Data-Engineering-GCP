import os
import argparse
from time import time
from datetime import timedelta

import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, tags=["fetch"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(url: str):
    # # the backup files are gzipped, and it's important to keep the correct extension
    # # for pandas to be able to open the file
    # if url.endswith('.csv.gz'):
    #     csv_name = 'yellow_tripdata_2021-01.csv.gz'
    # else:
    #     csv_name = 'output.csv'
    """ download the data and turn into dataframe"""
    pq_name = "output.parquet"
    os.system(f"wget {url} -O {pq_name}")
    trips = pq.read_table(pq_name)
    #convert to dataframe
    df = trips.to_pandas() 
    return df

# @task(log_prints=True)
# def transform_data(df):
#     print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
#     df = df[df['passenger_count'] != 0]
#     print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
#     return df

@task(log_prints=True, retries=3)
def load_data(table_name, df):
    """ This will load the data into postgres"""
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')

@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")

@flow(name="Ingest Data")
def load_data_flow(table_name: str = "yellow_taxi_trips"):

    dataset_url =f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    log_subflow(table_name)
    data = fetch(dataset_url)
    # data = transform_data(raw_data)
    load_data(table_name, data)

if __name__ == '__main__':
    main_flow(table_name = "yellow_trips")