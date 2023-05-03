#!/usr/bin/env python
# coding: utf-8

import pandas as pd

import argparse
import os

from sqlalchemy import create_engine
import pyarrow.parquet as pq

def main():
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    pq_name = "output.parquet"

    os.system(f"wget {url} -O {pq_name}")
    engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{db}")
    # engine.connect()

    # q = """ 
    # select 1 as num
    # """
    # pd.read_sql(q, con=engine)


    trips = pq.read_table(pq_name)
    df = trips.to_pandas() #convert to dataframe

   #write data in postgres in a table 
    df.to_sql(name=table_name, con=engine,
                if_exists='delete') 
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='load data into postgres')


    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='table name where we would write the data')
    parser.add_argument('--url', required=True, help='url for dataset to download')
    
    args = parser.parse_args()

    main(args)
