import pandas as pd
from pathlib import Path
import os

from time import time
from datetime import timedelta
import pyarrow.parquet as pq

from prefect import flow,task
from prefect.tasks import task_input_hash



@task(retries = 3,log_prints=True, tags=["fetch"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(url:str) -> pd.DataFrame:
    """ download the data and turn into dataframe"""
    pq_name = "output.parquet"
    os.system(f"wget {url} -O {pq_name}")
    trips = pq.read_table(pq_name)
    df = trips.to_pandas() #convert to dataframe
    return df

@task()
def write_local_df(df:pd.DataFrame, color:str, dataset_file:str) -> Path:
    """write data frame locally as a parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path)
    return path

# @task(reteries=3, print_logs = True)
# def write_to_gcs(path:pathlib.Path) -> None:


@flow(name="load_data")
def load_data_gcs(color,year,month):
    """ This is the main ETL function"""
    dataset_file =f"{color}_tripdata_{year}-{month:02}"
    dataset_url =f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02}.parquet"
    df = fetch(dataset_url)
    path = write_local_df(df,color,dataset_file)
    #write_to_gcs(path)
@flow()
def parent_flow(color,year,month) -> None:
    """ parent flow to set the params"""
    for month in month:
        load_data_gcs(color,year,month)

if __name__ == "__main__":
    color = "yellow"
    year = 2022
    month = [1,2,3]
    parent_flow(color,year,month)