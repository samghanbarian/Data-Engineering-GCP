import pandas as pd
from pathlib import Path
import os

from time import time
from datetime import timedelta
import pyarrow.parquet as pq

from prefect import flow,task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket




@task(retries = 3,log_prints=True, tags=["fetch"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(url:str,path):
    """ download the data and turn into dataframe"""
    os.system(f"wget {url} -O {path}")
    return 



# @task(retries=3, log_prints=True)
# def write_to_gcs(path:Path) -> None:
#     gcs_block = GcsBucket.load("gcs-ny-taxi")
#     gcs_block.upload_from_path(path)



@flow(name="load_data")
def load_data_gcs(color,year,month):
    """ This is the main ETL function"""
    dataset_file =f"{color}_tripdata_{year}-{month:02}"
    dataset_url =f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02}.parquet"
    path = Path(f"data/{color}/{year}/{dataset_file}.parquet")
    fetch(dataset_url,path)
    # path = write_local_df(df,color,dataset_file)
    # write_to_gcs(path)
@flow()
def parent_flow_yg(color,year,month) -> None:
    """ parent flow to set the params"""

    load_data_gcs(color,year,month)

if __name__ == "__main__":
    color = ['yellow','green']
    year = [2022,2021]
    month = [1,2,3,4,5,6]
    for color in color:
        for year in year:
            for month in month:
                parent_flow_yg(color,year,month)