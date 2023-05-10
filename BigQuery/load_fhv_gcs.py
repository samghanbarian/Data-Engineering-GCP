
from pathlib import Path
import os

from time import time
from datetime import timedelta
import pyarrow.parquet as pq

from prefect import flow,task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket




@task(retries = 3,log_prints=True, tags=["fetch"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(url:str,dataset_file):
    """ download the data and save locally"""
    
    # wget ${URL} -O ${LOCAL_PATH}
    path = Path(f"data/fhv/{dataset_file}.parquet")
    os.system(f"wget {url} -O {path}")
    return path


@task(retries=3, log_prints=True)
def write_to_gcs(path:Path) -> None:
    gcs_block = GcsBucket.load("gcs-ny-taxi")
    gcs_block.upload_from_path(path)



@flow(name="load_data")
def load_data_gcs(year,month):
    """ This is the main ETL function"""
    dataset_file =f"fhv_tripdata_{year}-{month:02}"
    dataset_url =f"https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{year}-{month}.parquet"
    
    path = fetch(dataset_url,dataset_file)
    write_to_gcs(path)
@flow()
def parent_flow_fhv(year,month) -> None:
    """ parent flow to set the params"""
    for month in month:
        load_data_gcs(year,month)

if __name__ == "__main__":
    parent_flow_fhv(year,month)