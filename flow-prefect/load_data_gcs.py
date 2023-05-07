import pandas as pd
from pathlib import Path
import os

from time import time
from datetime import timedelta
import pyarrow.parquet as pq

from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
# from prefect_gcp import GcpCredentials



@task(retries =3)
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

@task(retries=3, log_prints=True)
def write_to_gcs(path:Path) -> None:
    """ writing the parquet file to gcs datalake"""
    
    # gcp_cred= GcpCredentials.load("gcp-cred")
    #loading the gcs-block from prefect
    gcs_block = GcsBucket.load("gcs-ny-taxi")
    gcs_block.upload_from_path(path)
    return


@flow(name="load_data")
def load_data_gcs():
    """ This is the main ETL function"""

    color= "yellow"
    month = 1
    year = 2023
    dataset_file =f"{color}_tripdata_{year}-{month:02}"
    dataset_url =f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    df = fetch (dataset_url)
    path = write_local_df(df,color,dataset_file)
    write_to_gcs(path)


if __name__ == "__main__":
    load_data_gcs()
