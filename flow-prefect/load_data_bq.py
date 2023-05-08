import pandas as pd
from pathlib import Path


from time import time
from datetime import timedelta
# import pyarrow.parquet as pq

from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries = 3)
def extract_from_gcs(color:str,year:int,month:int) -> Path:
    """ extract data from bucket"""
    gcs_path = f"{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("gcs-ny-taxi")
    gcs_block.get_directory()
    return Path(gcs_path)
@task()
def convert_to_df(path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    return df

@task(retries=3)
def write_to_bq(df:pd.DataFrame,color:str,year:int) -> None:
    """ write the dataframe to bigquery using pandas"""
    gcp_cred= GcpCredentials.load("gcp-cred")
    df.to_gbq(
        destination_table = f"ny_taxi_trips.{color}_rides_{year}",
        project_id="zeta-de-384407",
        chunksize=500_000,
        reauth=False,
        if_exists='append',
        credentials=gcp_cred.get_credentials_from_service_account()
    )
@flow()
def load_data_bq() -> None:
    """ main etl flow to load data in Bigquery"""
    color = "yellow"
    year = 2022
    month = 1
    path = extract_from_gcs(color,year,month)
    df =convert_to_df(path)
    write_to_bq(df,color,year)


if __name__ == "__main__":
    load_data_bq()
