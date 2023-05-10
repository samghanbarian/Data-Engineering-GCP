create an external table in bigquery from data in Gcs -URI: 'gs://[bucketname]/[file-path-in-bucket]'

CREATE OR REPLACE EXTERNAL TABLE `zeta-de-384407.ny_taxi_trips.external_yellow_rides`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_data_lake_zeta-de-384407/yellow_tripdata_2022-*.parquet','gs://de_data_lake_zeta-de-384407/yellow_tripdata_2023-*.parquet']
);