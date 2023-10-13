from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True, retries=1)
def extract_from_gcs(color,year, month)-> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path="./")
    return Path(f"./{gcs_path}")

@task(log_prints= True)
def write_bq(df: pd.DataFrame) -> None: 
    """ Write a DataFrame to BigQuery """
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table="dezoomcamp.rides2019",
        project_id="prefect-de-zoomcamp-401020",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow(log_prints = True)
def etl_gcs_to_bq(year: int, month: int, color: str) -> int:
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = pd.read_parquet(path)
    write_bq(df)
    df_rows = len(df)
    return df_rows


@flow(log_prints = True)
def parent_flow(
    months: list[int],
    year: int,
    color: str
)-> None:
    count_rows = 0
    for month in months:
        count_rows += etl_gcs_to_bq(year,month,color)
    print(f"Total Number of rows processed:{count_rows}")

if __name__ == "__main__":
    color = "yellow"
    year = "2019"
    months = [2,3]
    parent_flow(color, year, months)






