
#create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

from pathlib import Path
import pandas as pd
import os
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries = 1, log_prints = True)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pd DataFrame """
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df["tpep_pickup_datetime"])
    df['tpep_dropiff_datetime'] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    directory = color
    # Parent Directory path 
    parent_dir = "./data"
    # Path 
    path = os.path.join(parent_dir, directory) 
    if(not os.path.isdir(path)):
    # Create the directory ls
        os.mkdir(path) 
        print("Directory '%s' created" %directory) 

    path = Path(f"{path}/{dataset_file}.parquet")
    df.to_parquet(path, compression = "gzip")
    return path

@task()
def write_gcs(path: Path) -> None: 
    """ Uploading local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path = f"{path}",
        to_path = f"{path}"
    )
    return


@flow(log_prints=True)
def etl_web_to_gcs(
    month: int,
      year: int, 
      color: str
) -> None:
    """The main ETL function """
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow(log_prints = True)
def parent_flow(color: str, months: list[int], year:int) -> None:
    for month in months:
        etl_web_to_gcs(month, year, color)

if __name__ == '__main__':
    color = "yellow"
    months = [2,3]
    year = 2019 
    etl_parent_flow(months, year, color)