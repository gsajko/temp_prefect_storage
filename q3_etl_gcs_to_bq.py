from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("de-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def extract(path: Path) -> pd.DataFrame:
    """Read parquet file into DataFrame"""
    df = pd.read_parquet(path)
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("gcp-zoom-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="dotted-tube-376010",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow() #log total rows processed
def etl_gcs_to_bq(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = extract(path)
    write_bq(df)
    # how many rows were processed?
    return len(df)

@flow(log_prints=True)
def etl_parent_flow(
    months: list, year: int, color: str
):
    procc_rows = 0
    for month in months:
        procc_rows += etl_gcs_to_bq(year, month, color)
    print(f"Total rows processed: {procc_rows}")

if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    etl_parent_flow(months, year, color)