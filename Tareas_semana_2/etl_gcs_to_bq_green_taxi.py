from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    #esta es la estructura que siguen nuestros datos almacenados en GCS. La sintaxis usada en month se usa para indicar que el tamano del string es de solo 2 digitos. Nota, los metodos del bucket son de un bloque personalizado.
    """"
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")
    """
    gcs_path = f"data\{color}\{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"..\data\{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    #en este caso solo se hace la transformacion de un path a un Df, pero no se hace ninguna limpieza de datos.
    df = pd.read_parquet(path)
    #print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    #df["passenger_count"].fillna(0, inplace=True)
    #print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame, table_name : str) -> None:
    """Write DataFrame to BiqQuery"""

    #estas son las credenciales que creamos y que tienen acceso a BQ. Aca las estamos "cargando" y mas adelante ya vamos a hacer el llamdo a la funcion que consulta a dichas credenciales
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    #esta funcion ya esta integrada en pandas. Las configuraciones se sacan del "Archivo" De BQ que creamos en GCP. Las credenciales son las mismas qye ya habiamos creado.
    df.to_gbq(
        destination_table=table_name,
        project_id="prefect-de-zoomcamp-382123",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@flow()
def etl_parent_flow(months = [1,2], year = 2021, color = "yellow"):

    for month in months:
        etl_gcs_to_bq(month,year,color)

@flow()
def etl_gcs_to_bq(month = 1,year = 2021,color = "green"):
    """Main ETL flow to load data into Big Query"""
    #color = "yellow"
    #year = 2021
    #month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df,"de_zoomcamp.rides_yellow")


if __name__ == "__main__":
    etl_parent_flow(1, 2021,"green")