#ocupamos crear un ETL que extraiga los datos de taxis verdes de en el mes de enero de 2020

import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.filesystems import GitHub


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # simulating failure to test retries
    # if randint(0, 1) == 1:
    #     raise Exception()
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame, color : str) -> pd.DataFrame:
    """Fix some dtype issues"""
    #se encarga de arreglar los valores de ciertas columnas para que no haya problemas al ingresarlo a la DB.
    
    if color == "yellow" :
    
        df['lpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    
    else: 
        #esto es debido a que estas columnas son diferentes en el DF de taxis verdes.
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    
    print(df.head(5))
    print(f'columns: {df.dtypes}')
    print(f'rows: {len(df)}')
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out as parquet file"""
    data_dir = f'data/{color}'
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    path = Path(f'{data_dir}/{dataset_file}.parquet')
    df.to_parquet(path, compression='gzip')
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)


@flow()
def etl_parent_flow(months = [1,2], year = 2021, color = "yellow"):

    for month in months:
        etl_web_to_gcs(month,year,color)

@flow()
def etl_web_to_gcs(month = 1, year= 2021 ,color = "yellow") -> None:
    """The main ETL function"""
    #con esta funcion podemos sacar varios tipos de datos de los archivos de los taxis. En este caso vamos a sacar informacion de taxis amarillos, del anio 2021 y del mes de enero.
    #color = 'green'
    #year = 2020
    #month = 1
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'
    df = fetch(dataset_url)
    df = clean(df, color)
    path = write_local(df, color, dataset_file)
    write_gcs(path)

if __name__ == '__main__':
    etl_parent_flow([1,2,3],2021, "green")
