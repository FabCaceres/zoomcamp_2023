import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

#fhv_tripdata_2019-01.csv.gz
#esta es la estructura de la url para descargar un dataset.
#este etl se va a tener que modificar para pasar todas las columnas de pickup y dropof a columnas de tipo date.
@task(retries = 3)
def get_data(dataset_url : str) -> pd.DataFrame :

    df = pd.read_csv(dataset_url)
    return df

@task()
def clean_data(df):

    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
    df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])
    #todos estos son float
    df["PUlocationID"] = df["PUlocationID"].astype(float)
    df["DOlocationID"] = df["DOlocationID"].astype(float)
    df["SR_Flag"] = df["SR_Flag"].astype(float)

    return df
    



@task()
def write_local(df : pd.DataFrame, dataset_file : str, year : int, month:int) -> Path: 
    #vamos a escribir el archivo como un archivo parket 
    
    data_dir = f'fhv/{year}_{month}'
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    path = Path(f'{data_dir}/{dataset_file}.parquet')
    df.to_parquet(path, compression='gzip')
    return path

@task(log_prints =  True)
def write_to_gcs(path: str) -> None:
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    # el timeout sirve para aumentar el tiempo de espera, esto es especialmente para archivos pesados. De otra forma podria ser que el archivo no se suba y cause error ya que el tiempo de espera por defecto es de 60 segundos, si el archivo no se sube en ese tiempo puede dar problemas con gcs 
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path,timeout = 600)


@flow()
def load_data_to_gs(month,year):
    
    #nombre del dataset
    dataset_file = f'fhv_tripdata_{year}-{month:02}'
    #en esta url cambiamos el parametro "tag" por "download"
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz'
    print(dataset_file)
    print(dataset_url)
    df  = get_data(dataset_url)
    clean_dataframe = clean_data(df)
    #dataset_file trae el nombre del archivo, de esta forma se guardan sin que se guarden con el mismo nombre
    path = write_local(clean_dataframe,dataset_file,year,month)
    write_to_gcs(path)


@flow()
def parent_flow(year = 2019) -> None:

    for i in range(12):
        load_data_to_gs(i+1,year)




if __name__ == "__main__":
    
    parent_flow()



