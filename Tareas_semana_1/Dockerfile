#vamos a poner todo lo que ocupamos en la imagen

FROM python:3.9 
RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_green_taxi.py ingest_green_taxi.py 

#forma de parametrizar nuestras pipelines de datos
ENTRYPOINT ["python","ingest_green_taxi.py"]
