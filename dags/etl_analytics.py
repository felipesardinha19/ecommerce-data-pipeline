from airflow.decorators import dag, task
from datetime import datetime

from src.ingestion.ingestion import fetch_data, save_raw_data
from src.processing.transform import transform_data
from src.analytics.price_metrics import run_analytics
from src.storage.load import load_data


@dag(
    dag_id='etl_analytics',
    start_date=datetime(2024, 6, 1),
    schedule=None,
    catchup=False
)
def etl_analytics():

    @task
    def extract():
        data = fetch_data()
        return save_raw_data(data)
         

    @task
    def transform(file_path):
        return transform_data(file_path)

    @task
    def analytics():
        run_analytics()

    @task
    def load():
        load_data()

    path = extract()    
    transform(path) >> analytics() >> load()


etl_analytics()