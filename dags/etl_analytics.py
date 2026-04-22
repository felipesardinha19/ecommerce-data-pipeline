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
    def analytics(file_path):
        return run_analytics(file_path)

    @task
    def load(file_dict):
        load_data(file_dict)

    raw = extract()
    trusted = transform(raw)
    analytics_file = analytics(trusted)
    load(analytics_file)


etl_analytics()