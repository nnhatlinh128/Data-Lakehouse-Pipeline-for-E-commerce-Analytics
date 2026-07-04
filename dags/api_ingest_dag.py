from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, json, os

default_args = {
    'owner': 'lakehouse',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def fetch_sample_data(**context):
    """
    Tạm thời dùng public API để test pipeline
    Sau này thay bằng API thật của project
    """
    response = requests.get(
        "https://jsonplaceholder.typicode.com/posts",
        timeout=30
    )
    data = response.json()

    # Lưu local (tạm thời, sau này upload lên MinIO)
    output_dir = os.path.expanduser("~/airflow/data/bronze")
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/posts_{context['ds']}.json"

    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"Saved {len(data)} records to {output_path}")
    return output_path

with DAG(
    'api_ingest',
    default_args=default_args,
    description='Ingest data from API to bronze layer',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ingestion', 'bronze'],
) as dag:

    ingest_task = PythonOperator(
        task_id='fetch_api_to_bronze',
        python_callable=fetch_sample_data,
        provide_context=True,
    )
