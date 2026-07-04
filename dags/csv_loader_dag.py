from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json, os

default_args = {'owner': 'lakehouse', 'retries': 3, 'retry_delay': timedelta(minutes=5)}

def create_sample_csv(**context):
    sample_dir = os.path.expanduser("~/airflow/data/input")
    os.makedirs(sample_dir, exist_ok=True)
    filepath = f"{sample_dir}/sales_{context['ds']}.csv"
    data = {
        'order_id':   [f"ORD{i:04d}" for i in range(1, 51)],
        'user_id':    [i % 10 + 1 for i in range(50)],
        'product':    ['Laptop','Phone','Tablet','Monitor','Keyboard'] * 10,
        'amount':     [round(100 + i * 15.5, 2) for i in range(50)],
        'order_date': [context['ds']] * 50,
        'status':     ['completed','pending','completed','cancelled','completed'] * 10,
    }
    pd.DataFrame(data).to_csv(filepath, index=False)
    print(f"Created sample CSV: {filepath} (50 rows)")
    return filepath

def validate_csv(**context):
    ti = context['ti']
    # Fallback nếu xcom_pull trả về None (khi test riêng lẻ)
    filepath = ti.xcom_pull(task_ids='create_sample_csv') or \
               os.path.expanduser(f"~/airflow/data/input/sales_{context['ds']}.csv")
    df = pd.read_csv(filepath)
    required_cols = ['order_id', 'user_id', 'product', 'amount', 'order_date']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    if (df['amount'] < 0).any():
        raise ValueError("Negative amount found")
    print(f"Validation passed: {len(df)} rows, total amount={df['amount'].sum():.2f}")
    return {'total_rows': len(df), 'total_amount': round(df['amount'].sum(), 2)}

def load_csv_to_bronze(**context):
    ti = context['ti']
    filepath = ti.xcom_pull(task_ids='create_sample_csv') or \
               os.path.expanduser(f"~/airflow/data/input/sales_{context['ds']}.csv")
    df = pd.read_csv(filepath)
    df['ingest_date'] = context['ds']
    df['source'] = 'csv_loader'
    output_dir = os.path.expanduser("~/airflow/data/bronze/sales")
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/sales_{context['ds']}.jsonl"
    with open(output_path, 'w') as f:
        for record in df.to_dict(orient='records'):
            f.write(json.dumps(record) + '\n')
    print(f"Loaded {len(df)} rows → {output_path}")
    return output_path

with DAG('csv_loader', default_args=default_args, description='Load CSV to bronze',
         schedule_interval='@weekly', start_date=datetime(2024, 1, 1),
         catchup=False, tags=['ingestion', 'bronze', 'csv']) as dag:
    t1 = PythonOperator(task_id='create_sample_csv', python_callable=create_sample_csv, provide_context=True)
    t2 = PythonOperator(task_id='validate_csv', python_callable=validate_csv, provide_context=True)
    t3 = PythonOperator(task_id='load_csv_to_bronze', python_callable=load_csv_to_bronze, provide_context=True)
    t1 >> t2 >> t3
