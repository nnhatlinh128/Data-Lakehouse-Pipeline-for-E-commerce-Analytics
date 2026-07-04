from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, json, os

default_args = {'owner': 'lakehouse', 'retries': 2, 'retry_delay': timedelta(minutes=10)}

def _fetch_data():
    """Helper: fetch data từ API, dùng cho cả scrape và fallback."""
    scraped = []
    for page in range(1, 4):
        response = requests.get(
            "https://jsonplaceholder.typicode.com/comments",
            params={'_page': page, '_limit': 20},
            timeout=30,
        )
        response.raise_for_status()
        for item in response.json():
            scraped.append({
                'post_id':    item.get('postId'),
                'comment_id': item.get('id'),
                'name':       item.get('name', '').strip(),
                'email':      item.get('email', '').strip(),
                'body':       item.get('body', '').strip()[:200],
                'page':       page,
            })
        print(f"Scraped page {page}: {len(response.json())} items")
    return scraped

def scrape_website(**context):
    scraped = _fetch_data()
    for item in scraped:
        item['scraped_date'] = context['ds']
        item['source_url'] = f"https://example.com/page/{item['page']}"
    print(f"Total scraped: {len(scraped)} records")
    return scraped

def clean_scraped_data(**context):
    ti = context['ti']
    raw_data = ti.xcom_pull(task_ids='scrape_website')

    # Fallback: re-fetch nếu XCom trả về None (khi test riêng lẻ)
    if raw_data is None:
        print("XCom returned None — re-fetching data as fallback")
        raw_data = _fetch_data()
        for item in raw_data:
            item['scraped_date'] = context['ds']
            item['source_url'] = f"https://example.com/page/{item['page']}"

    seen_ids, cleaned = set(), []
    for item in raw_data:
        if item['comment_id'] in seen_ids or not item.get('email'):
            continue
        seen_ids.add(item['comment_id'])
        item['email'] = item['email'].lower()
        cleaned.append(item)

    print(f"Cleaned: {len(cleaned)} records (removed {len(raw_data) - len(cleaned)} duplicates/invalids)")
    return cleaned

def save_to_bronze(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='clean_scraped_data')

    # Fallback nếu XCom trả về None
    if data is None:
        print("XCom returned None — re-fetching and cleaning as fallback")
        raw = _fetch_data()
        seen_ids, data = set(), []
        for item in raw:
            if item['comment_id'] in seen_ids or not item.get('email'):
                continue
            seen_ids.add(item['comment_id'])
            item['email'] = item['email'].lower()
            item['scraped_date'] = context['ds']
            data.append(item)

    output_dir = os.path.expanduser("~/airflow/data/bronze/web_scraper")
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/comments_{context['ds']}.jsonl"
    with open(output_path, 'w') as f:
        for record in data:
            f.write(json.dumps(record) + '\n')
    print(f"Saved {len(data)} records → {output_path}")
    return output_path

with DAG('web_scraper', default_args=default_args, description='Scrape web to bronze',
         schedule_interval='0 */4 * * *', start_date=datetime(2024, 1, 1),
         catchup=False, tags=['ingestion', 'bronze', 'scraper']) as dag:
    t1 = PythonOperator(task_id='scrape_website', python_callable=scrape_website, provide_context=True)
    t2 = PythonOperator(task_id='clean_scraped_data', python_callable=clean_scraped_data, provide_context=True)
    t3 = PythonOperator(task_id='save_to_bronze', python_callable=save_to_bronze, provide_context=True)
    t1 >> t2 >> t3
