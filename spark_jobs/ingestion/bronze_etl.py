from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date
import json, os, boto3

spark = SparkSession.builder \
    .appName("BronzeETL") \
    .master("local[2]") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

DATE = "2026-04-24"

s3 = boto3.client("s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin123",
)

# ── Convert JSON array → JSONL rồi upload ───────────────────────────
api_file = os.path.expanduser(f"~/airflow/data/bronze/posts_{DATE}.json")
if os.path.exists(api_file):
    with open(api_file) as f:
        records = json.load(f)  # đọc JSON array

    # Convert sang JSONL (mỗi record 1 dòng)
    jsonl_content = "\n".join(json.dumps(r) for r in records)
    s3.put_object(
        Bucket="lakehouse",
        Key=f"bronze/api/posts_{DATE}.jsonl",
        Body=jsonl_content.encode()
    )
    print(f"✅ Uploaded {len(records)} api records to MinIO")

# Upload CSV sales (đã là JSONL)
csv_file = os.path.expanduser(f"~/airflow/data/bronze/sales/sales_{DATE}.jsonl")
if os.path.exists(csv_file):
    s3.upload_file(csv_file, "lakehouse", f"bronze/csv/sales_{DATE}.jsonl")
    print(f"✅ Uploaded csv data to MinIO")

# Upload scraper (đã là JSONL)
scraper_file = os.path.expanduser(
    f"~/airflow/data/bronze/web_scraper/comments_{DATE}.jsonl")
if os.path.exists(scraper_file):
    s3.upload_file(scraper_file, "lakehouse",
                   f"bronze/scraper/comments_{DATE}.jsonl")
    print(f"✅ Uploaded scraper data to MinIO")

# ── Đọc từ MinIO ─────────────────────────────────────────────────────
print("\n── Reading from MinIO ──")

posts_df = spark.read.json("s3a://lakehouse/bronze/api/") \
    .withColumn("ingest_ts", current_timestamp()) \
    .withColumn("source", lit("api_ingest")) \
    .withColumn("ingest_date", to_date(lit(DATE))) \
    .cache()

print(f"API posts: {posts_df.count()} records")

sales_df = spark.read.json("s3a://lakehouse/bronze/csv/") \
    .withColumn("ingest_ts", current_timestamp()) \
    .withColumn("source", lit("csv_loader")) \
    .cache()

print(f"CSV sales: {sales_df.count()} records")

comments_df = spark.read.json("s3a://lakehouse/bronze/scraper/") \
    .withColumn("ingest_ts", current_timestamp()) \
    .withColumn("source", lit("web_scraper")) \
    .cache()

print(f"Scraper comments: {comments_df.count()} records")

# ── Ghi vào silver ───────────────────────────────────────────────────
print("\n── Writing to Silver (Parquet) ──")

posts_df.write.mode("overwrite").parquet("s3a://lakehouse/silver/posts/")
print("✅ Posts → silver/posts/")

sales_df.write.mode("overwrite").parquet("s3a://lakehouse/silver/sales/")
print("✅ Sales → silver/sales/")

comments_df.write.mode("overwrite").parquet("s3a://lakehouse/silver/comments/")
print("✅ Comments → silver/comments/")

print("\n✅ Bronze ETL hoàn tất!")
spark.stop()
