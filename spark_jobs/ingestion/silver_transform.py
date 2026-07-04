from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, when, isnan,
    to_timestamp, current_timestamp, lit, round as spark_round
)

spark = SparkSession.builder \
    .appName("SilverTransform") \
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

print("── Silver Transform: Posts ──")
posts_raw = spark.read.parquet("s3a://lakehouse/silver/posts/")

posts_clean = posts_raw \
    .withColumn("title", trim(col("title"))) \
    .withColumn("body",  trim(col("body"))) \
    .withColumn("userId", col("userId").cast("integer")) \
    .withColumn("id",     col("id").cast("integer")) \
    .filter(col("id").isNotNull()) \
    .filter(col("userId").isNotNull()) \
    .dropDuplicates(["id"]) \
    .withColumn("processed_at", current_timestamp())

print(f"Posts: {posts_raw.count()} raw → {posts_clean.count()} clean")
posts_clean.printSchema()

posts_clean.write.mode("overwrite") \
    .parquet("s3a://lakehouse/silver/posts_clean/")
print("✅ posts_clean → silver/posts_clean/")


print("\n── Silver Transform: Sales ──")
sales_raw = spark.read.parquet("s3a://lakehouse/silver/sales/")

sales_clean = sales_raw \
    .withColumn("order_id",  trim(col("order_id"))) \
    .withColumn("product",   trim(col("product"))) \
    .withColumn("status",    lower(trim(col("status")))) \
    .withColumn("amount",    col("amount").cast("double")) \
    .withColumn("user_id",   col("user_id").cast("integer")) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("amount") > 0) \
    .filter(col("status").isin(
        "completed", "pending", "cancelled")) \
    .dropDuplicates(["order_id"]) \
    .withColumn("processed_at", current_timestamp())

raw_count   = sales_raw.count()
clean_count = sales_clean.count()
print(f"Sales: {raw_count} raw → {clean_count} clean")
sales_clean.show(5)

sales_clean.write.mode("overwrite") \
    .parquet("s3a://lakehouse/silver/sales_clean/")
print("✅ sales_clean → silver/sales_clean/")


print("\n── Silver Transform: Comments ──")
comments_raw = spark.read.parquet("s3a://lakehouse/silver/comments/")

comments_clean = comments_raw \
    .withColumn("name",       trim(col("name"))) \
    .withColumn("email",      lower(trim(col("email")))) \
    .withColumn("body",       trim(col("body"))) \
    .withColumn("comment_id", col("comment_id").cast("integer")) \
    .withColumn("post_id",    col("post_id").cast("integer")) \
    .filter(col("email").isNotNull()) \
    .filter(col("email").contains("@")) \
    .filter(col("comment_id").isNotNull()) \
    .dropDuplicates(["comment_id"]) \
    .withColumn("processed_at", current_timestamp())

print(f"Comments: {comments_raw.count()} raw → {comments_clean.count()} clean")

comments_clean.write.mode("overwrite") \
    .parquet("s3a://lakehouse/silver/comments_clean/")
print("✅ comments_clean → silver/comments_clean/")


print("\n── Summary ──")
for table in ["posts_clean", "sales_clean", "comments_clean"]:
    df = spark.read.parquet(f"s3a://lakehouse/silver/{table}/")
    print(f"{table}: {df.count()} records, {len(df.columns)} columns")

print("\n✅ Silver Transform hoàn tất!")
spark.stop()
