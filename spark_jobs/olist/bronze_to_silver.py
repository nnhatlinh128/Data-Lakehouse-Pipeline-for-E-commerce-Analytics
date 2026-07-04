from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, trim, lower, when, avg, current_timestamp

spark = SparkSession.builder \
    .appName("olist-bronze-to-silver") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

base = "s3a://lakehouse"
src  = f"{base}/bronze/olist"
dst  = f"{base}/silver/olist"


def read_csv(name):
    return spark.read.option("header", True).csv(f"{src}/{name}")


orders    = read_csv("olist_orders_dataset.csv")
items     = read_csv("olist_order_items_dataset.csv")
customers = read_csv("olist_customers_dataset.csv")
products  = read_csv("olist_products_dataset.csv")
payments  = read_csv("olist_order_payments_dataset.csv")
reviews   = read_csv("olist_order_reviews_dataset.csv")
categories = read_csv("product_category_name_translation.csv")

print(f"orders: {orders.count():,}")
print(f"items: {items.count():,}")
print(f"customers: {customers.count():,}")

orders_clean = orders \
    .withColumn("order_purchase_timestamp", to_timestamp("order_purchase_timestamp")) \
    .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date")) \
    .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date")) \
    .withColumn("order_status", trim(lower(col("order_status")))) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("customer_id").isNotNull()) \
    .dropDuplicates(["order_id"]) \
    .withColumn("processed_at", current_timestamp())

orders_clean.write.mode("overwrite").parquet(f"{dst}/orders_clean/")
print(f"orders_clean: {orders_clean.count():,}")

items_clean = items \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("freight_value", col("freight_value").cast("double")) \
    .withColumn("order_item_id", col("order_item_id").cast("integer")) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("price") > 0) \
    .withColumn("total_value", col("price") + col("freight_value")) \
    .withColumn("processed_at", current_timestamp())

items_clean.write.mode("overwrite").parquet(f"{dst}/order_items_clean/")
print(f"items_clean: {items_clean.count():,}")

customers_clean = customers \
    .withColumn("customer_city", trim(lower(col("customer_city")))) \
    .withColumn("customer_state", trim(col("customer_state"))) \
    .filter(col("customer_id").isNotNull()) \
    .dropDuplicates(["customer_id"]) \
    .withColumn("processed_at", current_timestamp())

customers_clean.write.mode("overwrite").parquet(f"{dst}/customers_clean/")
print(f"customers_clean: {customers_clean.count():,}")

products_clean = products \
    .join(categories, "product_category_name", "left") \
    .withColumn("product_name_length", col("product_name_lenght").cast("integer")) \
    .withColumn("product_weight_g", col("product_weight_g").cast("double")) \
    .withColumn("product_photos_qty", col("product_photos_qty").cast("integer")) \
    .filter(col("product_id").isNotNull()) \
    .dropDuplicates(["product_id"]) \
    .withColumn("processed_at", current_timestamp())

products_clean.write.mode("overwrite").parquet(f"{dst}/products_clean/")
print(f"products_clean: {products_clean.count():,}")

payments_clean = payments \
    .withColumn("payment_value", col("payment_value").cast("double")) \
    .withColumn("payment_installments", col("payment_installments").cast("integer")) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("payment_value") > 0) \
    .withColumn("processed_at", current_timestamp())

payments_clean.write.mode("overwrite").parquet(f"{dst}/payments_clean/")
print(f"payments_clean: {payments_clean.count():,}")

reviews_clean = reviews \
    .withColumn("review_score", col("review_score").cast("integer")) \
    .withColumn("review_creation_date", to_timestamp("review_creation_date")) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("review_score").isNotNull()) \
    .dropDuplicates(["review_id"]) \
    .withColumn("processed_at", current_timestamp())

reviews_clean.write.mode("overwrite").parquet(f"{dst}/reviews_clean/")
print(f"reviews_clean: {reviews_clean.count():,}")

spark.stop()
