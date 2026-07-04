from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    countDistinct, when, to_date, month, year, quarter, dayofweek,
    current_timestamp, round as spark_round
)

spark = SparkSession.builder \
    .appName("olist-silver-to-gold") \
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
src  = f"{base}/silver/olist"
dst  = f"{base}/gold/olist"

orders    = spark.read.parquet(f"{src}/orders_clean/")
items     = spark.read.parquet(f"{src}/order_items_clean/")
customers = spark.read.parquet(f"{src}/customers_clean/")
products  = spark.read.parquet(f"{src}/products_clean/")
payments  = spark.read.parquet(f"{src}/payments_clean/")
reviews   = spark.read.parquet(f"{src}/reviews_clean/")

payments_agg = payments.groupBy("order_id").agg(
    spark_sum("payment_value").alias("payment_total")
)

reviews_agg = reviews.groupBy("order_id").agg(
    avg("review_score").alias("avg_review_score")
)

# dim_date
dim_date = orders \
    .withColumn("date", to_date("order_purchase_timestamp")) \
    .select("date").distinct() \
    .withColumn("month", month("date")) \
    .withColumn("quarter", quarter("date")) \
    .withColumn("year", year("date")) \
    .withColumn("day_of_week", dayofweek("date")) \
    .withColumn("is_weekend", when(dayofweek("date").isin([1, 7]), True).otherwise(False)) \
    .filter(col("date").isNotNull())

dim_date.write.mode("overwrite").parquet(f"{dst}/dim_date/")
print(f"dim_date: {dim_date.count():,}")

# dim_customer
dim_customer = customers.select(
    "customer_id", "customer_unique_id", "customer_city", "customer_state"
)
dim_customer.write.mode("overwrite").parquet(f"{dst}/dim_customer/")
print(f"dim_customer: {dim_customer.count():,}")

# dim_product
dim_product = products.select(
    "product_id",
    col("product_category_name_english").alias("category"),
    "product_name_length",
    "product_weight_g",
    "product_photos_qty"
).fillna({"category": "unknown"})

dim_product.write.mode("overwrite").parquet(f"{dst}/dim_product/")
print(f"dim_product: {dim_product.count():,}")

# fact_sales
fact_sales = orders \
    .join(items, "order_id", "left") \
    .join(payments_agg, "order_id", "left") \
    .join(reviews_agg, "order_id", "left") \
    .join(customers.select("customer_id", "customer_state"), "customer_id", "left") \
    .select(
        "order_id", "customer_id", "product_id", "seller_id",
        "order_status",
        to_date("order_purchase_timestamp").alias("order_date"),
        col("price").cast("double"),
        col("freight_value").cast("double"),
        col("payment_total").cast("double"),
        col("avg_review_score").cast("double"),
        "customer_state"
    ) \
    .withColumn("processed_at", current_timestamp())

fact_sales.write.mode("overwrite").parquet(f"{dst}/fact_sales/")
print(f"fact_sales: {fact_sales.count():,}")

# customer_features (for clustering)
customer_features = orders \
    .join(items, "order_id", "left") \
    .join(payments_agg, "order_id", "left") \
    .join(reviews_agg, "order_id", "left") \
    .join(customers.select("customer_id", "customer_unique_id", "customer_state"), "customer_id", "left") \
    .groupBy("customer_unique_id") \
    .agg(
        count("order_id").alias("total_orders"),
        spark_round(spark_sum("payment_total"), 2).alias("total_spend"),
        spark_round(avg("payment_total"), 2).alias("avg_order_value"),
        spark_max("payment_total").alias("max_order_value"),
        countDistinct("product_id").alias("unique_products"),
        spark_round(avg("avg_review_score"), 2).alias("avg_review_score"),
        count(when(col("order_status") == "delivered", 1)).alias("delivered_orders"),
        count(when(col("order_status") == "canceled", 1)).alias("cancelled_orders"),
    ) \
    .withColumn("cancel_rate", spark_round(col("cancelled_orders") / col("total_orders"), 3)) \
    .withColumn("churn_label", when(col("total_orders") == 1, 1).otherwise(0)) \
    .withColumn("processed_at", current_timestamp())

customer_features.write.mode("overwrite").parquet(f"{dst}/customer_features/")
print(f"customer_features: {customer_features.count():,}")

# product_performance
product_perf = items \
    .join(dim_product.select("product_id", "category"), "product_id", "left") \
    .join(reviews_agg, "order_id", "left") \
    .groupBy("category") \
    .agg(
        count("order_id").alias("total_orders"),
        spark_round(spark_sum("price"), 2).alias("total_revenue"),
        spark_round(avg("price"), 2).alias("avg_price"),
        spark_round(avg("avg_review_score"), 2).alias("avg_review"),
        countDistinct("product_id").alias("unique_products"),
    ) \
    .filter(col("category").isNotNull()) \
    .withColumn("processed_at", current_timestamp())

product_perf.write.mode("overwrite").parquet(f"{dst}/product_performance/")
print(f"product_performance: {product_perf.count():,}")

# monthly_summary
monthly = orders \
    .join(payments_agg, "order_id", "left") \
    .withColumn("year", year("order_purchase_timestamp")) \
    .withColumn("month", month("order_purchase_timestamp")) \
    .groupBy("year", "month") \
    .agg(
        count("order_id").alias("total_orders"),
        spark_round(spark_sum("payment_total"), 2).alias("total_revenue"),
        spark_round(avg("payment_total"), 2).alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers"),
        count(when(col("order_status") == "delivered", 1)).alias("delivered_orders"),
    ) \
    .filter(col("year").isNotNull()) \
    .orderBy("year", "month") \
    .withColumn("processed_at", current_timestamp())

monthly.write.mode("overwrite").parquet(f"{dst}/monthly_summary/")
print(f"monthly_summary: {monthly.count():,}")

spark.stop()
