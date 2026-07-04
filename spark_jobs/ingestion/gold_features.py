from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max,
    min as spark_min, countDistinct, when, lit,
    current_timestamp, round as spark_round
)

spark = SparkSession.builder \
    .appName("GoldFeatures") \
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

# ── Đọc silver data ──────────────────────────────────────────────────
posts    = spark.read.parquet("s3a://lakehouse/silver/posts_clean/")
sales    = spark.read.parquet("s3a://lakehouse/silver/sales_clean/")
comments = spark.read.parquet("s3a://lakehouse/silver/comments_clean/")

print(f"Silver data: {posts.count()} posts, "
      f"{sales.count()} sales, {comments.count()} comments")

# ── Gold 1: User Sales Features (cho ML churn prediction) ───────────
print("\n── Building: user_sales_features ──")

user_features = sales \
    .groupBy("user_id") \
    .agg(
        count("order_id")                          .alias("total_orders"),
        spark_sum("amount")                        .alias("total_spend"),
        spark_round(avg("amount"), 2)              .alias("avg_order_value"),
        spark_max("amount")                        .alias("max_order_value"),
        spark_min("amount")                        .alias("min_order_value"),
        count(when(col("status") == "completed", 1)).alias("completed_orders"),
        count(when(col("status") == "cancelled", 1)).alias("cancelled_orders"),
        count(when(col("status") == "pending",   1)).alias("pending_orders"),
        countDistinct("product")                   .alias("unique_products"),
    ) \
    .withColumn("cancel_rate",
        spark_round(col("cancelled_orders") / col("total_orders"), 2)) \
    .withColumn("completion_rate",
        spark_round(col("completed_orders") / col("total_orders"), 2)) \
    .withColumn("label",
        when(col("total_orders") < 3, 1).otherwise(0)) \
    .withColumn("feature_date", lit("2026-04-24")) \
    .withColumn("created_at", current_timestamp())

user_features.show(10)
print(f"User features: {user_features.count()} users")

user_features.write.mode("overwrite") \
    .parquet("s3a://lakehouse/gold/user_sales_features/")
print("✅ user_sales_features → gold/")

# ── Gold 2: Product Performance ──────────────────────────────────────
print("\n── Building: product_performance ──")

product_stats = sales \
    .groupBy("product") \
    .agg(
        count("order_id")                          .alias("total_orders"),
        spark_round(spark_sum("amount"), 2)        .alias("total_revenue"),
        spark_round(avg("amount"), 2)              .alias("avg_price"),
        count(when(col("status") == "completed", 1)).alias("completed"),
        count(when(col("status") == "cancelled", 1)).alias("cancelled"),
    ) \
    .withColumn("revenue_share",
        spark_round(
            col("total_revenue") / spark_sum("total_revenue").over(
                __import__("pyspark.sql.window", fromlist=["Window"])
                .Window.rowsBetween(
                    __import__("pyspark.sql.window", fromlist=["Window"])
                    .Window.unboundedPreceding,
                    __import__("pyspark.sql.window", fromlist=["Window"])
                    .Window.unboundedFollowing)
            ), 4)
    ) \
    .withColumn("created_at", current_timestamp())

product_stats.show()
print(f"Product stats: {product_stats.count()} products")

product_stats.write.mode("overwrite") \
    .parquet("s3a://lakehouse/gold/product_performance/")
print("✅ product_performance → gold/")

# ── Gold 3: Daily Summary (cho Superset dashboard) ───────────────────
print("\n── Building: daily_summary ──")

daily = sales \
    .groupBy("order_date") \
    .agg(
        count("order_id")                          .alias("total_orders"),
        spark_round(spark_sum("amount"), 2)        .alias("total_revenue"),
        spark_round(avg("amount"), 2)              .alias("avg_order_value"),
        countDistinct("user_id")                   .alias("unique_customers"),
        count(when(col("status") == "completed", 1)).alias("completed_orders"),
        count(when(col("status") == "cancelled", 1)).alias("cancelled_orders"),
    ) \
    .withColumn("created_at", current_timestamp())

daily.show()

daily.write.mode("overwrite") \
    .parquet("s3a://lakehouse/gold/daily_summary/")
print("✅ daily_summary → gold/")

# ── Final Summary ─────────────────────────────────────────────────────
print("\n── Gold Layer Summary ──")
for table in ["user_sales_features", "product_performance", "daily_summary"]:
    df = spark.read.parquet(f"s3a://lakehouse/gold/{table}/")
    print(f"{table}: {df.count()} records, {len(df.columns)} columns")

print("\n✅ Gold Features hoàn tất!")
spark.stop()
