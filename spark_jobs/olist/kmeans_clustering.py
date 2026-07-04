from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import col, count, avg, round as spark_round
import boto3, json

spark = SparkSession.builder \
    .appName("olist-kmeans") \
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

df = spark.read.parquet(f"{base}/gold/olist/customer_features/")
df = df.filter(
    col("total_orders").isNotNull() &
    col("total_spend").isNotNull() &
    col("avg_order_value").isNotNull() &
    col("avg_review_score").isNotNull()
)

print(f"customers: {df.count():,}")

feature_cols = [
    "total_orders",
    "total_spend",
    "avg_order_value",
    "avg_review_score",
    "cancel_rate",
    "unique_products",
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw", handleInvalid="skip")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=4, seed=42, maxIter=20)

pipeline = Pipeline(stages=[assembler, scaler, kmeans])
model = pipeline.fit(df)
predictions = model.transform(df)

evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster", metricName="silhouette")
silhouette = evaluator.evaluate(predictions)
print(f"silhouette: {silhouette:.4f}")

profiles = predictions.groupBy("cluster").agg(
    count("*").alias("customer_count"),
    spark_round(avg("total_orders"), 2).alias("avg_orders"),
    spark_round(avg("total_spend"), 2).alias("avg_spend"),
    spark_round(avg("avg_order_value"), 2).alias("avg_order_value"),
    spark_round(avg("avg_review_score"), 2).alias("avg_review"),
    spark_round(avg("cancel_rate"), 3).alias("avg_cancel_rate"),
    spark_round(avg("churn_label"), 3).alias("churn_rate"),
).orderBy("cluster")

profiles.show()

predictions.select(
    "customer_unique_id", "cluster",
    "total_orders", "total_spend",
    "avg_order_value", "avg_review_score",
    "cancel_rate", "unique_products", "churn_label"
).write.mode("overwrite").parquet(f"{base}/gold/olist/customer_segments/")

model.save(f"{base}/models/olist_kmeans_v1")

s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin123")

s3.put_object(
    Bucket="lakehouse",
    Key="models/kmeans_metrics.json",
    Body=json.dumps({
        "model": "KMeans",
        "k": 4,
        "silhouette": round(silhouette, 4),
        "features": feature_cols,
    }, indent=2).encode()
)

print("done")
spark.stop()
