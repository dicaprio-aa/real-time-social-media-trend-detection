
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, window, count
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    TimestampType
)

# Create Spark session
spark = (
    SparkSession.builder
    .appName("RealTimeSocialTrendDetection")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Kafka message schema
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("text", StringType(), True),
    StructField("hashtags", ArrayType(StringType()), True)
])

print("Starting Spark Structured Streaming...")

# Read posts from Kafka
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "social-media-posts")
    .option("startingOffsets", "latest")
    .load()
)

# Parse JSON messages
posts = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_data")
    .select(from_json(col("json_data"), schema).alias("post"))
    .select("post.*")
    .withColumn("event_time", col("timestamp").cast(TimestampType()))
    .filter(col("event_time").isNotNull())
)

# Extract hashtags
hashtags = (
    posts
    .select(
        col("event_time"),
        explode(col("hashtags")).alias("hashtag")
    )
)

# Count hashtags in 30-second windows
trends = (
    hashtags
    .groupBy(
        window(col("event_time"), "30 seconds"),
        col("hashtag")
    )
    .agg(count("*").alias("post_count"))
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("hashtag"),
        col("post_count")
    )
)

# Save Spark results to JSON for the dashboard
OUTPUT_FILE = os.path.abspath("spark_trends.json")
TEMP_FILE = os.path.abspath("spark_trends.tmp")


def save_trends(batch_df, batch_id):
    rows = batch_df.collect()

    data = []
    for row in rows:
        data.append({
            "window_start": row["window_start"].isoformat(),
            "window_end": row["window_end"].isoformat(),
            "hashtag": row["hashtag"],
            "post_count": row["post_count"]
        })

    with open(TEMP_FILE, "w", encoding="utf-8") as file:
        json.dump(data, file)

    os.replace(TEMP_FILE, OUTPUT_FILE)

    print(f"Spark batch {batch_id}: saved {len(data)} trend rows")


query = (
    trends.writeStream
    .outputMode("complete")
    .foreachBatch(save_trends)
    .option("checkpointLocation", "spark-checkpoint")
    .start()
)

query.awaitTermination()
