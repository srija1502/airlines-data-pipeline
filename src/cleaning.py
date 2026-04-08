from pyspark.sql import functions as F
from src.spark import get_spark
from datetime import datetime

def clean_logs():
    spark = get_spark()
    run_date = datetime.now().strftime("%Y%m%d")

    df = spark.read.parquet("/opt/airflow/data/silver/stream_logs_normalized/")

    # Convert timestamp safely (handles Z and milliseconds)
    df = df.withColumn(
        "timestamp_parsed",
        F.to_timestamp("timestamp")
    )

    # Keep only if booking_id AND timestamp exist
    df = df.filter(
        (F.col("timestamp_parsed").isNotNull())
    )

    # Replace original timestamp
    df = df.withColumn("timestamp", F.col("timestamp_parsed")) \
           .drop("timestamp_parsed")

    # Optional: fill missing values instead of dropping
    df = df.fillna({
        "booking_id": "NULL",
        "airline": "NULL",
        "status": "NULL",
        "action": "NULL",
        "price_amount": 0
    })

    # Write parquet
    df.write.mode("overwrite").parquet("/opt/airflow/data/silver/stream_logs_clean/")

    # Write CSV
    df = df.withColumn("data_date", F.lit(run_date))

    df.coalesce(1).write.mode("append") \
        .partitionBy("data_date") \
        .option("header", True) \
        .csv("/opt/airflow/data/silver/stream_logs_clean_csv/")

    spark.stop()