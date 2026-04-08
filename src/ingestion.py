from pyspark.sql import functions as F
from src.spark import get_spark
from datetime import datetime

def logs_ingestion():
    spark = get_spark()
    df = spark.read.json("/opt/airflow/data/raw/stream_logs.jsonl")
    run_date = datetime.now().strftime("%Y%m%d")
    df.write.mode("overwrite").parquet("/opt/airflow/data/bronze/stream_logs/")

    df = df.withColumn(
            "data_date",
            F.lit(run_date)
        )
    
    df.coalesce(1).write.mode("overwrite") \
    .partitionBy("data_date") \
    .option("header", True) \
    .csv("/opt/airflow/data/bronze/stream_logs_csv/")
    spark.stop()

def booking_ingestion():
    spark = get_spark()
    df = spark.read.parquet("/opt/airflow/data/raw/bookings_master.parquet")

    df.write.mode("overwrite").parquet("/opt/airflow/data/bronze/bookings_master/")
    spark.stop()