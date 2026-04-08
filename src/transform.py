from src.spark import get_spark
from pyspark.sql import functions as F
from datetime import datetime


def join_data():
    spark = get_spark()

    logs = spark.read.parquet("/opt/airflow/data/silver/stream_logs_clean/")
    bookings = spark.read.parquet("/opt/airflow/data/bronze/bookings_master/")

    run_date = datetime.now().strftime("%Y%m%d")
    combined_df = logs.join(bookings, "booking_id", "left")

    combined_df.write.mode("overwrite").parquet("/opt/airflow/data/silver/combined/")

    combined_df_csv = combined_df.withColumn(
            "data_date",
            F.lit(run_date)
        )

    combined_df_csv.coalesce(1).write.mode("overwrite") \
    .partitionBy("data_date") \
    .option("header", True) \
    .csv("/opt/airflow/data/silver/combined_csv/")
    spark.stop()