from pyspark.sql import functions as F
from src.spark import get_spark
from datetime import datetime

def clean_logs():
    spark = get_spark()
    run_date = datetime.now().strftime("%Y%m%d")

    df = spark.read.parquet("/opt/airflow/data/silver/stream_logs_normalized/")

    df = df.withColumn(
        "timestamp_parsed",
        F.expr("try_to_timestamp(timestamp)")
    )

    bad_df = df.filter(F.col("timestamp_parsed").isNull())
    bad_df.write.mode("overwrite") \
        .parquet("/opt/airflow/data/silver/bad_records/")

    clean_df = df.filter(
        (F.col("timestamp_parsed").isNotNull())
    )

    clean_df = clean_df.withColumn("timestamp", F.col("timestamp_parsed")) \
           .drop("timestamp_parsed")
    clean_df_final = clean_df.fillna({
                "booking_id":"NULL",
                "airline": "NULL",
                "status": "NULL",
                "action": "NULL",
                "price_amount": 0
            })

    # Write parquet
    clean_df_final.write.mode("overwrite").parquet("/opt/airflow/data/silver/stream_logs_clean/")

    # Write CSV
    clean_df_final_csv = clean_df_final.withColumn("data_date", F.lit(run_date))

    clean_df_final_csv.coalesce(1).write.mode("overwrite") \
        .partitionBy("data_date") \
        .option("header", True) \
        .csv("/opt/airflow/data/silver/stream_logs_clean_csv/")

    spark.stop()