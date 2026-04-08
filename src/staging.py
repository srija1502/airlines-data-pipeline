from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from src.spark import get_spark
from datetime import datetime

def normalize_logs():
    spark = get_spark()

    df = spark.read.parquet("/opt/airflow/data/bronze/stream_logs/")
    run_date = datetime.now().strftime("%Y%m%d")

    price_schema = StructType([
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True)
    ])

    df = df.withColumn("price_raw", F.col("price").cast("string"))
    df = df.withColumn("is_json", F.col("price_raw").contains("{"))
    df = df.withColumn(
        "price_amount",
        F.when(
            F.col("is_json"), 
            F.from_json("price_raw", price_schema).getItem("amount")
        ).otherwise(
            # Handles simple numeric strings directly
            F.col("price_raw").cast("double")
        )
    )
    staged_df = df.withColumn(
        "currency",
        F.when(
            F.col("is_json"), 
            F.from_json("price_raw", price_schema).getItem("currency")
        ).otherwise(F.lit("")) # Or your default currency
    )

    staged_df_final = staged_df.select(
        "action",
        "airline",
        "booking_id",
        "event_id",
        "status",
        "timestamp",
        "price_amount",
        "currency"
    )

    staged_df_final.write.mode("overwrite").parquet("/opt/airflow/data/silver/stream_logs_normalized/")
    staged_df_final_csv = staged_df_final.withColumn("data_date", F.lit(run_date))
    staged_df_final_csv.coalesce(1).write.mode("overwrite") \
        .partitionBy("data_date") \
        .option("header", True) \
        .csv("/opt/airflow/data/silver/stream_logs_normalized_csv/")

    spark.stop()