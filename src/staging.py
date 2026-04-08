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

    df = df.withColumn(
        "price_struct",
        F.from_json("price", price_schema)
    ).withColumn(
        "price_amount",
        F.coalesce(
            F.col("price_struct.amount"),
            F.col("price").cast("double"),
            F.regexp_replace("price", "[^0-9.]", "").cast("double")
        )
    ).withColumn(
        "currency",
        F.coalesce(F.col("price_struct.currency"), F.lit("USD"))
    ).drop("price_struct")

    df.write.mode("overwrite").parquet("/opt/airflow/data/silver/stream_logs_normalized/")

    df = df.withColumn("data_date", F.lit(run_date))

    df.coalesce(1).write.mode("overwrite") \
        .partitionBy("data_date") \
        .option("header", True) \
        .csv("/opt/airflow/data/silver/stream_logs_normalized_csv/")

    spark.stop()