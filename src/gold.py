from pyspark.sql import functions as F
from src.spark import get_spark
from datetime import datetime

def airline_metrics():
    spark = get_spark()

    df = spark.read.parquet("/opt/airflow/data/silver/combined/")
    run_date = datetime.now().strftime("%Y%m%d")

    result = df.groupBy("airline").agg(
        F.count("*").alias("total_events"),
        F.sum("price_amount").alias("total_revenue")
    )

    result.write.mode("overwrite").parquet("/opt/airflow/data/gold/airline_metrics/")

    result = result.withColumn(
        "data_date",
        F.lit(run_date)
    )

    result.coalesce(1).write.mode("overwrite") \
    .partitionBy("data_date") \
    .option("header", True) \
    .csv("/opt/airflow/data/gold/airline_metrics_csv/")
    spark.stop()


def suspicious_events():
    spark = get_spark()

    df = spark.read.parquet("/opt/airflow/data/silver/combined/")

    run_date = datetime.now().strftime("%Y%m%d")

    result = df.withColumn(
        "issue_type",
        F.when(
            (F.col("action") == "REFUND") & (F.col("status") == "ON_TIME"),    # though flight status is on time user rquesting for refund might not be usual
            "Invalid Refund"
        ).when(
            (F.col("action") == "BOOKING_CONFIRMED") & (F.col("status") == "CANCELLED"),  # Sending booking confirmed to customer though the flight is cancelled
            "Booking Conflict"
        ).when(
            (F.col("action") == "REFUND") & (F.col("status") == "DELAYED"),  # Possibly fraud attempt
            "Suspicious Refund"
        ).when(
            (F.col("action") == "CANCEL_REQUEST") & (F.col("status") == "ON_TIME"),
            "Unusual Cancellation"
        )
    ).filter(F.col("issue_type").isNotNull())

    result.write.mode("overwrite").parquet("/opt/airflow/data/gold/suspicious/")

    result = result.withColumn(
        "data_date",
        F.lit(run_date)
    )

    result.coalesce(1).write.mode("overwrite") \
    .partitionBy("data_date") \
    .option("header", True) \
    .csv("/opt/airflow/data/gold/suspicious_csv/")
    spark.stop()


def high_value_customers():
    spark = get_spark()

    df = spark.read.parquet("/opt/airflow/data/silver/combined/")

    run_date = datetime.now().strftime("%Y%m%d")

    result = df.groupBy("customer_name", "tier").agg(
        F.sum("price_amount").alias("total_spent"),
        F.count("*").alias("events")
    ).withColumn(
        "priority_score",
        F.when(F.col("tier") == "PLATINUM", 100).otherwise(50) + F.col("total_spent")
    )

    result.write.mode("overwrite").parquet("/opt/airflow/data/gold/high_value/")

    result = result.withColumn(
        "data_date",
        F.lit(run_date)
    )

    result.coalesce(1).write.mode("overwrite") \
    .partitionBy("data_date") \
    .option("header", True) \
    .csv("/opt/airflow/data/gold/high_value_csv/")
    spark.stop()

def multiple_bookings_same_time():
    spark = get_spark()

    df = spark.read.parquet("/opt/airflow/data/silver/combined/")
    run_date = datetime.now().strftime("%Y%m%d")

    df = df.filter(F.col("action") == "BOOKING_CONFIRMED")\
        .filter(F.col("status").isin("ON_TIME", "DELAYED"))

    # Optional: normalize timestamp to minute level (avoid microseconds mismatch)
    df = df.withColumn(
        "event_time",
        F.date_format("timestamp", "yyyy-MM-dd")
    )

    result = df.groupBy("customer_name", "event_time").agg(
        F.countDistinct("booking_id").alias("booking_count"),
        F.collect_set("booking_id").alias("booking_ids")
    ).filter(F.col("booking_count") > 1)

    # Select clean output
    result = result.select(
        F.col("customer_name").alias("user_id"),
        "event_time",
        "booking_count",
        "booking_ids"
    )

    # parquet
    result.write.mode("overwrite").parquet("/opt/airflow/data/gold/multiple_bookings/")

    # csv
    result = result.withColumn("data_date", F.lit(run_date))

    result.coalesce(1).write.mode("overwrite") \
        .partitionBy("data_date") \
        .option("header", True) \
        .csv("/opt/airflow/data/gold/multiple_bookings_csv/")

    spark.stop()