from airflow.decorators import dag, task
from datetime import datetime

from src.ingestion import booking_ingestion, logs_ingestion
from src.staging import normalize_logs
from src.cleaning import clean_logs
from src.transform import join_data
from src.gold import (
    airline_metrics,
    suspicious_events,
    high_value_customers,
    multiple_bookings_same_time
)

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["airline", "pipeline"]
)
def airline_pipeline():

    @task
    def stream_logs_ingestion():
        logs_ingestion()

    @task
    def bookings_ingestion():
        booking_ingestion()

    @task
    def normalizing_stream_logs():
        normalize_logs()

    @task
    def cleaning_stream_logs():
        clean_logs()

    @task
    def join_streamslogs_bookings():
        join_data()

    @task
    def airline_metrics6():
        airline_metrics()

    @task
    def suspicious_events():
        suspicious_events()

    @task
    def priority_customers():
        high_value_customers()

    @task
    def multile_bookings():
        multiple_bookings_same_time

    # DAG FLOW
    logs = stream_logs_ingestion()
    bookings = bookings_ingestion()

    normalise = normalizing_stream_logs()
    clean = cleaning_stream_logs()
    joined = join_streamslogs_bookings()

    logs >> normalise >> clean >>  joined
    bookings >> joined

    joined >> [airline_metrics6(), suspicious_events(), priority_customers(), multile_bookings()]

dag = airline_pipeline()