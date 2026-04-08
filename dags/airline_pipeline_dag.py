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
    def t1():
        logs_ingestion()

    @task
    def t2():
        booking_ingestion()

    @task
    def t3():
        normalize_logs()

    @task
    def t4():
        clean_logs()

    @task
    def t5():
        join_data()

    @task
    def t6():
        airline_metrics()

    @task
    def t7():
        suspicious_events()

    @task
    def t8():
        high_value_customers()

    @task
    def t9():
        multiple_bookings_same_time

    # DAG FLOW
    logs = t1()
    bookings = t2()

    norm = t3()
    clean = t4()
    joined = t5()

    logs >> norm >> clean >>  joined
    bookings >> joined

    joined >> [t6(), t7(), t8(), t9()]

dag = airline_pipeline()