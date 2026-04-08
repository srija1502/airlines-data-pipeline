FROM apache/airflow:2.8.1-python3.10

USER root

# Install Java 17 (works with Spark)
RUN apt-get update && apt-get install -y openjdk-17-jdk && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install pyspark