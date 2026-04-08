# Data Engineering Challenge Disruption Recover
## Overview 
 
This project builds a robust data pipeline to process airline event streams and booking data,
enabling customer service teams to identify high-value customers and suspicious behaviors during disruptions.

## Project Structure
dags/ 
    airline_pipeline_dag.py # Airflow DAG orchestration 
data/ 
    raw/ # Input data (jsonl, parquet) 
    bronze/ # Raw ingested data 
    silver/ # Cleaned & joined data 
    gold/ # Business-ready insights 
    outputs/ # Phase A outputs 
src/ 
    ingestion.py # Raw data ingestion 
    cleaning.py # Data cleaning & normalization 
    staging.py # Intermediate transformations 
    transform.py # Joins & enrichment 
    gold.py # Business logic & aggregations 
    spark.py # Spark session setup 
    phase_a.sh # CLI script for Phase A 
    run_pipeline.sh # Docker + Airflow runner 
    docker-compose.yaml # Local environment setup 
    Dockerfile # Custom Airflow image 
    DESIGN.md # Architecture & decisions
    
## Phase A Production Audit (CLI) 
    Generates: 
        * Top 5 airlines by event count 
        * Total revenue per airline 
        
    ### Run ```bash chmod +x phase_a.sh ./phase_a.sh data/raw/stream_logs.jsonl ``` 
    ### Output ``` data/outputs/top_airlines_revenue_<timestamp>.csv ```

## Phase B Airflow Pipeline ###

    Layers 
        * Bronze → Raw ingestion 
        * Silver → Cleaned & structured data 
        * Gold → Business insights 
        
    ## Gold Layer Outputs 
        * Airline metrics (events, revenue) 
        * High-value customers (priority scoring) 
        * Suspicious events (fraud/anomalies) 
        * Conflict detection 
        * Multiple bookings at same time 
    ## Run Pipeline
    ## First-time setup ```bash chmod +x run_pipeline.sh ./run_pipeline.sh fresh ``` 
    ## Airflow UI ``` http://localhost:8080 ``` (Default credentials set during initialization)



