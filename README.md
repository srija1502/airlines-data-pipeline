# Data Engineering Challenge Disruption Recover
## Overview 
 
This project builds a robust data pipeline to process airline event streams and booking data,
enabling customer service teams to identify high-value customers and suspicious behaviors during disruptions.

## Phase A Production Audit (CLI) 
    Generates: 
        * Top 5 airlines by event count 
        * Total revenue per airline 
        
   > Run 'bash chmod +x phase_a.sh ./phase_a.sh data/raw/stream_logs.jsonl'
   > Output 'data/outputs/top_airlines_revenue_<timestamp>.csv'

## Phase B Airflow Pipeline ###
    Layers 
        * Bronze → Raw ingestion 
        * Silver → Cleaned & structured data 
        * Gold → Business insights 
        
    Gold Layer Outputs 
        * Airline metrics (events, revenue) 
        * High-value customers (priority scoring) 
        * Suspicious events (fraud/anomalies) 
        * Conflict detection 
        * Multiple bookings at same time 
        
    > First-time setup 'bash chmod +x run_pipeline.sh ./run_pipeline.sh'
    > Airflow UI 'http://localhost:8080' (Default credentials set during initialization)

## Design.md file has complete details and explanation regarding the project
