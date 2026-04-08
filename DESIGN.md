
# AirLine Data Processing Pipeline (Phase A)

# Objective

Goal is to generate a high level audit of event strram to assess data health under strict system contraints.

1. Process raw airline log data which is in JSONL Format.
2. Perform minimal cleaning.
3. Handle inconsistent schema in price field.
4. Aggregations:
   4.1 Get the total events per airline.
   4.2 Get the total Revenue per airline.
5. Generate top 5 airlines per event report.
6. Save output as csv file.

# Key challenges
1. Memory Constraint (30MB): cannot use pandas or spark since this requires loading data to memory, but file is large.
2. Schema Inconsistency: The stream logs file contains **inconsistent structure**.
   price appears as both:
   1. **floating value**
   2. **nested object {amount,currency}**
This will **impact in breaking aggregations.**

# Strategies I used to mitigate Phase A:
1. To address these contraints, I implemented **streaming pipeline using jq, awk, sort**
2. **'jq'** will help to **transfrom difficult string parsing** into a **structured data format** which makes easy to manipulate.
3. Instead of processing all the data I selected only **airline, price objects** which helps **code to save memory** and **time for processing**.
4. Piped to **tsv (tab seperated values)** so that I can **use awk for faster manupulations**.
5. Used awk for aggregations since **it will automatically iterates over every input**.
6. Since the problem we are solving is to get the top 5 airlines I used **sort based on the events counts, in numeric and descending order**
    and used **head 5 to get the top 5 airlines**.
7. One of the challenges I faced while writing code is some of the prices are in object and some are in nrml key value, 
    for this I have used if else logic to fetch 'price.amount' if object else price.

# AirLine Data Processing Pipeline (Phase B)

# Overview:     
The system is reciving high volume of real time signals from airlines and internal user actions.

The challenge we are facing is the disconnection between the airline provided status signals and user behaviors
leading to ambiguity in customer side and potential misuse of system.

# Objective:      
Phase B evolves pipeline to a robust, scalable system using PySpark and Postgres.
The pipeline will **organise data into logical layers (stage, raw, gold)** to ensure **data quality, performance, business intelligence.**
Prioritizes high value customers during disruptions.

# Challenges Identified:                 
**1. Schema Inconsistency**
The stream logs file contains inconsistent structure.
Price appears as both:
    ->  floating value
    ->  nested object {amount,currency}
This will impact in **breaking downstream aggregations.**
**2. Noisy stream**
Observed **missing or null fields(booking_id,timestamp)**
timetsamp **date is in invalied format** for few records.
Requires Validations to ensure trust in analytics.
**3. Behavioral Ambiguity**
conflict between airline status and user actions.

# Arcitecture, Strategies, insights generation
**Arcitecture**
1. Created a pipeline that follows **multi layer data arcitecture:**
2. Created 3layers->
   1.  Bronze layer(**Raw Ingestion**) >
   2.  Silver layer (**Clean and structured**) >
   3.  Gold layer(**Aggregated metrics, conflict detection, suspicious behaviour**) 
**Strategy**
1. To maintain **schema consistency** and normalization
2. Handled **inconsistent price format->  extract structured values**.
3. **Regex parsing** for primitive values
4. Data Validation & cleaning
5. Replaced empty data with 'NULL' string which avoids data loss.
6. Filtered incorrect timestamps and seperated to another file

**Data transforming and business insight generation**
1. Joined both bookings dataframe and log streams dataframe to know tier, region of the booking.
2. Conflicts detected->
   1. User requesting REFUND while flight is ON_TIME
   2. Contradictory booking actions
   3. Fraud attempt possibilty, mistakenly opted refund on flight delays
   4. Same user booking multiple flights on the same time can also be implemented for business insight.
   5. Prioritizing customers tier level.

# Idempotency & Resilience               
1. Pipeline is designed safe for retries.
    1. Creates **datadate folder** and writes the files in **both csv(readability) and parquet(easy processing)**. new datadate folder created every day it gets triggered.
    2. writes **outputs using overwrite**, so that is we run the pipeline on **same date it will overwrite the file avoiding data duplication**.
    3. Each layer is independent
    4. Failures do not corrupt upstream data.
# Future Enhancements                    
1. Files are now places statically in a folder, it **can be enhanced dynamic file detection and run pieline**.
2. Handeling currency if currency is not given.
3. Provide **business insights based on timstamp** the latest booking id for the user.
4. Data quality monitoring
5. **Resuable utility functions** for common operations such as spark create session, standardize reads/writes and data transformations.
6. Loading all the layers data into a **standard schema defined tables (PostgresSQL)** which ensures **downstream systems to query actionable insights.**
7. Integrating **Phase A logic as airflow task**.
8. Identifying **suspicious users** who does **recursive status updates**.
