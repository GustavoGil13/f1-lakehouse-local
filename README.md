# f1-lakehouse-local

## Tools

MinIO + Spark + Great Expectations + Airflow

## API Fetch order / Bronze Tables

1. Meetings filtered by Year

```powershell
docker compose exec spark-master sh -lc '/opt/spark/bin/spark-submit \
  --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
  --conf spark.ui.showConsoleProgress=false \
  /opt/spark/jobs/bronze/bronze_ingestion_by_year.py \
  --endpoint meetings \
  --year 2023'
```

- Refers to a GP Weekend that includes multiple sessions.
- Contains meeting_key, circuit_key and country_key

2. Sessions filtered by Meetings meeting keys
```powershell
docker compose exec spark-master sh -lc '/opt/spark/bin/spark-submit \
  --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
  --conf spark.ui.showConsoleProgress=false \
  /opt/spark/jobs/bronze/bronze_ingestion_by_key.py \
  --source_endpoint meetings \
  --year 2023 \
  --key meeting_key \
  --target_endpoint sessions'
```

- Refers to a distinct period of track activity during a GP weekend (most common practice, qualifying and race)
- Contains session_key, meeting_key, circuit_key, country_key
- Even though Sessions contains circuit_key and country_key let's assume that Meetings is the Source of Truth

3. Drivers
```powershell
docker compose exec spark-master sh -lc '/opt/spark/bin/spark-submit \
  --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
  --conf spark.ui.showConsoleProgress=false \
  /opt/spark/jobs/bronze/bronze_ingestion_by_key.py \
  --source_endpoint meetings \
  --year 2023 \
  --key meeting_key \
  --target_endpoint drivers'
```

- Provides information about drivers for each session
- In a year, there are around 118 sessions distributed by 24 meetings
- Logically, Drivers should be filtered by Session session_key but doing so makes almost 5x more API calls (118/24)

4. Session Results filtered by Meetings meeting keys
```powershell
docker compose exec spark-master sh -lc '/opt/spark/bin/spark-submit \
  --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
  --conf spark.ui.showConsoleProgress=false \
  /opt/spark/jobs/bronze/bronze_ingestion_by_key.py \
  --source_endpoint meetings \
  --year 2023 \
  --key meeting_key \
  --target_endpoint session_result'
```

- Provides standings after a session
- Filtering by Meetings meeting keys retrieves the standings for all sessions within a year

5. Laps filtered by Sessions session_key and Sessions session_type different then "Practice"
```powershell
docker compose exec spark-master sh -lc '/opt/spark/bin/spark-submit \
  --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
  --conf spark.ui.showConsoleProgress=false \
  /opt/spark/jobs/bronze/bronze_ingestion_by_key.py \
  --source_endpoint sessions \
  --year 2023 \
  --key session_key \
  --target_endpoint laps \
  --target_endpoint_filter_column session_type \
  --target_endpoint_filter_operator ne \
  --target_endpoint_filter_value Practice'
```

- Provides detailed information about individual laps
- Only Qualifying, Sprint and Race results are going to be relevant in order to be efficient with the ingestion

## Silver Tables

1. Circuits
```powershell
docker compose exec spark-master sh -lc '/opt/spark/bin/spark-submit \
  --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
  --conf spark.ui.showConsoleProgress=false \
  /opt/spark/jobs/silver/circuits.py \
  --year 2023'
```
- Source: Meetings Bronze Table
- Schema:
  - circuit_key
  - circuit_short_name
  - circuit_type
  - circuit_info_url
  - circuit_image
  - year
  - run_ts
  - bronze_ingestion_ts: kept so we can easily check if the silver data is up to date
  - request_id: works as FK for Bronze Table


2. Countries
```powershell
docker compose exec spark-master sh -lc '/opt/spark/bin/spark-submit \
  --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
  --conf spark.ui.showConsoleProgress=false \
  /opt/spark/jobs/silver/countries.py \
  --year 2023'
```
- Source: Meetings Bronze Table


3. Locations
```powershell
docker compose exec spark-master sh -lc '/opt/spark/bin/spark-submit \
  --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
  --conf spark.ui.showConsoleProgress=false \
  /opt/spark/jobs/silver/locations.py \
  --year 2023'
```
- Source: Meetings Bronze Table

4. Meetings
```powershell
docker compose exec spark-master sh -lc '/opt/spark/bin/spark-submit \
  --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
  --conf spark.ui.showConsoleProgress=false \
  /opt/spark/jobs/silver/meetings.py \
  --year 2023'
```
- Source: Meetings Bronze Table

5. Sessions
```powershell
docker compose exec spark-master sh -lc '/opt/spark/bin/spark-submit \
  --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
  --conf spark.ui.showConsoleProgress=false \
  /opt/spark/jobs/silver/sessions.py \
  --year 2023'
```
- Source: Sessions Bronze Table


6. Teams
```powershell
docker compose exec spark-master sh -lc '/opt/spark/bin/spark-submit \
  --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
  --conf spark.ui.showConsoleProgress=false \
  /opt/spark/jobs/silver/teams.py \
  --year 2023'
```
- Source: Drivers Bronze Table


7. Drivers
```powershell
docker compose exec spark-master sh -lc '/opt/spark/bin/spark-submit \
  --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
  --conf spark.ui.showConsoleProgress=false \
  /opt/spark/jobs/silver/drivers.py \
  --year 2023'
```
- Source: Drivers Bronze Table


8. Drivers Sessions Association
```powershell
docker compose exec spark-master sh -lc '/opt/spark/bin/spark-submit \
  --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
  --conf spark.ui.showConsoleProgress=false \
  /opt/spark/jobs/silver/drivers_sessions_association.py \
  --year 2023'
```
- Source: Drivers Bronze Table


9. Session Result
```powershell
docker compose exec spark-master sh -lc '/opt/spark/bin/spark-submit \
  --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
  --conf spark.ui.showConsoleProgress=false \
  /opt/spark/jobs/silver/session_result.py \
  --year 2023'
```
- Source: Session Result Bronze Table


10. Laps
```powershell
docker compose exec spark-master sh -lc '/opt/spark/bin/spark-submit \
  --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
  --conf spark.ui.showConsoleProgress=false \
  /opt/spark/jobs/silver/laps.py \
  --year 2023'
```
- Source: Laps Bronze Table


### Data Quality Tests with GE
```powershell
docker compose exec spark-master sh -lc '/opt/spark/bin/spark-submit \
  --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
  --conf spark.ui.showConsoleProgress=false \
  /opt/spark/jobs/tests/dq_runner.py \
  --table ... \
  --year ...'
```
