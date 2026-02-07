# f1-lakehouse-local

## Tools

MinIO + Spark + Great Expectations + airflow

## API Fetch order

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
- Circuit and Country data can be useful for Silver Circuit and Country tables

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