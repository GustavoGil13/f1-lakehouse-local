# f1-lakehouse-local

## API Fetch order

1. Sessions filtered by Year (bronze_sessions_by_year.py)
- Gets every Session in a Year
- Contains session_key and meeting_key
2. Drivers filtered by Sessions meeting keys (bronze_from_sessions_meeting_keys.py)
- Gets drivers information in every meeting
- Contains session_key and meeting_key
3. Meetings filtered by Sessions meeting keys (bronze_from_sessions_meeting_keys.py)
- Gets meetings information
- Contains circuit_key, country_key, meeting_key
- Can build Silver Circuit and Country tables



## API Fetch order

1. Meetings filtered by Year (meetings_by_year.py)
- Refers to a GP Weekend that includes multiple sessions.
- Contains meeting_key, circuit_key and country_key
- Circuit and Country data can be useful for Silver Circuit and Country tables

2. Sessions filtered by Meetings meeting keys (bronze_ingestion_by_meeting_keys.py)
- Refers to a distinct period of track activity during a GP weekend (most common practice, qualifying and race)
- Contains session_key, meeting_key, circuit_key, country_key
- Even though Sessions contains circuit_key and country_key let's assume that Meetings is the Source of Truth

3. Drivers (bronze_ingestion_by_meeting_keys)
- Provides information about drivers for each session
- In a year, there are around 118 sessions distributed by 24 meetings
- Logically, Drivers should be filtered by Session session_key but doing so makes almost 5x more API calls (118/24)