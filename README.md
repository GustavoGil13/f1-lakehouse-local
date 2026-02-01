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