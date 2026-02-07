### ✅Circuits Table (from meetings table):
- circuit_key
- circuit_short_name
- circuit_type
- circuit_info_url
- circuit_image
- year

### ✅ Countries Table (from meetings table):
- country_key
- country_code
- country_flag
- country_name
- year

### Locations Table (from meetings table):
- location_key (needs to be generated F.xxhash64("location_name")) 
- country_key
- location_name
- gmt_offset
- gmt_offset_sec

### ✅ Meetings Table:
- meeting_key
- country_key
- location_key
- meeting_name
- meeting_official_name
- local_ts_start
- local_ts_end
- ts_start
- ts_end
- year

### Sessions Table:
- session_key
- meeting_key
- country_key
- location_key
- session_name
- session_type
- local_ts_start
- local_ts_end
- ts_start
- ts_end
- year

### Teams Table:
- team_key (needs to be generated F.xxhash64("team_name"))
- team_name
- team_colour


### Drivers Table:
- driver_key (?)
- team_key
- meeting_key
- session_key
- first_name
- last_name
- name_acronym
- broadcast_name
- driver_number
- country_code (tenho de ver como vou fazer por causa da country table que ja existe)
- headshot_url
- year


### Session Result Table:
- session_result_key (?)
- meeting_key
- session_key
- driver_key
- dnf
- dns
- dsq
- duration
- gap_to_leader
- number_of_laps
- points
- position

### Laps Table:
- lap_key (?)
- meeting_key
- session_key
- driver_key
- lap_number
- date_start
- duration_sector_1
- duration_sector_2
- duration_sector_3
- i1_speed
- i2_speed
- is_pit_out_lap
- lap_duration
- segments_sector_1
- segments_sector_2
- segments_sector_3
- st_speed