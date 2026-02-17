SELECT year, run_ts, count(*)
FROM delta.silver.drivers
group by 1,2
;