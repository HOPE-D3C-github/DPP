use workflow;
DROP TABLE IF EXISTS analyst.M_mapsCalls_outcomes;
CREATE TABLE analyst.M_mapsCalls_outcomes as

with maps_per_msg_outcomes as (SELECT * FROM (SELECT 
	PROC_INST_ID_
    , maps_msg_number
    , call_count
    , TIME_
    , maps_outcome
    , MIN(TIME_) OVER ( PARTITION BY PROC_INST_ID_, maps_msg_number )	time_of_earliest_call_per_msg_number
    , MAX(TIME_) OVER ( PARTITION BY PROC_INST_ID_, maps_msg_number )	lastvalue_time
 FROM analyst.L_mapsCalls_tall) sub
	WHERE TIME_ = lastvalue_time)

SELECT * FROM maps_per_msg_outcomes ORDER BY PROC_INST_ID_, maps_msg_number;