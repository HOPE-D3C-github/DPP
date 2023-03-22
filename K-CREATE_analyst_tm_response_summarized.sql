use workflow;
DROP TABLE IF EXISTS analyst.K_tm_response_summarized;
CREATE TABLE analyst.K_tm_response_summarized

SELECT
	PROC_INST_ID_
    , parent_time	 			tm_sent_hrts_GMT
    , parent_message_order		 message_type_order
    , last_response_time, last_response_categorical
 FROM analyst.J_tm_responses_wide ORDER BY PROC_INST_ID_, message_type_order;