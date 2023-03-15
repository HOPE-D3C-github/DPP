use workflow;
DROP TABLE IF EXISTS analyst.D_tm_response_summarized;
CREATE TABLE analyst.D_tm_response_summarized

SELECT
	PROC_INST_ID_
    , parent_time	 			tm_sent_hrts_GMT
    , parent_message_order		 message_type_order
    , last_response_time, last_response_categorical
 FROM analyst.C_tm_responses_wide ORDER BY PROC_INST_ID_, message_type_order;
   
/*select 
	PROC_INST_ID_, NAME_, TIME_ tm_sent_hrts_GMT, message_type_order, last_response_time, last_response_categorical
from analyst.tm_responses_wide order by PROC_INST_ID_, message_type_order;*/
