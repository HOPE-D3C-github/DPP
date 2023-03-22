/* Goal: Use timestamps of when we sent TMs, to match their responses to a respective TM */

WITH v1 as (SELECT m1.PROC_INST_ID_, m1_sent_hrts, m1_sent_text, m2_sent_hrts, m2_sent_text, m3_sent_hrts, m3_sent_text
	, m4_sent_hrts, m4_sent_text, m5_sent_hrts, m5_sent_text, resp.TIME_, resp.TEXT_
	FROM (SELECT PROC_INST_ID_, TIME_ m1_sent_hrts, TEXT_ m1_sent_text FROM ACT_HI_DETAIL where NAME_ = 'message1SentText') m1
	left join
		( select PROC_INST_ID_, NAME_, TIME_, TEXT_ FROM ACT_HI_DETAIL where NAME_ REGEXP 'response[0-9]Text') resp
	on resp.PROC_INST_ID_ = m1.PROC_INST_ID_    
    left join
		(SELECT PROC_INST_ID_, TIME_ m2_sent_hrts, TEXT_ m2_sent_text FROM ACT_HI_DETAIL where NAME_ = 'message2SentText') m2
	on m2.PROC_INST_ID_ = m1.PROC_INST_ID_
    left join
		(SELECT PROC_INST_ID_, TIME_ m3_sent_hrts, TEXT_ m3_sent_text FROM ACT_HI_DETAIL where NAME_ = 'message3SentText') m3
	on m3.PROC_INST_ID_ = m1.PROC_INST_ID_
    left join
		(SELECT PROC_INST_ID_, TIME_ m4_sent_hrts, TEXT_ m4_sent_text FROM ACT_HI_DETAIL where NAME_ = 'message4SentText') m4
	on m4.PROC_INST_ID_ = m1.PROC_INST_ID_
    left join
		(SELECT PROC_INST_ID_, TIME_ m5_sent_hrts, TEXT_ m5_sent_text FROM ACT_HI_DETAIL where NAME_ = 'message5SentText') m5
	on m5.PROC_INST_ID_ = m1.PROC_INST_ID_)

, v2 as (SELECT 
	*
    , CASE 
		WHEN (m2_sent_hrts is not null AND TIME_ BETWEEN m1_sent_hrts AND m2_sent_hrts) OR (m2_sent_hrts is null AND TIME_ > m1_sent_hrts)
			THEN 'message1'
		WHEN (m3_sent_hrts is not null AND TIME_ BETWEEN m2_sent_hrts AND m3_sent_hrts) OR (m3_sent_hrts is null AND TIME_ > m2_sent_hrts)
			THEN 'message2'
		WHEN (m4_sent_hrts is not null AND TIME_ BETWEEN m3_sent_hrts AND m4_sent_hrts) OR (m4_sent_hrts is null AND TIME_ > m3_sent_hrts)
			THEN 'message3'
		WHEN (m5_sent_hrts is not null AND TIME_ BETWEEN m4_sent_hrts AND m5_sent_hrts) OR (m5_sent_hrts is null AND TIME_ > m4_sent_hrts)
			THEN 'message4'
		WHEN TIME_ > m5_sent_hrts
			THEN 'message5'
		END AS tm_num_responding_to
	FROM v1 group by PROC_INST_ID_, tm_num_responding_to)
    

SELECT  
	ms.PROC_INST_ID_
    , m1_sent_hrts, m1_sent_text, m1_response_hrts, m1_response_text
    , m2_sent_hrts, m2_sent_text, m2_response_hrts, m2_response_text
    , m3_sent_hrts, m3_sent_text, m3_response_hrts, m3_response_text
    , m4_sent_hrts, m4_sent_text, m4_response_hrts, m4_response_text
    , m5_sent_hrts, m5_sent_text, m5_response_hrts, m5_response_text

	FROM 
	(SELECT PROC_INST_ID_, m1_sent_hrts, m1_sent_text, m2_sent_hrts, m2_sent_text, m3_sent_hrts, m3_sent_text, m4_sent_hrts, m4_sent_text, m5_sent_hrts, m5_sent_text
		FROM v2) ms
	left join 
		(SELECT PROC_INST_ID_, TIME_ m1_response_hrts, TEXT_ m1_response_text FROM v2
			WHERE tm_num_responding_to = 'message1') m1r
	on m1r.PROC_INST_ID_ = ms.PROC_INST_ID_
	left join 
		(SELECT PROC_INST_ID_, TIME_ m2_response_hrts, TEXT_ m2_response_text FROM v2
			WHERE tm_num_responding_to = 'message2') m2r
	on m2r.PROC_INST_ID_ = ms.PROC_INST_ID_
	left join 
		(SELECT PROC_INST_ID_, TIME_ m3_response_hrts, TEXT_ m3_response_text FROM v2
			WHERE tm_num_responding_to = 'message3') m3r
	on m3r.PROC_INST_ID_ = ms.PROC_INST_ID_
    left join 
		(SELECT PROC_INST_ID_, TIME_ m4_response_hrts, TEXT_ m4_response_text FROM v2
			WHERE tm_num_responding_to = 'message4') m4r
	on m4r.PROC_INST_ID_ = ms.PROC_INST_ID_
    left join 
		(SELECT PROC_INST_ID_, TIME_ m5_response_hrts, TEXT_ m5_response_text FROM v2
			WHERE tm_num_responding_to = 'message5') m5r
	on m5r.PROC_INST_ID_ = ms.PROC_INST_ID_

    ;