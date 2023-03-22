/*	
NOTES: The 'last_response_time' is currently tracking the time 
a pn manually reviewed and updated a response, when applicable. May
want to update the code so that it is tracking the time when the pt
texted their response as the 'last_response_time'
*/
DROP TABLE analyst.tm_responses_wide;
CREATE TABLE analyst.tm_responses_wide as


with tall_dat as (
SELECT 
    BUSINESS_KEY_ patient_cd, pd.NAME_ wf_name, un.*
FROM
    (SELECT 
        dt.PROC_INST_ID_,
            'ACT_HI_DETAIL' AS source_tbl,
            ai.ACT_ID_,
            NAME_,
            VAR_TYPE_,
            DOUBLE_, 
            LONG_, 
            TEXT_,
            TIME_ START_TIME_,
            TIME_ END_TIME_,
            ai.TRANSACTION_ORDER_,
            NULL AS DURATION_
    FROM
        ACT_HI_DETAIL dt 	
    LEFT JOIN (SELECT 
        PROC_INST_ID_, ID_, ACT_ID_, TRANSACTION_ORDER_
    FROM
        ACT_HI_ACTINST) ai ON ai.PROC_INST_ID_ = dt.PROC_INST_ID_
        AND ai.ID_ = dt.ACT_INST_ID_ UNION ALL SELECT 
        PROC_INST_ID_,
            'ACT_HI_ACTINST' AS source,
            ACT_ID_,
            ACT_NAME_ NAME_,
            NULL AS VAR_TYPE_,
            NULL AS DOUBLE_, 
            NULL AS LONG_, 
            NULL AS TEXT_,
            START_TIME_,
            END_TIME_,
            TRANSACTION_ORDER_,
            DURATION_
    FROM
        ACT_HI_ACTINST) un
        LEFT JOIN
    (SELECT 
        PROC_INST_ID_, BUSINESS_KEY_, PROC_DEF_ID_
    FROM
        ACT_HI_PROCINST) pi ON pi.PROC_INST_ID_ = un.PROC_INST_ID_
        LEFT JOIN
    (SELECT 
        ID_, NAME_
    FROM
        ACT_RE_PROCDEF) pd ON pd.ID_ = pi.PROC_DEF_ID_)
/* tall_dat above is redundant if copying over to tall dataset query */

, clarified_resp as (select 
	PROC_INST_ID_
    , 'manuallyClarifiedResponse' 	NAME_
    , START_TIME_					TIME_
    , TEXT_
	, 'manuallyClarifiedResponse'	textType
from tall_dat where NAME_ = 'pnResponse' AND ACT_ID_ in ('staffReviewTM1', 'staffReviewTMPlus', 'tm1AndMapsInvalidResponseSort', 'tmPlusInvalidResponseSort'))


, base as (SELECT
	PROC_INST_ID_
    , NAME_
    , TIME_
    , TEXT_ 
    , CASE
		WHEN NAME_ like 'tmPlusMapsMessage%'	THEN 'sentMAPSnotification'
		WHEN NAME_ regexp 'message[1-5]SentText' AND NAME_ != 'noContactMessage1SentText' THEN 'sentText'
        WHEN NAME_ regexp 'response[0-3]Text' THEN 'response'
        WHEN NAME_ = 'clarification1SentText' THEN 'sentClarfication'
        WHEN NAME_ = 'confirmation1SentText' THEN 'sentConfirmation'
        WHEN NAME_ = 'noContactMessage1SentText' THEN 'sentNoContact'
        END as textType 
    /* , if (NAME_ REGEXP 'response[0-3]Text', 'response', 'sentText') textType */
    FROM ACT_HI_DETAIL
	where (NAME_ REGEXP 'response[0-3]Text' /* Aside from the 3 pts with a bug, no pts had more than 3 responses per workflow*/
		OR NAME_ REGEXP 'SentText')
       -- /* just for testing, remove after*/ AND PROC_INST_ID_ = '26458214-384d-11ed-a7e6-005056be8d74' -- '0e853edb-3f6a-11ed-9f51-005056be8d74'
    order by PROC_INST_ID_, TIME_, NAME_
    )

-- select * from base order by PROC_INST_ID_, TIME_, NAME_ desc;
/*select distinct NAME_ from base;*/
/* select * from base;*/
-- select * from base;


/* 	• TM1andMAPS has second rand text message 'tm1AndMAPSMessage' labelled as 'message1SentText' which has the same label as the numbering of TMs (non MAPS notification TMs)
Need to re-name as  'tm1AndMapsMessage' */
, base_v2 as (select 
	base.PROC_INST_ID_
    , if( p4.TEXT_ = 'MAPS' AND base.NAME_ = 'message1SentText', 'tm1AndMapsMessage', base.NAME_) 		NAME_
    , base.TIME_
    , base.TEXT_
    , base.textType
from (select * from base) base
	left join
		( select * from ACT_HI_DETAIL where NAME_ = 'phase4RandomizationGroup') p4
	on p4.PROC_INST_ID_ = base.PROC_INST_ID_)

-- select * from base_v2;

, base_v3 as (select * from base_v2 union all select * FROM clarified_resp) 

-- select * from base_v3 order by PROC_INST_ID_, TIME_, NAME_ desc;


, msg_order as (SELECT 
	*
    , ROW_NUMBER() OVER( PARTITION BY PROC_INST_ID_ ORDER BY TIME_, FIELD(textType, 'response','sentClarification','sentConfirmation','sentNoContact','sentText','sentMAPSnotification'))	message_order 
    -- , ROW_NUMBER() OVER( PARTITION BY PROC_INST_ID_ ORDER BY TIME_, NAME_) 			message_order /* Saw cases where clarification#SentText had same timestamp as a response. Order should be that the response comes before the clarificationsenttext*/
    , ROW_NUMBER() OVER( PARTITION BY PROC_INST_ID_, textType ORDER BY TIME_, NAME_)	message_type_order
FROM base_v2
order by PROC_INST_ID_, textType, TIME_, NAME_)

/*select * from msg_order order by PROC_INST_ID_, message_order;*/
/*select NAME_ from msg_order group by NAME_;*/

/* select distinct message_order, count(1) n from msg_order group by message_order*/ /* The longest message order is 9*/
/*select distinct message_type_order, count(1) n from msg_order where textType = 'sentText' group by message_type_order*/  /* The most texts sent in 1 workflow is 6*/
/* select distinct message_order, count(1) n from msg_order where textType = 'response' group by message_order*/ /* As expected, a response text is never the first text message in the conversation. 
Responses range from msg 2 to 9*/

, wide_sent as (SELECT 
	mo.*
    , st1.TIME_					sentText1_time
    , st1.NAME_ 				sentText1_name
    , st1.message_order			sentText1_msg_order
    , st2.TIME_					sentText2_time
    , st2.NAME_ 				sentText2_name
    , st2.message_order			sentText2_msg_order	
    , st3.TIME_					sentText3_time
    , st3.NAME_ 				sentText3_name
    , st3.message_order			sentText3_msg_order
    , st4.TIME_					sentText4_time
    , st4.NAME_ 				sentText4_name
    , st4.message_order			sentText4_msg_order	
    , st5.TIME_					sentText5_time
    , st5.NAME_ 				sentText5_name
    , st5.message_order			sentText5_msg_order
FROM (SELECT * FROM msg_order) mo
LEFT JOIN 
	( SELECT * FROM msg_order where textType = 'sentText' and message_type_order = 1 ) st1
on st1.PROC_INST_ID_ = mo.PROC_INST_ID_
LEFT JOIN 
	( SELECT * FROM msg_order where textType = 'sentText' and message_type_order = 2 ) st2
on st2.PROC_INST_ID_ = mo.PROC_INST_ID_ 
LEFT JOIN 
	( SELECT * FROM msg_order where textType = 'sentText' and message_type_order = 3 ) st3
on st3.PROC_INST_ID_ = mo.PROC_INST_ID_
LEFT JOIN 
	( SELECT * FROM msg_order where textType = 'sentText' and message_type_order = 4 ) st4
on st4.PROC_INST_ID_ = mo.PROC_INST_ID_ 
LEFT JOIN 
	( SELECT * FROM msg_order where textType = 'sentText' and message_type_order = 5 ) st5
on st5.PROC_INST_ID_ = mo.PROC_INST_ID_)

-- SELECT * FROM wide_sent group by PROC_INST_ID_;
-- SELECT * FROM wide_sent order by PROC_INST_ID_, message_order;

, resp_matched as (SELECT 
	* 
	, CASE
		WHEN textType != 'sentText' AND sentText2_time is null THEN 1
		WHEN textType != 'sentText' AND TIME_ BETWEEN sentText1_time AND sentText2_time THEN 1
        WHEN textType != 'sentText' AND sentText3_time is null THEN 2
        WHEN textType != 'sentText' AND TIME_ BETWEEN sentText2_time AND sentText3_time THEN 2
        WHEN textType != 'sentText' AND sentText4_time is null THEN 3
        WHEN textType != 'sentText' AND TIME_ BETWEEN sentText3_time AND sentText4_time THEN 3
        WHEN textType != 'sentText' AND sentText5_time is null THEN 4
        WHEN textType != 'sentText' AND TIME_ BETWEEN sentText4_time AND sentText5_time THEN 4
        WHEN textType != 'sentText' AND TIME_ > sentText5_time THEN 5
	END as responding_to_sentText_num
        
FROM wide_sent
)

-- SELECT * FROM resp_matched order by PROC_INST_ID_, message_order;

, last_response_v1 as (SELECT 
	lr.PROC_INST_ID_
    , lr.NAME_
    , lr.TIME_
    , lr.TEXT_
    , lr.REV_
	, CASE
		WHEN  sentText2_time is null THEN 1
		WHEN  TIME_ BETWEEN sentText1_time AND sentText2_time THEN 1
        WHEN  sentText3_time is null THEN 2 
        WHEN  TIME_ BETWEEN sentText2_time AND sentText3_time THEN 2
        WHEN  sentText4_time is null THEN 3
        WHEN  TIME_ BETWEEN sentText3_time AND sentText4_time THEN 3
        WHEN  sentText5_time is null THEN 4
        WHEN  TIME_ BETWEEN sentText4_time AND sentText5_time THEN 4
        WHEN  TIME_ > sentText5_time THEN 5
        END AS sentText_num
	, st.* 
FROM 
	(SELECT 
		PROC_INST_ID_ PROC_INST_ID_dup, sentText1_time, sentText1_name, sentText1_msg_order, sentText2_time, sentText2_name, sentText2_msg_order, sentText3_time, sentText3_name, sentText3_msg_order
		, sentText4_time, sentText4_name, sentText4_msg_order, sentText5_time, sentText5_name, sentText5_msg_order
	FROM resp_matched group by PROC_INST_ID_) st
RIGHT JOIN 
	(SELECT * FROM ACT_HI_DETAIL WHERE NAME_ = 'lastResponse' and ACT_INST_ID_ is null)  lr
on lr.PROC_INST_ID_ = st.PROC_INST_ID_dup
/*where lr.PROC_INST_ID_ not in ('c5528b7a-d2d9-11ec-a852-005056be8d74', '668d2f59-e4f2-11ec-bbcf-005056be8d74', '1e23e8b7-d9f4-11ec-bcd7-005056be8d74')*/ /* @TB REMOVE THIS LATER */
)

-- SELECT * FROM last_response_v1 ORDER BY PROC_INST_ID_, sentText_num, REV_;

, matched_response_to_tm as (
SELECT 
	rm.*
    , if(responding_to_sentText_num is not null, ROW_NUMBER() OVER(PARTITION BY PROC_INST_ID_, responding_to_sentText_num ORDER BY TIME_), null) 	response_num_per_sentText
    , sto.message_type_order 				sent_text_num
    , sto.NAME_								text_message_name_responding_to   
FROM 
	(SELECT 
		PROC_INST_ID_
		, NAME_
		, TIME_
		, TEXT_
		, textType
		, message_order
		, message_type_order
		, responding_to_sentText_num
	FROM resp_matched) rm
LEFT JOIN
	( SELECT * from msg_order where textType = 'sentText') sto
on sto.PROC_INST_ID_ = rm.PROC_INST_ID_ AND sto.message_type_order = rm.responding_to_sentText_num
)

-- SELECT * FROM matched_response_to_tm ORDER BY PROC_INST_ID_, message_order;
 
, tm_w_resp as (SELECT 
	st.PROC_INST_ID_
    -- , st.NAME_
    , st.TIME_		parent_time
    , st.TEXT_		parent_text
    -- , st.textType /* PROBABLY DELETE THIS */
    , st.message_order		parent_message_order
    , st.message_type_order		parent_message_type_order
    , r1.textType		child1_textType
    , r1.TIME_			child1_time
    , r1.TEXT_			child1_text
    , r1.message_order	child1_message_order
    , r2.textType		child2_textType
    -- , r2.NAME_ 			response2_name  /* PROBABLY DELETE THIS */
    , r2.TIME_			child2_time
    , r2.TEXT_			child2_text
    , r2.message_order	child2_message_order
    , r3.textType		child3_textType
    -- , r3.NAME_ 			response3_name  /* PROBABLY DELETE THIS */
    , r3.TIME_			child3_time
    , r3.TEXT_			child3_text
    , r3.message_order	child3_message_order
    , r4.textType		child4_textType
    -- , r4.NAME_ 			response4_name  /* PROBABLY DELETE THIS */
    , r4.TIME_			child4_time
    , r4.TEXT_			child4_text
    , r4.message_order	child4_message_order
    , r5.textType		child5_textType
    -- , r5.NAME_ 			response5_name  /* PROBABLY DELETE THIS */
    , r5.TIME_			child5_time
    , r5.TEXT_			child5_text
    , r5.message_order	child5_message_order
FROM ( SELECT * FROM matched_response_to_tm WHERE textType = 'sentText' ) st
LEFT JOIN 
	( SELECT * FROM matched_response_to_tm WHERE textType != 'sentText' AND response_num_per_sentText = 1) r1
on r1.PROC_INST_ID_ = st.PROC_INST_ID_ AND r1.text_message_name_responding_to = st.NAME_
LEFT JOIN 
	( SELECT * FROM matched_response_to_tm WHERE textType != 'sentText' AND response_num_per_sentText = 2) r2
on r2.PROC_INST_ID_ = st.PROC_INST_ID_ AND r2.text_message_name_responding_to = st.NAME_
LEFT JOIN 
	( SELECT * FROM matched_response_to_tm WHERE textType != 'sentText' AND response_num_per_sentText = 3) r3
on r3.PROC_INST_ID_ = st.PROC_INST_ID_ AND r3.text_message_name_responding_to = st.NAME_
LEFT JOIN 
	( SELECT * FROM matched_response_to_tm WHERE textType != 'sentText' AND response_num_per_sentText = 4) r4
on r4.PROC_INST_ID_ = st.PROC_INST_ID_ AND r3.text_message_name_responding_to = st.NAME_
LEFT JOIN 
	( SELECT * FROM matched_response_to_tm WHERE textType != 'sentText' AND response_num_per_sentText = 5) r5
on r5.PROC_INST_ID_ = st.PROC_INST_ID_ AND r5.text_message_name_responding_to = st.NAME_)

-- SELECT * FROM tm_w_resp order by PROC_INST_ID_, message_order;

, last_responses as (SELECT * FROM (SELECT 
	*
    , MAX(REV_) OVER(PARTITION BY PROC_INST_ID_, sentText_num) as max_rev_per_sentText_num    
FROM
last_response_v1) sub
WHERE REV_ = max_rev_per_sentText_num group by PROC_INST_ID_, sentText_num, REV_)

-- SELECT * FROM last_responses;

, last_responses_v1 as ( SELECT 
	tms.* 
    , lr.TIME_		last_response_time
    , lr.TEXT_		last_response_text
FROM 
	(SELECT * FROM tm_w_resp) tms
LEFT JOIN
	( SELECT * FROM last_responses) lr
ON lr.PROC_INST_ID_ = tms.PROC_INST_ID_ AND lr.sentText_num = tms.parent_message_type_order)

-- SELECT * FROM last_responses_v1;

-- SELECT DISTINCT last_response_text, count(1) n FROM last_responses_v1 group by last_response_text    

, last_responses_v2 as (SELECT 
	*
    , if (last_response_text in ('NO', 'OPT-OUT', 'PARAR', 'SÍ', 'YES'), last_response_text, if(last_response_text is not null, 'OTHER', null))			last_response_categorical
FROM last_responses_v1)

SELECT 
	PROC_INST_ID_
    , parent_time
    , CONVERT(parent_text, CHAR(512))	parent_TEXT_
    , parent_message_order, parent_message_type_order, child1_textType, child1_time
    , CONVERT(child1_text, CHAR(256))		child1_text
    , child1_message_order, child2_textType, child2_time
    , CONVERT(child2_text, CHAR(256))		child2_text
    , child2_message_order, child3_textType, child3_time
    , CONVERT(child3_text, CHAR(256))		child3_text
    , child3_message_order, child4_textType, child4_time
    , CONVERT(child4_text, CHAR(256))		child4_text
    , child4_message_order, child5_textType, child5_time
    , CONVERT(child5_text, CHAR(256))		child5_text
    , child5_message_order, last_response_time
    , CONVERT(last_response_text, CHAR(256))	last_response_text
    , last_response_categorical	
FROM last_responses_v2;

-- SELECT * FROM last_responses_v2;
/*SELECT * FROM last_responses_v2;	*/

/* 	-- This can be used to add to the wide dataset for summary of TMs and responses	*/
/*select 
	PROC_INST_ID_, NAME_, TIME_ tm_sent_hrts_GMT, message_type_order, last_response_time, last_response_categorical
from last_responses_v2 order by PROC_INST_ID_, message_type_order */
;
