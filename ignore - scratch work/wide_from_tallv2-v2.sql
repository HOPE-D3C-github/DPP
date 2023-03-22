use workflow;

/*create view analyst.wide_test as */
with tall_dat as (SELECT 
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
        ACT_RE_PROCDEF) pd ON pd.ID_ = pi.PROC_DEF_ID_
ORDER BY patient_cd , START_TIME_ , END_TIME_ , TRANSACTION_ORDER_ , source_tbl)

/*  Uncomment this out later  
, base as (SELECT
	PROC_INST_ID_
    , NAME_
    , TIME_
    , TEXT_ 
    , if (NAME_ REGEXP 'response[0-3]Text', 'response', 'sentText') textType
    FROM ACT_HI_DETAIL
	where (NAME_ REGEXP 'response[0-3]Text' 
		OR NAME_ REGEXP 'SentText')
       
    order by PROC_INST_ID_, TIME_, NAME_
    )

-- TM1andMAPS has second rand text message 'tm1AndMAPSMessage' labelled as 'message1SentText' which has the same label as the numbering of TMs (non MAPS notification TMs). Need to re-name as  'tm1AndMapsMessage' 
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

, msg_order as (SELECT 
	*
    , ROW_NUMBER() OVER( PARTITION BY PROC_INST_ID_ ORDER BY TIME_, textType, NAME_) 			message_order -- Saw cases where clarification#SentText had same timestamp as a response. Order should be that the response comes before the clarificationsenttext
    , ROW_NUMBER() OVER( PARTITION BY PROC_INST_ID_, textType ORDER BY TIME_, NAME_)	message_type_order
FROM base_v2
order by PROC_INST_ID_, textType, TIME_, NAME_)

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
    , st6.TIME_					sentText6_time
    , st6.NAME_ 				sentText6_name
    , st6.message_order			sentText6_msg_order	
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
on st5.PROC_INST_ID_ = mo.PROC_INST_ID_
LEFT JOIN 
	( SELECT * FROM msg_order where textType = 'sentText' and message_type_order = 6 ) st6
on st6.PROC_INST_ID_ = mo.PROC_INST_ID_ )

, resp_matched as (SELECT 
	* 
	, CASE
		WHEN textType = 'response' AND sentText2_time is null THEN 1
		WHEN textType = 'response' AND TIME_ BETWEEN sentText1_time AND sentText2_time THEN 1
        WHEN textType = 'response' AND sentText3_time is null THEN 2
        WHEN textType = 'response' AND TIME_ BETWEEN sentText2_time AND sentText3_time THEN 2
        WHEN textType = 'response' AND sentText4_time is null THEN 3
        WHEN textType = 'response' AND TIME_ BETWEEN sentText3_time AND sentText4_time THEN 3
        WHEN textType = 'response' AND sentText5_time is null THEN 4
        WHEN textType = 'response' AND TIME_ BETWEEN sentText4_time AND sentText5_time THEN 4
        WHEN textType = 'response' AND sentText6_time is null THEN 5
        WHEN textType = 'response' AND TIME_ BETWEEN sentText5_time AND sentText6_time THEN 5
        WHEN textType = 'response' AND TIME_ > sentText6_time THEN 6
	END as responding_to_sentText_num
        
FROM wide_sent
)

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
        WHEN  sentText6_time is null THEN 5 
        WHEN  TIME_ BETWEEN sentText5_time AND sentText6_time THEN 5
        WHEN  TIME_ > sentText6_time THEN 6
        END AS sentText_num
	, st.* 
FROM 
	(SELECT 
		PROC_INST_ID_ PROC_INST_ID_dup, sentText1_time, sentText1_name, sentText1_msg_order, sentText2_time, sentText2_name, sentText2_msg_order, sentText3_time, sentText3_name, sentText3_msg_order
		, sentText4_time, sentText4_name, sentText4_msg_order, sentText5_time, sentText5_name, sentText5_msg_order, sentText6_time, sentText6_name, sentText6_msg_order
	FROM resp_matched group by PROC_INST_ID_) st
RIGHT JOIN 
	(SELECT * FROM ACT_HI_DETAIL WHERE NAME_ = 'lastResponse' and ACT_INST_ID_ is null)  lr
on lr.PROC_INST_ID_ = st.PROC_INST_ID_dup)

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
ORDER BY PROC_INST_ID_, message_order)

, tm_w_resp as (SELECT 
	st.PROC_INST_ID_
    , st.NAME_
    , st.TIME_
    , st.TEXT_
    , st.textType -- PROBABLY DELETE THIS 
    , st.message_order
    , st.message_type_order
    , r1.NAME_ 			response1_name  -- PROBABLY DELETE THIS 
    , r1.TIME_			response1_time
    , r1.TEXT_			response1_text
    , r1.message_order	response1_message_order
    , r2.NAME_ 			response2_name  -- PROBABLY DELETE THIS 
    , r2.TIME_			response2_time
    , r2.TEXT_			response2_text
    , r2.message_order	response2_message_order
FROM ( SELECT * FROM matched_response_to_tm WHERE textType = 'sentText' ) st
LEFT JOIN 
	( SELECT * FROM matched_response_to_tm WHERE textType = 'response' AND response_num_per_sentText = 1) r1
on r1.PROC_INST_ID_ = st.PROC_INST_ID_ AND r1.text_message_name_responding_to = st.NAME_
LEFT JOIN 
	( SELECT * FROM matched_response_to_tm WHERE textType = 'response' AND response_num_per_sentText = 2) r2
on r2.PROC_INST_ID_ = st.PROC_INST_ID_ AND r2.text_message_name_responding_to = st.NAME_)

, last_responses as (SELECT * FROM (SELECT 
	*
    , MAX(REV_) OVER(PARTITION BY PROC_INST_ID_, sentText_num) as max_rev_per_sentText_num    
FROM
last_response_v1) sub
WHERE REV_ = max_rev_per_sentText_num group by PROC_INST_ID_, sentText_num, REV_)

, last_responses_v1 as ( SELECT 
	tms.* 
    , lr.TIME_		last_response_time
    , lr.TEXT_		last_response_text
FROM 
	(SELECT * FROM tm_w_resp) tms
LEFT JOIN
	( SELECT * FROM last_responses) lr
ON lr.PROC_INST_ID_ = tms.PROC_INST_ID_ AND lr.sentText_num = tms.message_type_order)

, tm_w_response as (SELECT 
	*
    , if (last_response_text in ('NO', 'OPT-OUT', 'PARAR', 'SÍ', 'YES'), last_response_text, if(last_response_text is not null, 'OTHER', null))			last_response_categorical
FROM last_responses_v1)

, maps_outcomes_base as (SELECT 
	ai.*
    , dt.NAME_
    , dt.TIME_
    , dt.REV_
    , dt.TEXT_
    , cc.NAME_		NAME_2
    , cc.TEXT_ call_count
    , cc.REV_		REV_2
FROM 
	( SELECT 
		PROC_INST_ID_
		, ID_ 
		, ACT_ID_
		, ACT_NAME_
		, START_TIME_
		, END_TIME_
	FROM ACT_HI_ACTINST
	WHERE ACT_ID_ in ('tmPlusMAPSCall', 'tm1MAPSCall')) ai
LEFT JOIN
	(SELECT * FROM ACT_HI_DETAIL WHERE NAME_ = 'pnResponse') dt
ON dt.PROC_INST_ID_ = ai.PROC_INST_ID_ AND dt.ACT_INST_ID_ = ai.ID_
LEFT JOIN
	(SELECT * FROM ACT_HI_DETAIL WHERE NAME_ in ('tmPlusMAPSCallCount', 'tm1MAPSCallCount')) cc
ON cc.PROC_INST_ID_ = ai.PROC_INST_ID_ AND cc.ACT_INST_ID_ = ai.ID_    
    
ORDER BY PROC_INST_ID_, START_TIME_)

, maps_msg_times as (SELECT
	maps1.PROC_INST_ID_
    , maps1.TIME_			maps1_message_hrts_GMT
    , maps2.TIME_			maps2_message_hrts_GMT
	, maps3.TIME_			maps3_message_hrts_GMT
FROM 
	(SELECT * FROM msg_order WHERE NAME_ in ('tm1AndMapsMessage', 'tmPlusMapsMessage1SentText')) maps1
LEFT JOIN
	(SELECT * FROM msg_order WHERE NAME_ = 'tmPlusMapsMessage2SentText') maps2
ON maps2.PROC_INST_ID_ = maps1.PROC_INST_ID_
LEFT JOIN
	(SELECT * FROM msg_order WHERE NAME_ = 'tmPlusMapsMessage3SentText') maps3
ON maps3.PROC_INST_ID_ = maps1.PROC_INST_ID_)
 
 , maps_outcomes as (SELECT 
    maps_outcomes_base.PROC_INST_ID_
    , CASE 
		WHEN maps2_message_hrts_GMT is null THEN 1
        WHEN TIME_ BETWEEN maps1_message_hrts_GMT AND maps2_message_hrts_GMT THEN 1
        WHEN TIME_ > maps2_message_hrts_GMT AND maps3_message_hrts_GMT is null THEN 2
        WHEN TIME_ BETWEEN maps2_message_hrts_GMT AND maps3_message_hrts_GMT THEN 2
		WHEN TIME_ > maps3_message_hrts_GMT THEN 3
		END AS 'maps_msg_number'
	, call_count
    , TIME_
    , TEXT_		maps_outcome
FROM maps_outcomes_base
LEFT JOIN maps_msg_times
ON maps_msg_times.PROC_INST_ID_ = maps_outcomes_base.PROC_INST_ID_)
*/


/* 
START: MAIN SELECT STATEMENT 
*/

select 
	a1.patient_cd
    , a2.PROC_INST_ID_ 		firstrand_PROC_INST_ID
    , a3.PROC_INST_ID_ 		secondrand_PROC_INST_ID
    , a4.TEXT_			firstrand_msg_freq_rand 			/* 1st half message frequency randomization: TM1 versus TM+ */
    , a5.TEXT_			firstrand_msg_type_rand 			/* 1st half message type randomization: Autonomy vs Directive vs Mixed */
    , a6.START_TIME_		secondrand_rand_hrts_GMT			/* @TB move this as the first of the 2nd half workflow columns, when starting 2nd half */
    , a6.TEXT_			secondrand_msg_maps_freq_rand 		
    /* 2nd half message and MAPS frequency randomization: 
    TM1 could have either 'Nothing' or 'MAPS'
    TM2 could have either 'TM-CONT' or 'TM+MAPS' */
    , coalesce(a7.TEXT_, a8.TEXT_) 		secondrand_msg_type_rand		
    /* second half message type was stored in either 'phase5RandomizationGroup' or 'phase6RandomizationGroup', 
    depending on their first half randomization. coalesce grabs the one not-null, if any */
	, a4.START_TIME_		firstrand_rand_hrts_GMT
	, a9.START_TIME_		optOutAfterInitialEmail_hrts_GMT	
    /* 
    , r1_sm1.TIME_							firstrand_sent_msg_1_hrts_GMT
    , r1_sm1.NAME_							firstrand_sent_msg_1_name
    , r1_sm1.TEXT_							firstrand_sent_msg_1_text
    , r1_sm1.response1_time					firstrand_sent_msg_1_resp_1_hrts_GMT
    , r1_sm1.response1_text					firstrand_sent_msg_1_resp_1_text
    , r1_sm1.response2_time					firstrand_sent_msg_1_resp_2_hrts_GMT
    , r1_sm1.response2_text					firstrand_sent_msg_1_resp_2_text
    , r1_sm1.last_response_time				firstrand_sent_msg_1_outcome_hrts_GMT
    , r1_sm1.last_response_text				firstrand_sent_msg_1_outcome_text    
    , r1_sm1.last_response_categorical		firstrand_sent_msg_1_outcome_category
    
    , r1_sm2.TIME_							firstrand_sent_msg_2_hrts_GMT
    , r1_sm2.NAME_							firstrand_sent_msg_2_name
    , r1_sm2.TEXT_							firstrand_sent_msg_2_text
    , r1_sm2.response1_time					firstrand_sent_msg_2_resp_1_hrts_GMT
    , r1_sm2.response1_text					firstrand_sent_msg_2_resp_1_text
    , r1_sm2.response2_time					firstrand_sent_msg_2_resp_2_hrts_GMT
    , r1_sm2.response2_text					firstrand_sent_msg_2_resp_2_text
    , r1_sm2.last_response_time				firstrand_sent_msg_2_outcome_hrts_GMT
    , r1_sm2.last_response_text				firstrand_sent_msg_2_outcome_text    
    , r1_sm2.last_response_categorical		firstrand_sent_msg_2_outcome_category
    
    , r1_sm3.TIME_							firstrand_sent_msg_3_hrts_GMT
    , r1_sm3.NAME_							firstrand_sent_msg_3_name
    , r1_sm3.TEXT_							firstrand_sent_msg_3_text
    , r1_sm3.response1_time					firstrand_sent_msg_3_resp_1_hrts_GMT
    , r1_sm3.response1_text					firstrand_sent_msg_3_resp_1_text
    , r1_sm3.response2_time					firstrand_sent_msg_3_resp_2_hrts_GMT
    , r1_sm3.response2_text					firstrand_sent_msg_3_resp_2_text
    , r1_sm3.last_response_time				firstrand_sent_msg_3_outcome_hrts_GMT
    , r1_sm3.last_response_text				firstrand_sent_msg_3_outcome_text    
    , r1_sm3.last_response_categorical		firstrand_sent_msg_3_outcome_category
    
    , r1_sm4.TIME_							firstrand_sent_msg_4_hrts_GMT
    , r1_sm4.NAME_							firstrand_sent_msg_4_name
    , r1_sm4.TEXT_							firstrand_sent_msg_4_text
    , r1_sm4.response1_time					firstrand_sent_msg_4_resp_1_hrts_GMT
    , r1_sm4.response1_text					firstrand_sent_msg_4_resp_1_text
    , r1_sm4.response2_time					firstrand_sent_msg_4_resp_2_hrts_GMT
    , r1_sm4.response2_text					firstrand_sent_msg_4_resp_2_text
    , r1_sm4.last_response_time				firstrand_sent_msg_4_outcome_hrts_GMT
    , r1_sm4.last_response_text				firstrand_sent_msg_4_outcome_text    
    , r1_sm4.last_response_categorical		firstrand_sent_msg_4_outcome_category
    
    , r1_sm5.TIME_							firstrand_sent_msg_5_hrts_GMT
    , r1_sm5.NAME_							firstrand_sent_msg_5_name
    , r1_sm5.TEXT_							firstrand_sent_msg_5_text
    , r1_sm5.response1_time					firstrand_sent_msg_5_resp_1_hrts_GMT
    , r1_sm5.response1_text					firstrand_sent_msg_5_resp_1_text
    , r1_sm5.response2_time					firstrand_sent_msg_5_resp_2_hrts_GMT
    , r1_sm5.response2_text					firstrand_sent_msg_5_resp_2_text
    , r1_sm5.last_response_time				firstrand_sent_msg_5_outcome_hrts_GMT
    , r1_sm5.last_response_text				firstrand_sent_msg_5_outcome_text    
    , r1_sm5.last_response_categorical		firstrand_sent_msg_5_outcome_category
    
    , r1_sm6.TIME_							firstrand_sent_msg_6_hrts_GMT
    , r1_sm6.NAME_							firstrand_sent_msg_6_name
    , r1_sm6.TEXT_							firstrand_sent_msg_6_text
    , r1_sm6.response1_time					firstrand_sent_msg_6_resp_1_hrts_GMT
    , r1_sm6.response1_text					firstrand_sent_msg_6_resp_1_text
    , r1_sm6.response2_time					firstrand_sent_msg_6_resp_2_hrts_GMT
    , r1_sm6.response2_text					firstrand_sent_msg_6_resp_2_text
    , r1_sm6.last_response_time				firstrand_sent_msg_6_outcome_hrts_GMT
    , r1_sm6.last_response_text				firstrand_sent_msg_6_outcome_text    
    , r1_sm6.last_response_categorical		firstrand_sent_msg_6_outcome_category
    */
    , a21.START_TIME_		firsthalf_optout_hrts_GMT
    , a22.START_TIME_		firsthalf_responseYES_hrts_GMT
        
    /* UPDATED to categorical outcome*/
    , a23.START_TIME_		firstphase_DPP_contact_hrts_GMT
    , a23.TEXT_				firstphase_DPP_contact_outcome	
        
    , a25.START_TIME_		firsthalf_dppEnrolled_hrts_GMT
    , a26.TEXT_				firsthalf_IncentaHealthID
    , a27.concat_notes		firsthalf_concatenated_notes
    
    /*	START Second Stage info				*/
    , b1.END_TIME_		secondphase_rand_hrts_GMT
    , b2.START_TIME_	secondphase_optout_hrts_GMT
    , CASE 
		WHEN b2.ACT_ID_ in ('sid-CC2EA9ED-95AF-4F47-98AD-950D8A597E4F' /* TM+ MAPS TEXT opt-out */, 'sid-73CC9859-DCF9-42F1-AF9B-8DA8F9CB5B2C' /* TM+ TEXT opt-out */) THEN 'text'
        WHEN b2.ACT_ID_ = 'sid-20A255E7-3DEA-42DB-B2DE-E6EA09D9CF45' /* TM+ MAPS Call opt-out*/ THEN 'MAPScall'
        END as secondphase_optout_how    
    , b3.START_TIME_	secondphase_responseYES_hrts_GMT	/* 1st:	Responded Yes	2nd: Responded Yes Medium (text vs MAPS)	*/
    , CASE
		WHEN b3.ACT_ID_ in ( 'sid-8A937BCA-B2E0-4C1F-8459-792C5F0D2B22' /*TM1 text*/, 'confirmationTmPlus' /* TM+ text*/ ) THEN 'text'
        WHEN b3.ACT_ID_ in ('sid-466385EC-CA34-4121-ABCF-24D2981D266E' /*TM1 MAPS*/, 'sid-059C1580-DDAB-48EB-A081-ACAF16AC58DC' /* TM+ MAPS */) THEN 'MAPScall'
        END as secondphase_responseYES_how
    , b4.START_TIME_		secondphase_DPP_contact_hrts_GMT
    , b4.TEXT_				secondphase_DPP_contact_outcome
    , b5.START_TIME_		secondphase_dppEnrolled_hrts_GMT
    , b6.TEXT_				secondphase_IncentaHealthID

    /* skipping to second rand messages and outcomes */
    /* 		Commented out for now, can uncomment to include		
    , r2_sm1.TIME_							secondrand_sent_msg_1_hrts_GMT
    , r2_sm1.NAME_							secondrand_sent_msg_1_name
    , r2_sm1.TEXT_							secondrand_sent_msg_1_text
    , r2_sm1.response1_time					secondrand_sent_msg_1_resp_1_hrts_GMT
    , r2_sm1.response1_text					secondrand_sent_msg_1_resp_1_text
    , r2_sm1.response2_time					secondrand_sent_msg_1_resp_2_hrts_GMT
    , r2_sm1.response2_text					secondrand_sent_msg_1_resp_2_text
    , r2_sm1.last_response_time				secondrand_sent_msg_1_outcome_hrts_GMT
    , r2_sm1.last_response_text				secondrand_sent_msg_1_outcome_text    
    , r2_sm1.last_response_categorical		secondrand_sent_msg_1_outcome_category
    
    , r2_sm2.TIME_							secondrand_sent_msg_2_hrts_GMT
    , r2_sm2.NAME_							secondrand_sent_msg_2_name
    , r2_sm2.TEXT_							secondrand_sent_msg_2_text
    , r2_sm2.response1_time					secondrand_sent_msg_2_resp_1_hrts_GMT
    , r2_sm2.response1_text					secondrand_sent_msg_2_resp_1_text
    , r2_sm2.response2_time					secondrand_sent_msg_2_resp_2_hrts_GMT
    , r2_sm2.response2_text					secondrand_sent_msg_2_resp_2_text
    , r2_sm2.last_response_time				secondrand_sent_msg_2_outcome_hrts_GMT
    , r2_sm2.last_response_text				secondrand_sent_msg_2_outcome_text    
    , r2_sm2.last_response_categorical		secondrand_sent_msg_2_outcome_category
    
    , r2_sm3.TIME_							secondrand_sent_msg_3_hrts_GMT
    , r2_sm3.NAME_							secondrand_sent_msg_3_name
    , r2_sm3.TEXT_							secondrand_sent_msg_3_text
    , r2_sm3.response1_time					secondrand_sent_msg_3_resp_1_hrts_GMT
    , r2_sm3.response1_text					secondrand_sent_msg_3_resp_1_text
    , r2_sm3.response2_time					secondrand_sent_msg_3_resp_2_hrts_GMT
    , r2_sm3.response2_text					secondrand_sent_msg_3_resp_2_text
    , r2_sm3.last_response_time				secondrand_sent_msg_3_outcome_hrts_GMT
    , r2_sm3.last_response_text				secondrand_sent_msg_3_outcome_text    
    , r2_sm3.last_response_categorical		secondrand_sent_msg_3_outcome_category
    
    , r2_sm4.TIME_							secondrand_sent_msg_4_hrts_GMT
    , r2_sm4.NAME_							secondrand_sent_msg_4_name
    , r2_sm4.TEXT_							secondrand_sent_msg_4_text
    , r2_sm4.response1_time					secondrand_sent_msg_4_resp_1_hrts_GMT
    , r2_sm4.response1_text					secondrand_sent_msg_4_resp_1_text
    , r2_sm4.response2_time					secondrand_sent_msg_4_resp_2_hrts_GMT
    , r2_sm4.response2_text					secondrand_sent_msg_4_resp_2_text
    , r2_sm4.last_response_time				secondrand_sent_msg_4_outcome_hrts_GMT
    , r2_sm4.last_response_text				secondrand_sent_msg_4_outcome_text    
    , r2_sm4.last_response_categorical		secondrand_sent_msg_4_outcome_category
    
    , r2_sm5.TIME_							secondrand_sent_msg_5_hrts_GMT
    , r2_sm5.NAME_							secondrand_sent_msg_5_name
    , r2_sm5.TEXT_							secondrand_sent_msg_5_text
    , r2_sm5.response1_time					secondrand_sent_msg_5_resp_1_hrts_GMT
    , r2_sm5.response1_text					secondrand_sent_msg_5_resp_1_text
    , r2_sm5.response2_time					secondrand_sent_msg_5_resp_2_hrts_GMT
    , r2_sm5.response2_text					secondrand_sent_msg_5_resp_2_text
    , r2_sm5.last_response_time				secondrand_sent_msg_5_outcome_hrts_GMT
    , r2_sm5.last_response_text				secondrand_sent_msg_5_outcome_text    
    , r2_sm5.last_response_categorical		secondrand_sent_msg_5_outcome_category
    
    , r2_sm6.TIME_							secondrand_sent_msg_6_hrts_GMT
    , r2_sm6.NAME_							secondrand_sent_msg_6_name
    , r2_sm6.TEXT_							secondrand_sent_msg_6_text
    , r2_sm6.response1_time					secondrand_sent_msg_6_resp_1_hrts_GMT
    , r2_sm6.response1_text					secondrand_sent_msg_6_resp_1_text
    , r2_sm6.response2_time					secondrand_sent_msg_6_resp_2_hrts_GMT
    , r2_sm6.response2_text					secondrand_sent_msg_6_resp_2_text
    , r2_sm6.last_response_time				secondrand_sent_msg_6_outcome_hrts_GMT
    , r2_sm6.last_response_text				secondrand_sent_msg_6_outcome_text    
    , r2_sm6.last_response_categorical		secondrand_sent_msg_6_outcome_category    
     -- END LARGE COMMENT OUT 
     */
    /*skipping ahead to MAPS Call Outcomes*/
    /* -- Uncomment this out later
    , m1c1.TIME_				secondhalf_msg1MAPS1_hrts_GMT
    , m1c1.maps_outcome			secondhalf_msg1MAPS1_CallOutcome
    , m1c2.TIME_				secondhalf_msg1MAPS2_hrts_GMT
    , m1c2.maps_outcome			secondhalf_msg1MAPS2_CallOutcome
    , m1c3.TIME_				secondhalf_msg1MAPS3_hrts_GMT
    , m1c3.maps_outcome			secondhalf_msg1MAPS3_CallOutcome
    , m1c4.TIME_				secondhalf_msg1MAPS4_hrts_GMT
    , m1c4.maps_outcome			secondhalf_msg1MAPS4_CallOutcome
    , m1c5.TIME_				secondhalf_msg1MAPS5_hrts_GMT
    , m1c5.maps_outcome			secondhalf_msg1MAPS5_CallOutcome
    , m1c6.TIME_				secondhalf_msg1MAPS6_hrts_GMT
    , m1c6.maps_outcome			secondhalf_msg1MAPS6_CallOutcome
    
    , m2c1.TIME_				secondhalf_msg2MAPS1_hrts_GMT
    , m2c1.maps_outcome			secondhalf_msg2MAPS1_CallOutcome
    , m2c2.TIME_				secondhalf_msg2MAPS2_hrts_GMT
    , m2c2.maps_outcome			secondhalf_msg2MAPS2_CallOutcome
    , m2c3.TIME_				secondhalf_msg2MAPS3_hrts_GMT
    , m2c3.maps_outcome			secondhalf_msg2MAPS3_CallOutcome
    , m2c4.TIME_				secondhalf_msg2MAPS4_hrts_GMT
    , m2c4.maps_outcome			secondhalf_msg2MAPS4_CallOutcome
    , m2c5.TIME_				secondhalf_msg2MAPS5_hrts_GMT
    , m2c5.maps_outcome			secondhalf_msg2MAPS5_CallOutcome
    , m2c6.TIME_				secondhalf_msg2MAPS6_hrts_GMT
    , m2c6.maps_outcome			secondhalf_msg2MAPS6_CallOutcome
    
    , m3c1.TIME_				secondhalf_msg3MAPS1_hrts_GMT
    , m3c1.maps_outcome			secondhalf_msg3MAPS1_CallOutcome
    , m3c2.TIME_				secondhalf_msg3MAPS2_hrts_GMT
    , m3c2.maps_outcome			secondhalf_msg3MAPS2_CallOutcome
    , m3c3.TIME_				secondhalf_msg3MAPS3_hrts_GMT
    , m3c3.maps_outcome			secondhalf_msg3MAPS3_CallOutcome
    , m3c4.TIME_				secondhalf_msg3MAPS4_hrts_GMT
    , m3c4.maps_outcome			secondhalf_msg3MAPS4_CallOutcome
    , m3c5.TIME_				secondhalf_msg3MAPS5_hrts_GMT
    , m3c5.maps_outcome			secondhalf_msg3MAPS5_CallOutcome
    , m3c6.TIME_				secondhalf_msg3MAPS6_hrts_GMT
    , m3c6.maps_outcome			secondhalf_msg3MAPS6_CallOutcome
    -- END large comment chunk */
from 
	( select patient_cd from tall_dat group by patient_cd) a1
left join
	( select patient_cd, PROC_INST_ID_ from tall_dat where wf_name = 'DPP V1' group by PROC_INST_ID_ ) a2
    on a2.patient_cd = a1.patient_cd 
left join 
	( select patient_cd, PROC_INST_ID_ from tall_dat where wf_name != 'DPP V1' group by PROC_INST_ID_) a3
    on a3.patient_cd = a1.patient_cd 
left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from tall_dat where NAME_ = 'phase1RandomizationGroup') a4
    on a4.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from tall_dat where NAME_ = 'phase2RandomizationGroup') a5
    on a5.PROC_INST_ID_ = a2.PROC_INST_ID_
   
left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from tall_dat where NAME_ = 'phase4RandomizationGroup') a6
    on a6.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from tall_dat where NAME_ = 'phase5RandomizationGroup') a7
    on a7.PROC_INST_ID_ = a3.PROC_INST_ID_    
left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from tall_dat where NAME_ = 'phase6RandomizationGroup') a8
    on a8.PROC_INST_ID_ = a3.PROC_INST_ID_     
left join
	( select PROC_INST_ID_, START_TIME_ from tall_dat where ACT_ID_ = 'optOutAfterInitialEmail') a9
    on a9.PROC_INST_ID_ = a2.PROC_INST_ID_
/* v2 updating the text messages and response/outcomes */
/* Uncomment this later
left join
	( select * from tm_w_response where message_type_order = 1) r1_sm1
	on r1_sm1.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select * from tm_w_response where message_type_order = 2) r1_sm2
	on r1_sm2.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select * from tm_w_response where message_type_order = 3) r1_sm3
	on r1_sm3.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select * from tm_w_response where message_type_order = 4) r1_sm4
	on r1_sm4.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select * from tm_w_response where message_type_order = 5) r1_sm5
	on r1_sm5.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select * from tm_w_response where message_type_order = 6) r1_sm6
	on r1_sm6.PROC_INST_ID_ = a2.PROC_INST_ID_    
*/
/* Grabbing datetimes for Opt-out and DPP states. Presence of a datetime indicates they were ever in that state - can make indicators easily from that */
left join
	( select PROC_INST_ID_, START_TIME_ from tall_dat where ACT_ID_ = 'endOptOut') a21
	on a21.PROC_INST_ID_ = a2.PROC_INST_ID_
 left join
	( select PROC_INST_ID_, START_TIME_ from tall_dat where
		ACT_ID_ in ('sid-923DFB3A-03EC-42B8-8D7C-44E45EA47CD6', 'sid-27A4E17F-D6D7-46CE-84FC-D5B8A091AB42')) a22
	on a22.PROC_INST_ID_ = a2.PROC_INST_ID_
 left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from tall_dat where 
		NAME_ = 'dppResponse' ) a23
	on a23.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select PROC_INST_ID_, START_TIME_ from tall_dat where ACT_ID_ = 'enrolled') a25
    on a25.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select PROC_INST_ID_, TEXT_ from tall_dat where
		( ACT_ID_ = 'pnEngaged' AND NAME_ = 'incentaHealthID' )) a26
	on a26.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select PROC_INST_ID_, group_concat(TEXT_ separator '---\n') concat_notes 
		from tall_dat where NAME_ = '_notes' group by PROC_INST_ID_) a27
	on a27.PROC_INST_ID_ = a2.PROC_INST_ID_
/*	Second Phase Indicators				*/
left join
	( select PROC_INST_ID_, END_TIME_ from tall_dat where ACT_ID_ = 'setRandomizationFromDB') b1
    on b1.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select PROC_INST_ID_, ACT_ID_, START_TIME_ from tall_dat where ACT_ID_ in ('sid-CC2EA9ED-95AF-4F47-98AD-950D8A597E4F' /* TM+ MAPS TEXT opt-out */,
			'sid-73CC9859-DCF9-42F1-AF9B-8DA8F9CB5B2C' /* TM+ TEXT opt-out */, 'sid-20A255E7-3DEA-42DB-B2DE-E6EA09D9CF45' /* TM+ MAPS Call opt-out*/) ) b2	
    on b2.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select PROC_INST_ID_, ACT_ID_, START_TIME_ from tall_dat where ACT_ID_ in ('sid-466385EC-CA34-4121-ABCF-24D2981D266E' /*TM1 MAPS*/ , 'sid-8A937BCA-B2E0-4C1F-8459-792C5F0D2B22' /*TM1 text*/, 
	'sid-059C1580-DDAB-48EB-A081-ACAF16AC58DC' /* TM+ MAPS */, 'confirmationTmPlus' /* TM+ text*/) ) b3
    on b3.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from tall_dat where 
		NAME_ = 'dppResponse' ) b4
	on b4.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select PROC_INST_ID_, START_TIME_ from tall_dat 
		where ACT_ID_ in ('enrolled' /* TM1 */,'enrolledTMPlus' /* TM+ */) ) b5
	on b5.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select PROC_INST_ID_, TEXT_ from tall_dat where
		( ACT_ID_ = 'pnEngaged' AND NAME_ = 'incentaHealthID' ) ) b6
	on b6.PROC_INST_ID_ = a3.PROC_INST_ID_


/* Second Half messages and outcomes 	*/
/*
left join
	( select * from tm_w_response where message_type_order = 1) r2_sm1
	on r2_sm1.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select * from tm_w_response where message_type_order = 2) r2_sm2
	on r2_sm2.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select * from tm_w_response where message_type_order = 3) r2_sm3
	on r2_sm3.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select * from tm_w_response where message_type_order = 4) r2_sm4
	on r2_sm4.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select * from tm_w_response where message_type_order = 5) r2_sm5
	on r2_sm5.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select * from tm_w_response where message_type_order = 6) r2_sm6
	on r2_sm6.PROC_INST_ID_ = a3.PROC_INST_ID_   

left join 
	( select * from maps_outcomes where maps_msg_number = 1 AND call_count = 1) m1c1
	on m1c1.PROC_INST_ID_ = a3.PROC_INST_ID_
left join 
	( select * from maps_outcomes where maps_msg_number = 1 AND call_count = 2) m1c2
	on m1c2.PROC_INST_ID_ = a3.PROC_INST_ID_
left join 
	( select * from maps_outcomes where maps_msg_number = 1 AND call_count = 3) m1c3
	on m1c3.PROC_INST_ID_ = a3.PROC_INST_ID_
left join 
	( select * from maps_outcomes where maps_msg_number = 1 AND call_count = 4) m1c4
	on m1c4.PROC_INST_ID_ = a3.PROC_INST_ID_
left join 
	( select * from maps_outcomes where maps_msg_number = 1 AND call_count = 5) m1c5
	on m1c5.PROC_INST_ID_ = a3.PROC_INST_ID_
left join 
	( select * from maps_outcomes where maps_msg_number = 1 AND call_count = 6) m1c6
	on m1c6.PROC_INST_ID_ = a3.PROC_INST_ID_

left join 
	( select * from maps_outcomes where maps_msg_number = 2 AND call_count = 1) m2c1
	on m2c1.PROC_INST_ID_ = a3.PROC_INST_ID_
left join 
	( select * from maps_outcomes where maps_msg_number = 2 AND call_count = 2) m2c2
	on m2c2.PROC_INST_ID_ = a3.PROC_INST_ID_
left join 
	( select * from maps_outcomes where maps_msg_number = 2 AND call_count = 3) m2c3
	on m2c3.PROC_INST_ID_ = a3.PROC_INST_ID_
left join 
	( select * from maps_outcomes where maps_msg_number = 2 AND call_count = 4) m2c4
	on m2c4.PROC_INST_ID_ = a3.PROC_INST_ID_
left join 
	( select * from maps_outcomes where maps_msg_number = 2 AND call_count = 5) m2c5
	on m2c5.PROC_INST_ID_ = a3.PROC_INST_ID_
left join 
	( select * from maps_outcomes where maps_msg_number = 2 AND call_count = 6) m2c6
	on m2c6.PROC_INST_ID_ = a3.PROC_INST_ID_

left join 
	( select * from maps_outcomes where maps_msg_number = 3 AND call_count = 1) m3c1
	on m3c1.PROC_INST_ID_ = a3.PROC_INST_ID_
left join 
	( select * from maps_outcomes where maps_msg_number = 3 AND call_count = 2) m3c2
	on m3c2.PROC_INST_ID_ = a3.PROC_INST_ID_
left join 
	( select * from maps_outcomes where maps_msg_number = 3 AND call_count = 3) m3c3
	on m3c3.PROC_INST_ID_ = a3.PROC_INST_ID_
left join 
	( select * from maps_outcomes where maps_msg_number = 3 AND call_count = 4) m3c4
	on m3c4.PROC_INST_ID_ = a3.PROC_INST_ID_
left join 
	( select * from maps_outcomes where maps_msg_number = 3 AND call_count = 5) m3c5
	on m3c5.PROC_INST_ID_ = a3.PROC_INST_ID_
left join 
	( select * from maps_outcomes where maps_msg_number = 3 AND call_count = 6) m3c6
	on m3c6.PROC_INST_ID_ = a3.PROC_INST_ID_
*/
group by patient_cd
	;
