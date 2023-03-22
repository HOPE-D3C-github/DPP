use workflow;

/*create view analyst.wide_test as */
with tall_dat as (select
	BUSINESS_KEY_ patient_cd
    , pd.NAME_ wf_name
    , un.* 
from (
	select 
		dt.PROC_INST_ID_
		, "ACT_HI_DETAIL" as source_tbl
		, ai.ACT_ID_ 		
		, NAME_
		, VAR_TYPE_
		, coalesce(DOUBLE_, LONG_, TEXT_) 		var_value 
		, TIME_ START_TIME_
		, TIME_ END_TIME_
		, ai.TRANSACTION_ORDER_
		, null as DURATION_ 
	from ACT_HI_DETAIL	dt
	left join ( select
		PROC_INST_ID_
		, ID_
		, ACT_ID_ 
		, TRANSACTION_ORDER_
		from ACT_HI_ACTINST) ai
	on ai.PROC_INST_ID_ = dt.PROC_INST_ID_ and
		ai.ID_ = dt.ACT_INST_ID_
	union all 
	select
		PROC_INST_ID_
		, "ACT_HI_ACTINST" as source
		, ACT_ID_
		, ACT_NAME_			NAME_
		, null as VAR_TYPE_
		, null as var_value    
		, START_TIME_
		, END_TIME_
		, TRANSACTION_ORDER_
		, DURATION_
	from ACT_HI_ACTINST ) un
left join (
	select PROC_INST_ID_, BUSINESS_KEY_, PROC_DEF_ID_
		from ACT_HI_PROCINST) pi
on pi.PROC_INST_ID_ = un.PROC_INST_ID_
left join (
	select ID_, NAME_
		from ACT_RE_PROCDEF) pd
on pd.ID_ = pi.PROC_DEF_ID_
	order by patient_cd, START_TIME_, END_TIME_, TRANSACTION_ORDER_, source_tbl)
  
, mapsCalls as (select 
resp.PROC_INST_ID_
, resp.TEXT_ 		MAPSCallOutcome
, resp.TIME_		MapsCall_hrts_GMT
, if(resp.TIME_ < coalesce(msg2_time, '2023-01-01 01:01:01'), 1, if(resp.TIME_ < coalesce(msg3_time, '2023-01-01 01:01:01'), 2, 3))		msg_count
, callcnt.MAPSCallCount
from 
(select * from ACT_HI_DETAIL where NAME_ ='pnResponse' AND TEXT_ is not null) resp
left join
	(select PROC_INST_ID_, TIME_ msg1_time from ACT_HI_DETAIL 
		where (NAME_ = 'tmPlusMapsMessageCount' AND TEXT_ = 1) OR NAME_='message1SentText'
    ) msg1
    on msg1.PROC_INST_ID_ = resp.PROC_INST_ID_
left join
	(select PROC_INST_ID_, TIME_ msg2_time from ACT_HI_DETAIL 
		where NAME_ = 'tmPlusMapsMessageCount' AND TEXT_ = 2
    ) msg2
    on msg2.PROC_INST_ID_ = resp.PROC_INST_ID_
left join
	(select PROC_INST_ID_, TIME_ msg3_time from ACT_HI_DETAIL 
		where NAME_ = 'tmPlusMapsMessageCount' AND TEXT_ = 3
    ) msg3
    on msg3.PROC_INST_ID_ = resp.PROC_INST_ID_   
left join
	( select PROC_INST_ID_, TIME_, TEXT_ MAPSCallCount from ACT_HI_DETAIL
		where NAME_ in ('tmPlusMAPSCallCount', 'tm1MAPSCallCount')) callcnt
	on callcnt.PROC_INST_ID_ = resp.PROC_INST_ID_ AND callcnt.TIME_ = resp.TIME_
where MAPSCallCount is not null
group by PROC_INST_ID_, msg_count, MAPSCallCount
order by PROC_INST_ID_, MapsCall_hrts_GMT)

, v1 as (SELECT m1.PROC_INST_ID_, m1_sent_hrts, m1_sent_text, m2_sent_hrts, m2_sent_text, m3_sent_hrts, m3_sent_text
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

, tm_and_responses as (SELECT  
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
	on m5r.PROC_INST_ID_ = ms.PROC_INST_ID_)

select 
	a1.patient_cd
    , a2.PROC_INST_ID_ 		firsthalf_PROC_INST_ID
    , a3.PROC_INST_ID_ 		secondhalf_PROC_INST_ID
    , a4.var_value			firsthalf_msg_freq_rand 			/* 1st half message frequency randomization: TM1 versus TM+ */
    , a5.var_value			firsthalf_msg_type_rand 			/* 1st half message type randomization: Autonomy vs Directive vs Mixed */
    , a6.START_TIME_		secondhalf_rand_hrts_GMT			/* @TB move this as the first of the 2nd half workflow columns, when starting 2nd half */
    , a6.var_value			secondhalf_msg_maps_freq_rand 		
    /* 2nd half message and MAPS frequency randomization: 
    TM1 could have either 'Nothing' or 'MAPS'
    TM2 could have either 'TM-CONT' or 'TM+MAPS' */
    , coalesce(a7.var_value, a8.var_value) 		secondhalf_msg_type_rand		
    /* second half message type was stored in either 'phase5RandomizationGroup' or 'phase6RandomizationGroup', 
    depending on their first half randomization. coalesce grabs the one not-null, if any */
	, a4.START_TIME_		firsthalf_rand_hrts_GMT
	, a9.START_TIME_		optOutAfterInitialEmail_hrts_GMT	
    , m1_sent_hrts			message1_sent_hrts_GMT
    , m1_sent_text			message1_sent_text
    , m1_response_hrts		message1_response_hrts_GMT
    , m1_response_text		message1_response_text
    , SUBSTRING(REPLACE(a11.var_value, 'T', ' '), 1, 19)			message2_time2send_hrts_MntTz		
    , m2_sent_hrts			message2_sent_hrts_GMT
    , m2_sent_text			message2_sent_text
    , m2_response_hrts		message2_response_hrts_GMT
    , m2_response_text		message2_response_text
    , SUBSTRING(REPLACE(a13.var_value, 'T', ' '), 1, 19)			message3_time2send_hrts_MntTz	
    , m3_sent_hrts			message3_sent_hrts_GMT
    , m3_sent_text			message3_sent_text
    , m3_response_hrts		message3_response_hrts_GMT
    , m3_response_text		message3_response_text
    , SUBSTRING(REPLACE(a15.var_value, 'T', ' '), 1, 19)			message4_time2send_hrts_MntTz
    , m4_sent_hrts			message4_sent_hrts_GMT
    , m4_sent_text			message4_sent_text
    , m4_response_hrts		message4_response_hrts_GMT
    , m4_response_text		message4_response_text
    , SUBSTRING(REPLACE(a17.var_value, 'T', ' '), 1, 19)			message5_time2send_hrts_MntTz	
    , m5_sent_hrts			message5_sent_hrts_GMT
    , m5_sent_text			message5_sent_text
    , m5_response_hrts		message5_response_hrts_GMT
    , m5_response_text		message5_response_text
    /* UPDATE for per message outcomes */
    , a21.START_TIME_		firsthalf_optout_hrts_GMT
    
    , CASE 
		WHEN (m2_sent_hrts is not null AND a21.START_TIME_ BETWEEN m1_sent_hrts AND m2_sent_hrts) OR (m2_sent_hrts is null AND a21.START_TIME_ > m1_sent_hrts)
			THEN 'message1'
		WHEN (m3_sent_hrts is not null AND a21.START_TIME_ BETWEEN m2_sent_hrts AND m3_sent_hrts) OR (m3_sent_hrts is null AND a21.START_TIME_ > m2_sent_hrts)
			THEN 'message2'
		WHEN (m4_sent_hrts is not null AND a21.START_TIME_ BETWEEN m3_sent_hrts AND m4_sent_hrts) OR (m4_sent_hrts is null AND a21.START_TIME_ > m3_sent_hrts)
			THEN 'message3'
		WHEN (m5_sent_hrts is not null AND a21.START_TIME_ BETWEEN m4_sent_hrts AND m5_sent_hrts) OR (m5_sent_hrts is null AND a21.START_TIME_ > m4_sent_hrts)
			THEN 'message4'
		WHEN a21.START_TIME_ > m5_sent_hrts
			THEN 'message5'
		END AS firsthalf_optout_at_message_number
    , a22.START_TIME_		firsthalf_responseYES_hrts_GMT
    , CASE 
		WHEN (m2_sent_hrts is not null AND a22.START_TIME_ BETWEEN m1_sent_hrts AND m2_sent_hrts) OR (m2_sent_hrts is null AND a22.START_TIME_ > m1_sent_hrts)
			THEN 'message1'
		WHEN (m3_sent_hrts is not null AND a22.START_TIME_ BETWEEN m2_sent_hrts AND m3_sent_hrts) OR (m3_sent_hrts is null AND a22.START_TIME_ > m2_sent_hrts)
			THEN 'message2'
		WHEN (m4_sent_hrts is not null AND a22.START_TIME_ BETWEEN m3_sent_hrts AND m4_sent_hrts) OR (m4_sent_hrts is null AND a22.START_TIME_ > m3_sent_hrts)
			THEN 'message3'
		WHEN (m5_sent_hrts is not null AND a22.START_TIME_ BETWEEN m4_sent_hrts AND m5_sent_hrts) OR (m5_sent_hrts is null AND a22.START_TIME_ > m4_sent_hrts)
			THEN 'message4'
		WHEN a22.START_TIME_ > m5_sent_hrts
			THEN 'message5'
		END AS firsthalf_resp_YES_at_message_number
    
    /* UPDATED to categorical outcome*/
    , a23.var_value			DPP_contact_outcome	
    , a23.START_TIME_		DPP_contact_hrts_GMT
    
    , a25.START_TIME_		firsthalf_dppEnrolled_hrts_GMT
    , a26.var_value			firsthalf_IncentaHealthID
    , a27.concat_notes		firsthalf_concatenated_notes
    /*skipping ahead to MAPS Call Outcomes*/
    , m1c1.MAPSCall_hrts_GMT		secondhalf_msg1MAPS1_hrts_GMT
    , m1c1.MAPSCallOutcome			secondhalf_msg1MAPS1_CallOutcome
    , m1c2.MAPSCall_hrts_GMT		secondhalf_msg1MAPS2_hrts_GMT
    , m1c2.MAPSCallOutcome			secondhalf_msg1MAPS2_CallOutcome
    , m1c3.MAPSCall_hrts_GMT		secondhalf_msg1MAPS3_hrts_GMT
    , m1c3.MAPSCallOutcome			secondhalf_msg1MAPS3_CallOutcome
    , m1c4.MAPSCall_hrts_GMT		secondhalf_msg1MAPS4_hrts_GMT
    , m1c4.MAPSCallOutcome			secondhalf_msg1MAPS4_CallOutcome
    , m1c5.MAPSCall_hrts_GMT		secondhalf_msg1MAPS5_hrts_GMT
    , m1c5.MAPSCallOutcome			secondhalf_msg1MAPS5_CallOutcome
    , m1c6.MAPSCall_hrts_GMT		secondhalf_msg1MAPS6_hrts_GMT
    , m1c6.MAPSCallOutcome			secondhalf_msg1MAPS6_CallOutcome
    
    , m2c1.MAPSCall_hrts_GMT		secondhalf_msg2MAPS1_hrts_GMT
    , m2c1.MAPSCallOutcome			secondhalf_msg2MAPS1_CallOutcome
    , m2c2.MAPSCall_hrts_GMT		secondhalf_msg2MAPS2_hrts_GMT
    , m2c2.MAPSCallOutcome			secondhalf_msg2MAPS2_CallOutcome
    , m2c3.MAPSCall_hrts_GMT		secondhalf_msg2MAPS3_hrts_GMT
    , m2c3.MAPSCallOutcome			secondhalf_msg2MAPS3_CallOutcome
    , m2c4.MAPSCall_hrts_GMT		secondhalf_msg2MAPS4_hrts_GMT
    , m2c4.MAPSCallOutcome			secondhalf_msg2MAPS4_CallOutcome
    , m2c5.MAPSCall_hrts_GMT		secondhalf_msg2MAPS5_hrts_GMT
    , m2c5.MAPSCallOutcome			secondhalf_msg2MAPS5_CallOutcome
    , m2c6.MAPSCall_hrts_GMT		secondhalf_msg2MAPS6_hrts_GMT
    , m2c6.MAPSCallOutcome			secondhalf_msg2MAPS6_CallOutcome
    
    , m3c1.MAPSCall_hrts_GMT		secondhalf_msg3MAPS1_hrts_GMT
    , m3c1.MAPSCallOutcome			secondhalf_msg3MAPS1_CallOutcome
    , m3c2.MAPSCall_hrts_GMT		secondhalf_msg3MAPS2_hrts_GMT
    , m3c2.MAPSCallOutcome			secondhalf_msg3MAPS2_CallOutcome
    , m3c3.MAPSCall_hrts_GMT		secondhalf_msg3MAPS3_hrts_GMT
    , m3c3.MAPSCallOutcome			secondhalf_msg3MAPS3_CallOutcome
    , m3c4.MAPSCall_hrts_GMT		secondhalf_msg3MAPS4_hrts_GMT
    , m3c4.MAPSCallOutcome			secondhalf_msg3MAPS4_CallOutcome
    , m3c5.MAPSCall_hrts_GMT		secondhalf_msg3MAPS5_hrts_GMT
    , m3c5.MAPSCallOutcome			secondhalf_msg3MAPS5_CallOutcome
    , m3c6.MAPSCall_hrts_GMT		secondhalf_msg3MAPS6_hrts_GMT
    , m3c6.MAPSCallOutcome			secondhalf_msg3MAPS6_CallOutcome
    
from 
	( select patient_cd from tall_dat group by patient_cd) a1
left join
	( select patient_cd, PROC_INST_ID_ from tall_dat where wf_name = 'DPP V1' group by PROC_INST_ID_ ) a2
    on a2.patient_cd = a1.patient_cd 
left join 
	( select patient_cd, PROC_INST_ID_ from tall_dat where wf_name != 'DPP V1' group by PROC_INST_ID_) a3
    on a3.patient_cd = a1.patient_cd 
left join
	( select PROC_INST_ID_, var_value, START_TIME_ from tall_dat where NAME_ = 'phase1RandomizationGroup') a4
    on a4.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select PROC_INST_ID_, var_value, START_TIME_ from tall_dat where NAME_ = 'phase2RandomizationGroup') a5
    on a5.PROC_INST_ID_ = a2.PROC_INST_ID_
   
left join
	( select PROC_INST_ID_, var_value, START_TIME_ from tall_dat where NAME_ = 'phase4RandomizationGroup') a6
    on a6.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select PROC_INST_ID_, var_value, START_TIME_ from tall_dat where NAME_ = 'phase5RandomizationGroup') a7
    on a7.PROC_INST_ID_ = a3.PROC_INST_ID_    
left join
	( select PROC_INST_ID_, var_value, START_TIME_ from tall_dat where NAME_ = 'phase6RandomizationGroup') a8
    on a8.PROC_INST_ID_ = a3.PROC_INST_ID_     
left join
	( select PROC_INST_ID_, START_TIME_ from tall_dat where ACT_ID_ = 'optOutAfterInitialEmail') a9
    on a9.PROC_INST_ID_ = a2.PROC_INST_ID_
left join 
	( select PROC_INST_ID_, START_TIME_ from tall_dat where NAME_ = 'message1SentDate') a10
	on a10.PROC_INST_ID_ = a2.PROC_INST_ID_
left join 
	( select PROC_INST_ID_, var_value from tall_dat where NAME_ = 'message2TimeToSend') a11
	on a11.PROC_INST_ID_ = a2.PROC_INST_ID_
left join 
	( select PROC_INST_ID_, START_TIME_ from tall_dat where NAME_ = 'message2SentDate') a12
	on a12.PROC_INST_ID_ = a2.PROC_INST_ID_
left join 
	( select PROC_INST_ID_, var_value from tall_dat where NAME_ = 'message3TimeToSend') a13
	on a13.PROC_INST_ID_ = a2.PROC_INST_ID_
left join 
	( select PROC_INST_ID_, START_TIME_ from tall_dat where NAME_ = 'message3SentDate') a14
	on a14.PROC_INST_ID_ = a2.PROC_INST_ID_
left join 
	( select PROC_INST_ID_, var_value from tall_dat where NAME_ = 'message4TimeToSend') a15
	on a15.PROC_INST_ID_ = a2.PROC_INST_ID_
left join 
	( select PROC_INST_ID_, START_TIME_ from tall_dat where NAME_ = 'message4SentDate') a16
	on a16.PROC_INST_ID_ = a2.PROC_INST_ID_
left join 
	( select PROC_INST_ID_, var_value from tall_dat where NAME_ = 'message5TimeToSend') a17
	on a17.PROC_INST_ID_ = a2.PROC_INST_ID_
left join 
	( select PROC_INST_ID_, START_TIME_ from tall_dat where NAME_ = 'message5SentDate') a18
	on a18.PROC_INST_ID_ = a2.PROC_INST_ID_
/* Grabbing the patient responses: datetime and text value
	the first half had 2 max responses aside from the 3 participants who
    show 100+ responses recieved with the same message all in the same day - likely a software issue?? */
left join
	( select * from tm_and_responses) tm_resp
	on tm_resp.PROC_INST_ID_ = a2.PROC_INST_ID_ 

left join 
	( select PROC_INST_ID_, var_value, START_TIME_ from tall_dat where NAME_ = 'response1Text') a19
    on a19.PROC_INST_ID_ = a2.PROC_INST_ID_
left join 
	( select PROC_INST_ID_, var_value, START_TIME_ from tall_dat where NAME_ = 'response2Text') a20
    on a20.PROC_INST_ID_ = a2.PROC_INST_ID_   
/* Grabbing datetimes for Opt-out and DPP states. Presence of a datetime indicates they were ever in that state - can make indicators easily from that */
left join
	( select PROC_INST_ID_, START_TIME_ from tall_dat where ACT_ID_ = 'endOptOut') a21
	on a21.PROC_INST_ID_ = a2.PROC_INST_ID_
 left join
	( select PROC_INST_ID_, START_TIME_ from tall_dat where
		ACT_ID_ in ('sid-923DFB3A-03EC-42B8-8D7C-44E45EA47CD6', 'sid-27A4E17F-D6D7-46CE-84FC-D5B8A091AB42')) a22
	on a22.PROC_INST_ID_ = a2.PROC_INST_ID_
 left join
	( select PROC_INST_ID_, var_value, START_TIME_ from tall_dat where 
		NAME_ = 'dppResponse' ) a23
	on a23.PROC_INST_ID_ = a2.PROC_INST_ID_
/*left join
	( select PROC_INST_ID_, var_value, START_TIME_ from tall_dat where
		( NAME_ = 'dppResponse' AND var_value != 'Enrolled')) a24
	on a24.PROC_INST_ID_ = a2.PROC_INST_ID_ */
left join
	( select PROC_INST_ID_, START_TIME_ from tall_dat where ACT_ID_ = 'enrolled') a25
    on a25.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select PROC_INST_ID_, var_value from tall_dat where
		( ACT_ID_ = 'pnEngaged' AND NAME_ = 'incentaHealthID' )) a26
	on a26.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select PROC_INST_ID_, group_concat(var_value separator '---\n') concat_notes 
		from tall_dat where NAME_ = '_notes' group by PROC_INST_ID_) a27
	on a27.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select * from mapsCalls where msg_count = 1 AND MAPSCallCount = 1) m1c1
	on m1c1.PROC_INST_ID_ = a3.PROC_INST_ID_  /* Second half matches to a3 */
 left join
	( select * from mapsCalls where msg_count = 1 AND MAPSCallCount = 2) m1c2
	on m1c2.PROC_INST_ID_ = a3.PROC_INST_ID_ 
left join
	( select * from mapsCalls where msg_count = 1 AND MAPSCallCount = 3) m1c3
	on m1c3.PROC_INST_ID_ = a3.PROC_INST_ID_ 
left join
	( select * from mapsCalls where msg_count = 1 AND MAPSCallCount = 4) m1c4
	on m1c4.PROC_INST_ID_ = a3.PROC_INST_ID_  
 left join
	( select * from mapsCalls where msg_count = 1 AND MAPSCallCount = 5) m1c5
	on m1c5.PROC_INST_ID_ = a3.PROC_INST_ID_ 
left join
	( select * from mapsCalls where msg_count = 1 AND MAPSCallCount = 6) m1c6
	on m1c6.PROC_INST_ID_ = a3.PROC_INST_ID_     

left join
	( select * from mapsCalls where msg_count = 2 AND MAPSCallCount = 1) m2c1
	on m2c1.PROC_INST_ID_ = a3.PROC_INST_ID_ 
 left join
	( select * from mapsCalls where msg_count = 2 AND MAPSCallCount = 2) m2c2
	on m2c2.PROC_INST_ID_ = a3.PROC_INST_ID_ 
left join
	( select * from mapsCalls where msg_count = 2 AND MAPSCallCount = 3) m2c3
	on m2c3.PROC_INST_ID_ = a3.PROC_INST_ID_ 
left join
	( select * from mapsCalls where msg_count = 2 AND MAPSCallCount = 4) m2c4
	on m2c4.PROC_INST_ID_ = a3.PROC_INST_ID_  
 left join
	( select * from mapsCalls where msg_count = 2 AND MAPSCallCount = 5) m2c5
	on m2c5.PROC_INST_ID_ = a3.PROC_INST_ID_ 
left join
	( select * from mapsCalls where msg_count = 2 AND MAPSCallCount = 6) m2c6
	on m2c6.PROC_INST_ID_ = a3.PROC_INST_ID_ 

left join
	( select * from mapsCalls where msg_count = 3 AND MAPSCallCount = 1) m3c1
	on m3c1.PROC_INST_ID_ = a3.PROC_INST_ID_ 
 left join
	( select * from mapsCalls where msg_count = 3 AND MAPSCallCount = 2) m3c2
	on m3c2.PROC_INST_ID_ = a3.PROC_INST_ID_ 
left join
	( select * from mapsCalls where msg_count = 3 AND MAPSCallCount = 3) m3c3
	on m3c3.PROC_INST_ID_ = a3.PROC_INST_ID_ 
left join
	( select * from mapsCalls where msg_count = 3 AND MAPSCallCount = 4) m3c4
	on m3c4.PROC_INST_ID_ = a3.PROC_INST_ID_  
 left join
	( select * from mapsCalls where msg_count = 3 AND MAPSCallCount = 5) m3c5
	on m3c5.PROC_INST_ID_ = a3.PROC_INST_ID_ 
left join
	( select * from mapsCalls where msg_count = 3 AND MAPSCallCount = 6) m3c6
	on m3c6.PROC_INST_ID_ = a3.PROC_INST_ID_ 

	;

select * from analyst.wide_test where firsthalf_response2_text is not null;
