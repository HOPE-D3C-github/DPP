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

/* START: REMOVE THIS FROM COPY PASTE INTO MAIN - it is redundant */
, base as (SELECT
	PROC_INST_ID_
    , NAME_
    , TIME_
    , TEXT_ 
    , if (NAME_ REGEXP 'response[0-3]Text', 'response', 'sentText') textType
    FROM ACT_HI_DETAIL
	where (NAME_ REGEXP 'response[0-3]Text' /* Aside from the 3 pts with a bug, no pts had more than 3 responses per workflow*/
		OR NAME_ REGEXP 'SentText')
       -- /* just for testing, remove after*/ AND PROC_INST_ID_ = '26458214-384d-11ed-a7e6-005056be8d74' -- '0e853edb-3f6a-11ed-9f51-005056be8d74'
    order by PROC_INST_ID_, TIME_, NAME_
    )

/* select * from base;*/

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

, msg_order as (SELECT 
	*
    , ROW_NUMBER() OVER( PARTITION BY PROC_INST_ID_ ORDER BY TIME_, textType, NAME_) 			message_order /* Saw cases where clarification#SentText had same timestamp as a response. Order should be that the response comes before the clarificationsenttext*/
    , ROW_NUMBER() OVER( PARTITION BY PROC_INST_ID_, textType ORDER BY TIME_, NAME_)	message_type_order
FROM base_v2
order by PROC_INST_ID_, textType, TIME_, NAME_)

/* END: REMOVE THIS FROM COPY PASTE INTO MAIN - it is redundant */

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

/*SELECT * FROM maps_outcomes_base ORDER BY PROC_INST_ID_, START_TIME_; */


/* NEED TO MATCH UP THE CORRESPONDING MESSAGE # for the call count

Can use one of the interim tables used for the message response matching process 

JOIN ON THE MESSAGE 1, 2, 3 START TIMES*/

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

-- SELECT * FROM maps_msg_times
 
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

-- SELECT * FROM maps_outcomes ORDER BY PROC_INST_ID_, TIME_;

, maps_per_msg_outcomes as (SELECT * FROM (SELECT 
	*
    , MIN(TIME_) OVER ( PARTITION BY PROC_INST_ID_, maps_msg_number )	time_of_earliest_call_per_msg_number
    , MAX(TIME_) OVER ( PARTITION BY PROC_INST_ID_, maps_msg_number )	lastvalue_time
 FROM maps_outcomes) sub
WHERE TIME_ = lastvalue_time)

/* 
START: MAIN SELECT STATEMENT 
*/

select 
	a1.patient_cd
    , pl.patient_language		primary_language
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
    
    , a21.START_TIME_		firsthalf_optout_hrts_GMT
    , a22.START_TIME_		firsthalf_responseYES_hrts_GMT
        
    
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
    , b7.concat_notes		secondphase_concatenated_notes
    , maps1.time_of_earliest_call_per_msg_number		MAPS_1_first_call_hrts_GMT
    , maps1.lastvalue_time								MAPS_1_last_call_hrts_GMT    
    , maps1.call_count		MAPS_1_call_count
    , maps1.maps_outcome	MAPS_1_outcome
    , maps2.time_of_earliest_call_per_msg_number		MAPS_2_first_call_hrts_GMT
    , maps2.lastvalue_time								MAPS_2_last_call_hrts_GMT    
    , maps2.call_count		MAPS_2_call_count
    , maps2.maps_outcome	MAPS_2_outcome
    , maps3.time_of_earliest_call_per_msg_number		MAPS_3_first_call_hrts_GMT
    , maps3.lastvalue_time								MAPS_3_last_call_hrts_GMT    
    , maps3.call_count		MAPS_3_call_count
    , maps3.maps_outcome	MAPS_3_outcome
    
from 
	( select patient_cd from tall_dat group by patient_cd) a1
left join
	( select patient_cd, patient_language from bmi_clinic_patient) pl
    on pl.patient_cd = a1.patient_cd    
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
left join
	( select PROC_INST_ID_, group_concat(TEXT_ separator '---\n') concat_notes 
		from tall_dat where NAME_ = '_notes' group by PROC_INST_ID_) b7
	on b7.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select * from maps_per_msg_outcomes where maps_msg_number = 1 ) maps1
	on maps1.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select * from maps_per_msg_outcomes where maps_msg_number = 2 ) maps2
	on maps2.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select * from maps_per_msg_outcomes where maps_msg_number = 3 ) maps3
	on maps3.PROC_INST_ID_ = a3.PROC_INST_ID_
    
group by patient_cd
	;
