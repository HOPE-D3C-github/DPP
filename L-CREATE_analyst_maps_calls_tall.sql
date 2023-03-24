use workflow;
DROP TABLE IF EXISTS analyst.L_mapsCalls_tall;
CREATE TABLE analyst.L_mapsCalls_tall as
with base as (SELECT
	PROC_INST_ID_
    , NAME_
    , TIME_
    , TEXT_ 
    , if (NAME_ REGEXP 'response[0-3]Text', 'response', 'sentText') textType
    FROM ACT_HI_DETAIL
	where (NAME_ REGEXP 'response[0-3]Text' /* Aside from the 3 pts with a bug, no pts had more than 3 responses per workflow*/
		OR NAME_ REGEXP 'SentText')
	order by PROC_INST_ID_, TIME_, NAME_
    )

/* 	TM1andMAPS has second rand text message 'tm1AndMAPSMessage' labelled as 'message1SentText' which has the same label as the numbering of TMs (non MAPS notification TMs)
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

, msg_order as (SELECT 
	*
    , ROW_NUMBER() OVER( PARTITION BY PROC_INST_ID_ ORDER BY TIME_, textType, NAME_) 	B_textmessages_tallmessage_order 
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
	cw.uid
	, pi.BUSINESS_KEY_
    , maps_outcomes_base.PROC_INST_ID_
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
ON maps_msg_times.PROC_INST_ID_ = maps_outcomes_base.PROC_INST_ID_
LEFT JOIN ACT_HI_PROCINST pi
ON pi.PROC_INST_ID_ = maps_outcomes_base.PROC_INST_ID_
LEFT JOIN analyst.C_crosswalk_uid_to_MRN cw
ON cw.patient_cd = pi.BUSINESS_KEY_

)

SELECT * FROM maps_outcomes ORDER BY PROC_INST_ID_, TIME_;