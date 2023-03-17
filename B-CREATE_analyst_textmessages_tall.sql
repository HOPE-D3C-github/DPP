/* 
Summary: This script writes the tall text messages dataset as a table on the analyst schema. 
		 That dataset feeds into the pipeline to be used in the wide outcomes WF dataset, but can also be a standalone 'per request' additional dataset for analysis

Inputs from analyst schema: analyst.A_workflow_data_tall

Outputs: analyst.B_textmessages_tall
*/
use workflow;
DROP TABLE IF EXISTS analyst.B_textmessages_tall;
CREATE TABLE analyst.B_textmessages_tall

with 

/*clarified_resp as (select 
	PROC_INST_ID_
    , 'manuallyClarifiedResponse' 	NAME_
    , START_TIME_					TIME_
    , analyst.convert_to_AmDenv(START_TIME_) TIME_AmDenv
    , TEXT_
	, 'manuallyClarifiedResponse'	textType
from analyst.A_workflow_data_tall where NAME_ = 'pnResponse' AND ACT_ID_ in ('staffReviewTM1', 'staffReviewTMPlus', 'tm1AndMapsInvalidResponseSort', 'tmPlusInvalidResponseSort'))


,*/ 
base as (SELECT
	PROC_INST_ID_
    , NAME_
    , TIME_
    , TEXT_ 
    , CASE
		WHEN NAME_ like 'tmPlusMapsMessage%'	THEN 'sentMAPSnotification'
		WHEN NAME_ regexp 'message[1-5]SentText' AND NAME_ != 'noContactMessage1SentText' THEN 'sentMotivationalTM'
        WHEN NAME_ regexp 'response[0-3]Text' THEN 'response'
        WHEN NAME_ = 'clarification1SentText' THEN 'sentClarfication'
        WHEN NAME_ = 'confirmation1SentText' THEN 'sentConfirmation'
        WHEN NAME_ = 'noContactMessage1SentText' THEN 'sentNoContact'
        END as textType 
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
    , analyst.convert_to_AmDenv(base.TIME_) TIME_AmDenv
    , base.TEXT_
    , if( p4.TEXT_ = 'MAPS' AND base.NAME_ = 'message1SentText', 'tm1AndMapsMessage', base.textType) 		textType
from (select * from base) base
	left join
		( select * from ACT_HI_DETAIL where NAME_ = 'phase4RandomizationGroup') p4
	on p4.PROC_INST_ID_ = base.PROC_INST_ID_)

-- select * from base_v2;

/*, base_v3 as (select * from base_v2 union all select * FROM clarified_resp) */

-- select * from base_v3 order by PROC_INST_ID_, TIME_, NAME_ desc;


, msg_order as (SELECT 
	*
    , ROW_NUMBER() OVER( PARTITION BY PROC_INST_ID_ ORDER BY TIME_, FIELD(textType, 'response','sentClarification','sentConfirmation','sentNoContact','sentMotivationalTM','sentMAPSnotification'))	message_order 
    -- , ROW_NUMBER() OVER( PARTITION BY PROC_INST_ID_ ORDER BY TIME_, NAME_) 			message_order /* Saw cases where clarification#SentText had same timestamp as a response. Order should be that the response comes before the clarificationsenttext*/
    , ROW_NUMBER() OVER( PARTITION BY PROC_INST_ID_, textType ORDER BY TIME_, NAME_)	message_type_order
FROM base_v2
order by PROC_INST_ID_, textType, TIME_, NAME_)

select * from msg_order
;