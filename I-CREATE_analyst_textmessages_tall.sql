/* 
Summary: This script writes the tall text messages dataset as a table on the analyst schema. 
		 That dataset feeds into the pipeline to be used in the wide outcomes WF dataset, but can also be a standalone 'per request' additional dataset for analysis

Inputs from analyst schema: NONE

Outputs: analyst.I_textmessages_tall
*/
use workflow;
DROP TABLE IF EXISTS analyst.I_textmessages_tall;
CREATE TABLE analyst.I_textmessages_tall

with base as (SELECT
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
    order by PROC_INST_ID_, TIME_, NAME_
    )

/* 	TM1andMAPS has second rand text message 'tm1AndMAPSMessage' labelled as 'message1SentText' which has the same label as the numbering of TMs (non MAPS notification TMs)
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

/*, base_v3 as (select * from base_v2 union all select * FROM clarified_resp) */

, msg_order as (SELECT 
	*
    , ROW_NUMBER() OVER( PARTITION BY PROC_INST_ID_ ORDER BY TIME_, FIELD(textType, 'response','sentClarification','sentConfirmation','sentNoContact','sentMotivationalTM','sentMAPSnotification'))	message_order 
    , ROW_NUMBER() OVER( PARTITION BY PROC_INST_ID_, textType ORDER BY TIME_, NAME_)	message_type_order
FROM base_v2
order by PROC_INST_ID_, textType, TIME_, NAME_)

select * from msg_order;