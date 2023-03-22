use workflow;
DROP TABLE IF EXISTS analyst.wide_cur_dat_4_dm;
CREATE TABLE analyst.wide_cur_dat_4_dm

with wide_w_outcomes as (select 
	a1.patient_cd
    , pl.patient_language		primary_language
    , CONVERT( a2.PROC_INST_ID_, CHAR(75)) 		firstphase_PROC_INST_ID
    , CONVERT( a3.PROC_INST_ID_, CHAR(75)) 		secondphase_PROC_INST_ID
    , a4.START_TIME_		firstphase_rand_hrts_GMT
    , a4.TEXT_			firstphase_msg_freq_rand 			/* 1st phase message frequency randomization: TM1 versus TM+ */
    , a5.TEXT_			firstphase_msg_type_rand 			/* 1st phase message type randomization: Autonomy vs Directive vs Mixed */
    , a6.START_TIME_		secondphase_rand_hrts_GMT			/* @TB move this as the first of the 2nd phase workflow columns, when starting 2nd phase */
    , a6.TEXT_			secondphase_msg_maps_freq_rand 		
    /* 2nd phase message and MAPS frequency randomization: 
    TM1 could have either 'Nothing' or 'MAPS'
    TM2 could have either 'TM-CONT' or 'TM+MAPS' */
    , coalesce(a7.TEXT_, a8.TEXT_) 		secondphase_msg_type_rand		
    /* second phase message type was stored in either 'phase5RandomizationGroup' or 'phase6RandomizationGroup', 
    depending on their first phase randomization. coalesce grabs the one not-null, if any */
	
	, a9.START_TIME_		optOutAfterInitialEmail_hrts_GMT
	
    , tm1.tm_sent_hrts_GMT				firstphase_tm1_sent_hrts_GMT
    , tm1.last_response_time			firstphase_tm1_last_response_hrts_GMT
    , tm1.last_response_categorical		firstphase_tm1_last_response_categ    
    , tm2.tm_sent_hrts_GMT				firstphase_tm2_sent_hrts_GMT
    , tm2.last_response_time			firstphase_tm2_last_response_hrts_GMT
    , tm2.last_response_categorical		firstphase_tm2_last_response_categ    
    , tm3.tm_sent_hrts_GMT				firstphase_tm3_sent_hrts_GMT
    , tm3.last_response_time			firstphase_tm3_last_response_hrts_GMT
    , tm3.last_response_categorical		firstphase_tm3_last_response_categ    
    , tm4.tm_sent_hrts_GMT				firstphase_tm4_sent_hrts_GMT
    , tm4.last_response_time			firstphase_tm4_last_response_hrts_GMT
    , tm4.last_response_categorical		firstphase_tm4_last_response_categ    
    , tm5.tm_sent_hrts_GMT				firstphase_tm5_sent_hrts_GMT
    , tm5.last_response_time			firstphase_tm5_last_response_hrts_GMT
    , tm5.last_response_categorical		firstphase_tm5_last_response_categ  
  
    , a21.START_TIME_		firstphase_optout_hrts_GMT
    , a22.START_TIME_		firstphase_responseYES_hrts_GMT
    , a23.START_TIME_		firstphase_uHealth_contact_hrts_GMT
    , CONVERT( a23.TEXT_, CHAR(50))				firstphase_uHealth_contact_outcome
    , a25.START_TIME_		firstphase_dppEnrolled_hrts_GMT
    , CONVERT( a26.TEXT_, CHAR(50))				firstphase_IncentaHealthID
    , CONVERT( a27.concat_notes, CHAR(500))		firstphase_concatenated_notes
    
    /*	START Second Stage info				*/
    /*, b1.END_TIME_		secondphase_rand_hrts_GMT */
    
	, p2tm1.tm_sent_hrts_GMT				secondphase_tm1_sent_hrts_GMT
    , p2tm1.last_response_time				secondphase_tm1_last_response_hrts_GMT
    , CONVERT( p2tm1.last_response_categorical, CHAR(50))		secondphase_tm1_last_response_categ        
    , maps1.time_of_earliest_call_per_msg_number		MAPS_1_first_call_hrts_GMT
    , maps1.lastvalue_time								MAPS_1_last_call_hrts_GMT    
    , CONVERT(maps1.call_count, UNSIGNED)				MAPS_1_call_count
    , CONVERT( maps1.maps_outcome, CHAR(50))			MAPS_1_outcome
    , p2tm2.tm_sent_hrts_GMT							secondphase_tm2_sent_hrts_GMT
    , p2tm2.last_response_time							secondphase_tm2_last_response_hrts_GMT
    , CONVERT (p2tm2.last_response_categorical, CHAR(50))		secondphase_tm2_last_response_categ    
    , maps2.time_of_earliest_call_per_msg_number		MAPS_2_first_call_hrts_GMT
    , maps2.lastvalue_time								MAPS_2_last_call_hrts_GMT    
    , CONVERT (maps2.call_count, UNSIGNED)				MAPS_2_call_count
    , CONVERT( maps2.maps_outcome, CHAR(50))			MAPS_2_outcome
    , p2tm3.tm_sent_hrts_GMT							secondphase_tm3_sent_hrts_GMT
    , p2tm3.last_response_time							secondphase_tm3_last_response_hrts_GMT
    , CONVERT( p2tm3.last_response_categorical, CHAR(50))		secondphase_tm3_last_response_categ 
    , maps3.time_of_earliest_call_per_msg_number		MAPS_3_first_call_hrts_GMT
    , maps3.lastvalue_time								MAPS_3_last_call_hrts_GMT    
    , CONVERT( maps3.call_count, UNSIGNED)				MAPS_3_call_count
    , CONVERT( maps3.maps_outcome, CHAR(50))			MAPS_3_outcome
    
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
    , b4.START_TIME_		secondphase_uHealth_contact_hrts_GMT
    , b4.TEXT_				secondphase_uHealth_contact_outcome
    , b5.START_TIME_		secondphase_dppEnrolled_hrts_GMT
    , b6.TEXT_				secondphase_IncentaHealthID
    , CONVERT( b7.concat_notes, CHAR(7500))		secondphase_concatenated_notes
        
from 
	( select patient_cd from analyst.tall_dat group by patient_cd) a1
left join
	( select patient_cd, patient_language from bmi_clinic_patient) pl
    on pl.patient_cd = a1.patient_cd    
left join
	( select patient_cd, PROC_INST_ID_ from analyst.tall_dat where wf_name = 'DPP V1' group by PROC_INST_ID_ ) a2
    on a2.patient_cd = a1.patient_cd 
left join 
	( select patient_cd, PROC_INST_ID_ from analyst.tall_dat where wf_name != 'DPP V1' group by PROC_INST_ID_) a3
    on a3.patient_cd = a1.patient_cd 
left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from analyst.tall_dat where NAME_ = 'phase1RandomizationGroup') a4
    on a4.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from analyst.tall_dat where NAME_ = 'phase2RandomizationGroup') a5
    on a5.PROC_INST_ID_ = a2.PROC_INST_ID_
   
left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from analyst.tall_dat where NAME_ = 'phase4RandomizationGroup') a6
    on a6.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from analyst.tall_dat where NAME_ = 'phase5RandomizationGroup') a7
    on a7.PROC_INST_ID_ = a3.PROC_INST_ID_    
left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from analyst.tall_dat where NAME_ = 'phase6RandomizationGroup') a8
    on a8.PROC_INST_ID_ = a3.PROC_INST_ID_     
left join
	( select PROC_INST_ID_, START_TIME_ from analyst.tall_dat where ACT_ID_ = 'optOutAfterInitialEmail') a9
    on a9.PROC_INST_ID_ = a2.PROC_INST_ID_
/* Grabbing datetimes for Opt-out and DPP states. Presence of a datetime indicates they were ever in that state - can make indicators easily from that */
left join
	( select PROC_INST_ID_, START_TIME_ from analyst.tall_dat where ACT_ID_ = 'endOptOut') a21
	on a21.PROC_INST_ID_ = a2.PROC_INST_ID_
 left join
	( select PROC_INST_ID_, START_TIME_ from analyst.tall_dat where
		ACT_ID_ in ('sid-923DFB3A-03EC-42B8-8D7C-44E45EA47CD6', 'sid-27A4E17F-D6D7-46CE-84FC-D5B8A091AB42')) a22
	on a22.PROC_INST_ID_ = a2.PROC_INST_ID_
 left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from analyst.tall_dat where 
		NAME_ = 'dppResponse' ) a23
	on a23.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select PROC_INST_ID_, START_TIME_ from analyst.tall_dat where ACT_ID_ = 'enrolled') a25
    on a25.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select PROC_INST_ID_, TEXT_ from analyst.tall_dat where
		( ACT_ID_ = 'pnEngaged' AND NAME_ = 'incentaHealthID' )) a26
	on a26.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select PROC_INST_ID_, group_concat(TEXT_ separator '---\n') concat_notes 
		from analyst.tall_dat where NAME_ = '_notes' group by PROC_INST_ID_) a27
	on a27.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select * from analyst.tm_response_summarized where message_type_order = 1) tm1
    on tm1.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select * from analyst.tm_response_summarized where message_type_order = 2) tm2
    on tm2.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select * from analyst.tm_response_summarized where message_type_order = 3) tm3
    on tm3.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select * from analyst.tm_response_summarized where message_type_order = 4) tm4
    on tm4.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select * from analyst.tm_response_summarized where message_type_order = 5) tm5
    on tm5.PROC_INST_ID_ = a2.PROC_INST_ID_
    
/*	Second Phase Indicators				*/
left join
	( select PROC_INST_ID_, END_TIME_ from analyst.tall_dat where ACT_ID_ = 'setRandomizationFromDB' group by PROC_INST_ID_) b1
    on b1.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select PROC_INST_ID_, ACT_ID_, START_TIME_ from analyst.tall_dat where ACT_ID_ in ('sid-CC2EA9ED-95AF-4F47-98AD-950D8A597E4F' /* TM+ MAPS TEXT opt-out */,
			'sid-73CC9859-DCF9-42F1-AF9B-8DA8F9CB5B2C' /* TM+ TEXT opt-out */, 'sid-20A255E7-3DEA-42DB-B2DE-E6EA09D9CF45' /* TM+ MAPS Call opt-out*/) group by PROC_INST_ID_ ) b2	
    on b2.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select PROC_INST_ID_, ACT_ID_, START_TIME_ from analyst.tall_dat where ACT_ID_ in ('sid-466385EC-CA34-4121-ABCF-24D2981D266E' /*TM1 MAPS*/ , 'sid-8A937BCA-B2E0-4C1F-8459-792C5F0D2B22' /*TM1 text*/, 
	'sid-059C1580-DDAB-48EB-A081-ACAF16AC58DC' /* TM+ MAPS */, 'confirmationTmPlus' /* TM+ text*/) group by PROC_INST_ID_ ) b3
    on b3.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from analyst.tall_dat where 
		NAME_ = 'dppResponse' ) b4
	on b4.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select PROC_INST_ID_, START_TIME_ from analyst.tall_dat 
		where ACT_ID_ in ('enrolled' /* TM1 */,'enrolledTMPlus' /* TM+ */) ) b5
	on b5.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select PROC_INST_ID_, TEXT_ from analyst.tall_dat where
		( ACT_ID_ = 'pnEngaged' AND NAME_ = 'incentaHealthID' ) ) b6
	on b6.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select PROC_INST_ID_, group_concat(TEXT_ separator '---\n') concat_notes 
		from analyst.tall_dat where NAME_ = '_notes' group by PROC_INST_ID_) b7
	on b7.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select * from analyst.mapsCalls_outcomes where maps_msg_number = 1 ) maps1
	on maps1.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select * from analyst.mapsCalls_outcomes where maps_msg_number = 2 ) maps2
	on maps2.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select * from analyst.mapsCalls_outcomes where maps_msg_number = 3 ) maps3
	on maps3.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select * from analyst.tm_response_summarized where message_type_order = 1) p2tm1
    on p2tm1.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select * from analyst.tm_response_summarized where message_type_order = 2) p2tm2
    on p2tm2.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select * from analyst.tm_response_summarized where message_type_order = 3) p2tm3
    on p2tm3.PROC_INST_ID_ = a3.PROC_INST_ID_)

   
/* Known issue where TM+ pts didnt recieve their 5th TM in first phase	*/
/**/
, no5th_tm_p1 as (SELECT
	firstphase_PROC_INST_ID, firstphase_msg_freq_rand, firstphase_tm4_sent_hrts_GMT, firstphase_tm5_sent_hrts_GMT
    , firstphase_optout_hrts_GMT, firstphase_responseYES_hrts_GMT
FROM wide_w_outcomes
WHERE firstphase_msg_freq_rand = 'TM+' AND firstphase_optout_hrts_GMT is null AND firstphase_responseYES_hrts_GMT 
	is null AND firstphase_tm5_sent_hrts_GMT is null AND optOutAfterInitialEmail_hrts_GMT is null)
/**/


/* Known issue where TM+ No MAPS pts didn't recieve their 3rd TM in second phase 	*/
/**/
, no3rd_tm_p2 as (SELECT
	secondphase_PROC_INST_ID
    , secondphase_msg_maps_freq_rand
    , secondphase_tm2_sent_hrts_GMT
    , secondphase_tm3_sent_hrts_GMT
    , secondphase_optout_hrts_GMT
    , secondphase_responseYES_hrts_GMT
FROM wide_w_outcomes
WHERE secondphase_msg_maps_freq_rand in ('TM-CONT', 'TM+MAPS') AND secondphase_optout_hrts_GMT is null AND secondphase_responseYES_hrts_GMT is null AND secondphase_tm3_sent_hrts_GMT is null)

SELECT 
	wi.*
    , if (n5.firstphase_PROC_INST_ID is not null, 1, 0)		issue_A_p1_no_5th_TM
    , if (n3.secondphase_PROC_INST_ID is not null, 1, 0) 	issue_B_p2_no_3rd_TM
FROM ( SELECT * FROM wide_w_outcomes) wi 
LEFT JOIN 
	( SELECT firstphase_PROC_INST_ID from no5th_tm_p1) n5
on n5.firstphase_PROC_INST_ID = wi.firstphase_PROC_INST_ID
LEFT JOIN
	( SELECT secondphase_PROC_INST_ID from no3rd_tm_p2) n3
on n3.secondphase_PROC_INST_ID = wi.secondphase_PROC_INST_ID

;
/**/
/* People from 1st workflow that we are waiting for their data, may need to hard code their incentahealth id later*/
/* 
Create new unique ID w/o phi	
GMT to local time
add standardized responses for each inbound text
cast or convert for the text fields to enable table creation w/o error
add timetosend for each message. message 1 time to send is when it was sent. Add to tall and wide (before the intervention message was sent)
*/

	;
