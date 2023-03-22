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
	( select PROC_INST_ID_, TEXT_, START_TIME_ from tall_dat where NAME_ = 'phase1RandomizationGroup' group by PROC_INST_ID_) a4
    on a4.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from tall_dat where NAME_ = 'phase2RandomizationGroup' group by PROC_INST_ID_) a5
    on a5.PROC_INST_ID_ = a2.PROC_INST_ID_
   
left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from tall_dat where NAME_ = 'phase4RandomizationGroup' group by PROC_INST_ID_) a6
    on a6.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from tall_dat where NAME_ = 'phase5RandomizationGroup' group by PROC_INST_ID_) a7
    on a7.PROC_INST_ID_ = a3.PROC_INST_ID_    
left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from tall_dat where NAME_ = 'phase6RandomizationGroup' group by PROC_INST_ID_) a8
    on a8.PROC_INST_ID_ = a3.PROC_INST_ID_   
left join
	( select PROC_INST_ID_, START_TIME_ from tall_dat where ACT_ID_ = 'optOutAfterInitialEmail' group by PROC_INST_ID_) a9
    on a9.PROC_INST_ID_ = a2.PROC_INST_ID_
/* Grabbing datetimes for Opt-out and DPP states. Presence of a datetime indicates they were ever in that state - can make indicators easily from that */
left join
	( select PROC_INST_ID_, START_TIME_ from tall_dat where ACT_ID_ = 'endOptOut' group by PROC_INST_ID_) a21
	on a21.PROC_INST_ID_ = a2.PROC_INST_ID_
 left join
	( select PROC_INST_ID_, START_TIME_ from tall_dat where
		ACT_ID_ in ('sid-923DFB3A-03EC-42B8-8D7C-44E45EA47CD6', 'sid-27A4E17F-D6D7-46CE-84FC-D5B8A091AB42') group by PROC_INST_ID_) a22
	on a22.PROC_INST_ID_ = a2.PROC_INST_ID_
 left join
	( select PROC_INST_ID_, TEXT_, START_TIME_ from tall_dat where 
		NAME_ = 'dppResponse' group by PROC_INST_ID_) a23
	on a23.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select PROC_INST_ID_, START_TIME_ from tall_dat where ACT_ID_ = 'enrolled' group by PROC_INST_ID_) a25
    on a25.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select PROC_INST_ID_, TEXT_ from tall_dat where
		( ACT_ID_ = 'pnEngaged' AND NAME_ = 'incentaHealthID' ) group by PROC_INST_ID_) a26
	on a26.PROC_INST_ID_ = a2.PROC_INST_ID_
left join
	( select PROC_INST_ID_, group_concat(TEXT_ separator '---\n') concat_notes 
		from tall_dat where NAME_ = '_notes' group by PROC_INST_ID_) a27
	on a27.PROC_INST_ID_ = a2.PROC_INST_ID_
/*	Second Phase Indicators				*/
left join
	( select PROC_INST_ID_, END_TIME_ from tall_dat where ACT_ID_ = 'setRandomizationFromDB' group by PROC_INST_ID_) b1
    on b1.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select PROC_INST_ID_, ACT_ID_, START_TIME_ from tall_dat where ACT_ID_ in ('sid-CC2EA9ED-95AF-4F47-98AD-950D8A597E4F' /* TM+ MAPS TEXT opt-out */,
			'sid-73CC9859-DCF9-42F1-AF9B-8DA8F9CB5B2C' /* TM+ TEXT opt-out */, 'sid-20A255E7-3DEA-42DB-B2DE-E6EA09D9CF45' /* TM+ MAPS Call opt-out*/) group by PROC_INST_ID_ ) b2	
    on b2.PROC_INST_ID_ = a3.PROC_INST_ID_
left join
	( select PROC_INST_ID_, ACT_ID_, START_TIME_ from tall_dat where ACT_ID_ in ('sid-466385EC-CA34-4121-ABCF-24D2981D266E' /*TM1 MAPS*/ , 'sid-8A937BCA-B2E0-4C1F-8459-792C5F0D2B22' /*TM1 text*/, 
	'sid-059C1580-DDAB-48EB-A081-ACAF16AC58DC' /* TM+ MAPS */, 'confirmationTmPlus' /* TM+ text*/) group by PROC_INST_ID_ ) b3
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
-- group by patient_cd
	;
