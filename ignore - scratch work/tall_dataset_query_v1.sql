use workflow;

select 
	pi.BUSINESS_KEY_ 			patient_cd
    , pd.NAME_ 					workflow_name
    , rand.firsthalf_randomization
	, rand.secondhalf_randomization
    , dt.NAME_					detail_name
    , dt.VAR_TYPE_				detail_var_type
    , TIME_						detail_time
    , coalesce(dt.DOUBLE_, dt.LONG_, dt.TEXT_, dt.TEXT2_) detail_value 
	, ai.ACT_ID_
    , ai.ACT_NAME_ 
    , ai.START_TIME_
    , ai.END_TIME_
    , ai.DURATION_
    , pi.START_TIME_ wf_start_time
    , pi.END_TIME_ wf_end_time
    , pi.END_ACT_ID_ wf_end_act_id 
    , pi.PROC_INST_ID_
    , rand.firsthalf_msg_randomization
    , rand.firsthalf_randomization_date
	, rand.secondhalf_msg_randomization
    , rand.secondhalf_randomization_date 
	from
		ACT_HI_PROCINST pi
    left join ( select *
		from 
			ACT_RE_PROCDEF) pd
		on pd.ID_ = pi.PROC_DEF_ID_
	left join ( select * 
		from 
			ACT_HI_ACTINST) ai
		on ai.PROC_INST_ID_ = pi.PROC_INST_ID_
    
   left join ( select * 
		from
			ACT_HI_DETAIL) dt
		on dt.PROC_INST_ID_ = ai.PROC_INST_ID_ and
			dt.ACT_INST_ID_ = ai.ID_
    
    left join (
		select 
			p1.patient_cd
			, p1.randomization_value			firsthalf_randomization    
			, p23.randomization_value			firsthalf_msg_randomization
			, p1.randomization_date				firsthalf_randomization_date
			, p4.randomization_value			secondhalf_randomization
			, p56.randomization_value			secondhalf_msg_randomization
			, p4.randomization_date				secondhalf_randomization_date
			from
			(select *
				from bmi_randomization_schedule_dpp 
					where randomization_group_cd = 'Phase1Treatment' and
						patient_cd is not null) p1
			left join      
				(select *
					from bmi_randomization_schedule_dpp 
						where randomization_group_cd in ('Phase2Treatment', 'Phase3Treatment') and
							patient_cd is not null) p23
				on p23.patient_cd = p1.patient_cd      
			left join 
				( select * 
					from bmi_randomization_schedule_dpp 
						where randomization_group_cd in ('Phase4_1Treatment', 'Phase4_2Treatment') and
							patient_cd is not null) p4
				on p4.patient_cd = p1.patient_cd     
			left join
				( select *
					from bmi_randomization_schedule_dpp 
						where randomization_group_cd in ('Phase5_1Treatment', 'Phase6_1Treatment') and
							patient_cd is not null) p56
				on p56.patient_cd = p1.patient_cd) rand
		on rand.patient_cd = pi.BUSINESS_KEY_
    
    where (pd.KEY_ = 'dpp-V1' and
			ai.ACT_ID_ in 
			('dppRandomize',
            'endControl',
			'sid-5CE863C6-03EE-4DE8-84C3-6A844628A171',
			'optOutAfterInitialEmail',
			'endOptOut',
			'tmPlusMessage',
            'sid-205FA4AB-4752-469A-A753-F7C18B455C91', /* wait for response to tm (tm+)*/
			'sid-A8F22EDC-5EA7-42EE-AEC3-38716A13D69C',
            'sid-CBE45FDA-D24A-4A66-8CD3-2F309CC90B26', /* response to tm (tm+)*/
            'sid-36228ADB-EB7A-4FE7-B7A2-0E45C0699D04', /* gateway for responses to tm (tm+)*/
			'endTmPlusNoResponse' ,
			'sid-1C8FB50E-7E5B-4046-A3C2-33FDC4BB3746',
			'confirmationTmPlus',
			'tm1Message',
			'sid-E4D4D207-B738-404A-8C22-719446191C6B',
			'fourteenWeekNoResponseTm1',
			'sid-45059D84-28B0-47FD-84A1-51C26D661C39',
			'confirmationTm1',
			'sid-8E631827-5EB8-4FD1-8C34-CD4B63512602',
			'contactedByDPP',
			'dppDeclined',
			'sid-8F64D4F0-2778-4293-9941-44548B0DF7FB',
			'unreachable',
			'optOutDPPContact',
			'pnEngaged',
			'sid-3183A6A9-4FE1-4FC9-9B69-D0507ADD0E41',
			'enrolled',
			'sid-D49DA966-A815-47F9-89FB-152A8D0996D7',
			'sid-0FD482E8-0A4C-416F-8F39-434F445B40B6'
            ) /* testing  and dt.NAME_ = 'message2SentText' and ai.ACT_ID_ = 'tmPlusMessage' end testing */
            ) 
            OR 
            (pd.KEY_ in ('dppSecondHalfTM1', 'v2dppSecondHalfTM1', 'v3dppSecondHalfTM1') and
			ai.ACT_ID_ in
				( 'wrongRandomizationGroup',
				'sid-4E2600C0-D59D-4596-BCE5-AFFAA5D42F23',
				'tm1Wait',
				'sid-DC42FD30-69D1-4774-A562-58C53C8EC5E2',
				'sid-28989D63-EE3A-4D84-AA87-EBB0D5099ADE',
				'sid-D609D298-D9A5-45AB-B1EA-1FD644DB5B74',
				'tm1AndMAPSMessage',
				'sid-BC8121B9-0401-4A61-A4E4-7694436BB0A9',
				'sid-FB1B740C-A566-481E-BABB-218427B564BA',
				'sid-8A937BCA-B2E0-4C1F-8459-792C5F0D2B22',
				'tm1MAPSCall',
				'sid-45D94EED-FF59-4647-971C-7403C2B00301',
				'endNoAnswerAfterThreeCycles',
				'endNotInterestedTM1AndMAPS',
				'sid-466385EC-CA34-4121-ABCF-24D2981D266E',
				'unreachableTM1AndMAPS',
				'contactedByDPP',
				'sid-77284697-8C67-4658-90FA-68D81A88A7D5',
				'dppDeclinedTM1AndMAPS',
				'sid-8561ECF8-77E2-4AE7-9F1E-555BED90C4E8',
				'enrolled' )            
            )
		order by pi.BUSINESS_KEY_, ai.START_TIME_, ai.TRANSACTION_ORDER_;



