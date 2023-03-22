/*
This script was modified from the phase 1 consort query, to be a wide per patient dataset of indicators
*/


use workflow;

select *
	from (select        chc.name_abbrev                                          CHC
     , concat(pdef.NAME_,':',coh.name)                          Workflow_Cohort
     , pi.PROC_INST_ID_
     , pi.START_TIME_  
     , pi.END_TIME_    
     , if (opt.patient_cd is null, 0, 1) GlobalOptOut
     , if ( a0.PROC_INST_ID_ is not null, 1, 0)  RandControl 
     , a0.START_TIME_	RandControl_START_TIME_
     , a0.END_TIME_		RandControl_END_TIME_
     , if ( a1.PROC_INST_ID_ is not null, 1, 0)  AllocatedTMPlusorTM1 
     , if ( a2.PROC_INST_ID_ is not null, 1, 0)  optOutAfterInitialEmail 
     , if ( a3.PROC_INST_ID_ is not null, 1, 0)  TotalOptOutText
     , if ( a4.PROC_INST_ID_ is not null, 1, 0)  tmPlusFirstMessage 
     , if ( a5.PROC_INST_ID_ is not null, 1, 0)  RandTMPlus 
     , if ( a6.PROC_INST_ID_ is not null, 1, 0)  endTmPlusNoResponse 
     , if ( a7.PROC_INST_ID_ is not null, 1, 0)  TMPlusOptOutText 
     , if ( a8.PROC_INST_ID_ is not null, 1, 0)  TMPlusTextYes 
     , if ( a9.PROC_INST_ID_ is not null, 1, 0)  tm1Message 
     , if ( a10.PROC_INST_ID_ is not null, 1, 0)  RandTM1 
     , if ( a11.PROC_INST_ID_ is not null, 1, 0)  fourteenWeekNoResponseTm1 
     , if ( a12.PROC_INST_ID_ is not null, 1, 0)  TM1TextOptOut 
     , if ( a13.PROC_INST_ID_ is not null, 1, 0)  TM1TextYes 
     , if ( a14.PROC_INST_ID_ is not null, 1, 0)  YesTM1staffreview 
     , if ( a15.PROC_INST_ID_ is not null, 1, 0)  TotalContactedByDPP 
     , if ( a15.PROC_INST_ID_ is not null and a5.PROC_INST_ID_ is not null, 1, 0)  TMPlusContactedByDPP 
     , if ( a15.PROC_INST_ID_ is not null and a10.PROC_INST_ID_ is not null, 1, 0)  TM1ContactedByDPP 
     
     , if ( a16.PROC_INST_ID_ is not null, 1, 0)  TotaldppDeclined 
     , if ( a16.PROC_INST_ID_ is not null and a5.PROC_INST_ID_ is not null, 1, 0)  TMPlusdppDeclined 
     , if ( a16.PROC_INST_ID_ is not null and a10.PROC_INST_ID_ is not null, 1, 0)  TM1dppDeclined 
     
     -- , if ( a17.PROC_INST_ID_ is not null, 1, 0)  iTotalLineToDppDeclined 
     , if ( a18.PROC_INST_ID_ is not null, 1, 0)  TotaldppUnreachable 
     , if ( a18.PROC_INST_ID_ is not null and a5.PROC_INST_ID_ is not null, 1, 0)  TMPlusdppUnreachable
     , if ( a18.PROC_INST_ID_ is not null and a10.PROC_INST_ID_ is not null, 1, 0)  TM1dppUnreachable
     
     , if ( a19.PROC_INST_ID_ is not null, 1, 0)  TotaloptOutDPPContact 
     , if ( a19.PROC_INST_ID_ is not null and a5.PROC_INST_ID_ is not null, 1, 0)  TMPlusoptOutDPPContact
     , if ( a19.PROC_INST_ID_ is not null and a10.PROC_INST_ID_ is not null, 1, 0)  TM1optOutDPPContact
     
     , if ( a20.PROC_INST_ID_ is not null, 1, 0)  TotalpnEngaged 
     , if ( a20.PROC_INST_ID_ is not null and a5.PROC_INST_ID_ is not null, 1, 0)  TMPluspnEngaged
     , if ( a20.PROC_INST_ID_ is not null and a10.PROC_INST_ID_ is not null, 1, 0)  TM1pnEngaged
     
     
     -- , if ( a21.PROC_INST_ID_ is not null, 1, 0)  iTotalLineToEngaged 
     , if ( a22.PROC_INST_ID_ is not null, 1, 0)  TotalDPPenrolledWithID 
     , if ( a22.PROC_INST_ID_ is not null and a5.PROC_INST_ID_ is not null, 1, 0)  TMPlusDPPenrolledWithID 
     , if ( a22.PROC_INST_ID_ is not null and a10.PROC_INST_ID_ is not null, 1, 0)  TM1DPPenrolledWithID 
     
     , if ( a30.PROC_INST_ID_ is not null, 1, 0)  EligibleForRandomization
     , if ( a31.PROC_INST_ID_ is not null, 1, 0)  RandomizedTMPlusorTM1
     
from bmi_clinic_patient pt
join ACT_HI_PROCINST pi on pt.patient_cd = pi.BUSINESS_KEY_ 
left join bmi_pat_global_opt_out_v opt on opt.patient_cd = pt.patient_cd 
join bmi_clinic clinic on clinic.clinic_id = pt.clinic_id and pt.clinic_id > 0 
join bmi_chc chc on chc.chc_id = clinic.chc_id 
join ACT_RE_PROCDEF pdef on pdef.ID_ = pi.PROC_DEF_ID_ 
join bmi_cohort_process_instance cpi on cpi.proc_inst_id_ = pi.PROC_INST_ID_
join bmi_cohort coh on coh.cohort_id = cpi.cohort_id
join bmi_report_workflow rw on rw.proc_def_key = pdef.KEY_ and rw.report_id = 4192 
left join ACT_HI_ACTINST a0 on a0.PROC_INST_ID_ = pi.PROC_INST_ID_  and a0.ACT_ID_  = 'endControl'
left join ACT_HI_ACTINST a1 on a1.PROC_INST_ID_ = pi.PROC_INST_ID_  and a1.ACT_ID_  = 'sid-5CE863C6-03EE-4DE8-84C3-6A844628A171' 
left join ACT_HI_ACTINST a2 on a2.PROC_INST_ID_ = pi.PROC_INST_ID_  and a2.ACT_ID_  = 'optOutAfterInitialEmail'
left join ACT_HI_ACTINST a3 on a3.PROC_INST_ID_ = pi.PROC_INST_ID_  and a3.ACT_ID_  = 'endOptOut'
left join (select aii.PROC_INST_ID_, count(1) cnt from ACT_HI_ACTINST aii where aii.ACT_ID_  = 'tmPlusMessage'
 group by aii.PROC_INST_ID_) a4 on a4.PROC_INST_ID_ = pi.PROC_INST_ID_ 
left join ACT_HI_ACTINST a5 on a5.PROC_INST_ID_ = pi.PROC_INST_ID_  and a5.ACT_ID_  = 'sid-A8F22EDC-5EA7-42EE-AEC3-38716A13D69C'
left join ACT_HI_ACTINST a6 on a6.PROC_INST_ID_ = pi.PROC_INST_ID_  and a6.ACT_ID_  = 'endTmPlusNoResponse' 
-- and a6.END_TIME_ is null 

left join ACT_HI_ACTINST a7 on a7.PROC_INST_ID_ = pi.PROC_INST_ID_  and a7.ACT_ID_  = 'sid-1C8FB50E-7E5B-4046-A3C2-33FDC4BB3746' 
left join ACT_HI_ACTINST a8 on a8.PROC_INST_ID_ = pi.PROC_INST_ID_  and a8.ACT_ID_  = 'confirmationTmPlus'
left join (select aii.PROC_INST_ID_, count(1) cnt from ACT_HI_ACTINST aii where aii.ACT_ID_  = 'tm1Message'
 group by aii.PROC_INST_ID_) a9 on a9.PROC_INST_ID_ = pi.PROC_INST_ID_ 
left join ACT_HI_ACTINST a10 on a10.PROC_INST_ID_ = pi.PROC_INST_ID_  and a10.ACT_ID_  = 'sid-E4D4D207-B738-404A-8C22-719446191C6B'
left join ACT_HI_ACTINST a11 on a11.PROC_INST_ID_ = pi.PROC_INST_ID_  and a11.ACT_ID_  = 'fourteenWeekNoResponseTm1'
left join ACT_HI_ACTINST a12 on a12.PROC_INST_ID_ = pi.PROC_INST_ID_  and a12.ACT_ID_  = 'sid-45059D84-28B0-47FD-84A1-51C26D661C39'
left join ACT_HI_ACTINST a13 on a13.PROC_INST_ID_ = pi.PROC_INST_ID_  and a13.ACT_ID_  = 'confirmationTm1'
left join ACT_HI_ACTINST a14 on a14.PROC_INST_ID_ = pi.PROC_INST_ID_  and a14.ACT_ID_  = 'sid-8E631827-5EB8-4FD1-8C34-CD4B63512602'
left join ACT_HI_ACTINST a15 on a15.PROC_INST_ID_ = pi.PROC_INST_ID_  and a15.ACT_ID_  = 'contactedByDPP'
left join ACT_HI_ACTINST a16 on a16.PROC_INST_ID_ = pi.PROC_INST_ID_  and a16.ACT_ID_  = 'dppDeclined'
left join ACT_HI_ACTINST a17 on a17.PROC_INST_ID_ = pi.PROC_INST_ID_  and a17.ACT_ID_  = 'sid-8F64D4F0-2778-4293-9941-44548B0DF7FB'
left join ACT_HI_ACTINST a18 on a18.PROC_INST_ID_ = pi.PROC_INST_ID_  and a18.ACT_ID_  = 'unreachable'
left join ACT_HI_ACTINST a19 on a19.PROC_INST_ID_ = pi.PROC_INST_ID_  and a19.ACT_ID_  = 'optOutDPPContact'
left join ACT_HI_ACTINST a20 on a20.PROC_INST_ID_ = pi.PROC_INST_ID_  and a20.ACT_ID_  = 'pnEngaged'
left join ACT_HI_ACTINST a21 on a21.PROC_INST_ID_ = pi.PROC_INST_ID_  and a21.ACT_ID_  = 'sid-3183A6A9-4FE1-4FC9-9B69-D0507ADD0E41'
left join ACT_HI_ACTINST a22 on a22.PROC_INST_ID_ = pi.PROC_INST_ID_  and a22.ACT_ID_  = 'enrolled'

left join ACT_HI_ACTINST a30 on a30.PROC_INST_ID_ = pi.PROC_INST_ID_  and a30.ACT_ID_  = 'sid-D49DA966-A815-47F9-89FB-152A8D0996D7'
left join ACT_HI_ACTINST a31 on a31.PROC_INST_ID_ = pi.PROC_INST_ID_  and a31.ACT_ID_  = 'sid-0FD482E8-0A4C-416F-8F39-434F445B40B6'

) oo 
