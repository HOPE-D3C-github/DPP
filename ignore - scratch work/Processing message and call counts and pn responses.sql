select 
resp.PROC_INST_ID_
, resp.TEXT_ 		MAPSCallOutcome
, resp.TIME_		MapsCall_hrts_GMT
, if(resp.TIME_ < coalesce(msg2_time, '2023-01-01 01:01:01'), 1, if(resp.TIME_ < coalesce(msg3_time, '2023-01-01 01:01:01'), 2, 3))		msg_count
, callcnt.MAPSCallCount
/*, callcnt.TIME_					remove_CallTime
, msg1.msg1_time
, msg2.msg2_time
, msg3.msg3_time	*/
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
order by PROC_INST_ID_, MapsCall_hrts_GMT
    ;
    
    
    
with mapsCalls as (select 
resp.PROC_INST_ID_
, resp.TEXT_ 		MAPSCallOutcome
, resp.TIME_		MapsCall_hrts_GMT
, if(resp.TIME_ < coalesce(msg2_time, '2023-01-01 01:01:01'), 1, if(resp.TIME_ < coalesce(msg3_time, '2023-01-01 01:01:01'), 2, 3))		msg_count
, callcnt.MAPSCallCount
/*, callcnt.TIME_					remove_CallTime
, msg1.msg1_time
, msg2.msg2_time
, msg3.msg3_time	*/
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

select msg_count, MAPSCallCount, count(1) N from mapsCalls group by msg_count, MAPSCallCount
    ;