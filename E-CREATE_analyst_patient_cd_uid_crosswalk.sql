USE workflow;
DROP TABLE IF EXISTS analyst.E_crosswalk_uid_to_patient_cd;
CREATE TABLE analyst.E_crosswalk_uid_to_patient_cd

SELECT 
	DISTINCT BUSINESS_KEY_ patient_cd
	, CONVERT(CONCAT( 'DPP:', ROW_NUMBER() OVER ( ORDER BY START_TIME_, PROC_INST_ID_ )), CHAR(750)) uid
FROM workflow.ACT_HI_PROCINST 
group by BUSINESS_KEY_
order by uid;
