/*
Summary: Inspect distinct MRN from EHR datasets. Create UID crosswalk with MRN. Write crosswalk table to analyst schema
*/
DROP TABLE IF EXISTS analyst.C_crosswalk_uid_to_MRN;
CREATE TABLE analyst.C_crosswalk_uid_to_MRN

with v1 as (SELECT 
	DISTINCT MRN
    , CONCAT('DPP:', MRN)	patient_cd
    , CONVERT(CONCAT( 'DPP:', ROW_NUMBER() OVER (ORDER BY MRN)), CHAR(750)) uid
FROM analyst.raw_EHR_demographics)

SELECT v1.*, pi.PROC_INST_ID_ FROM v1
LEFT JOIN (SELECT PROC_INST_ID_, BUSINESS_KEY_ FROM workflow.ACT_HI_PROCINST) pi
ON v1.patient_cd = pi.BUSINESS_KEY_
;