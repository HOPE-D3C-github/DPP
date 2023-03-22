/*
Summary: Inspect distinct MRN from EHR datasets. Create UID crosswalk with MRN. Write crosswalk table to analyst schema
*/
DROP TABLE IF EXISTS analyst.C_crosswalk_uid_to_MRN;
CREATE TABLE analyst.C_crosswalk_uid_to_MRN

SELECT 
	DISTINCT MRN
    , CONCAT('DPP:', MRN)	patient_cd
    , CONVERT(CONCAT( 'DPP:', ROW_NUMBER() OVER (ORDER BY MRN)), CHAR(750)) uid
FROM analyst.raw_EHR_demographics;