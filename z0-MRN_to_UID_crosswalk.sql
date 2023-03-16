/*
Summary: Inspect distinct MRN from EHR datasets. Create UID crosswalk with MRN. Write crosswalk table to analyst schema
*/
-- SELECT DISTINCT MRN FROM analyst.raw_EHR_demographics; /*2106*/
-- SELECT DISTINCT MRN FROM analyst.raw_EHR_encounters;   /*2106*/
-- SELECT DISTINCT MRN FROM analyst.raw_EHR_pt_prob_list; /*2006*/
-- SELECT DISTINCT MRN FROM analyst.raw_EHR_race; /*2106*/
DROP TABLE IF EXISTS analyst.z0_crosswalk_uid_to_MRN;
CREATE TABLE analyst.z0_crosswalk_uid_to_MRN

SELECT 
	DISTINCT MRN
    , CONVERT(CONCAT( 'DPP:', ROW_NUMBER() OVER (ORDER BY MRN)), CHAR(750)) uid
FROM analyst.raw_EHR_demographics;














