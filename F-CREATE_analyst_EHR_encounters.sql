/* 
Summary: Update the raw encounters EHR data by crosswalking MRN to uid and updating date format.

Inputs:	
		analyst.raw_EHR_encounters
		analyst.C_crosswalk_uid_to_MRN	

Outputs:	analyst.F_EHR_encounters

*/
DROP TABLE IF EXISTS analyst.F_EHR_encounters;
CREATE TABLE analyst.F_EHR_encounters

SELECT
	cw.uid    
    , STR_TO_DATE(ENCOUNTER_DT, '%d-%b-%Y') ENCOUNTER_DT
	, ENCOUNTER_CSN_ID, DX_ID, DIAGNOSIS, CURRENT_ICD10_LIST
FROM ( SELECT * FROM analyst.raw_EHR_encounters) ec
LEFT JOIN analyst.C_crosswalk_uid_to_MRN cw
ON ec.MRN = cw.MRN;