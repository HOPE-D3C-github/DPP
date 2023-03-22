/* 
Summary: Update the raw pt_prob_list EHR data by crosswalking MRN to uid and updating date format.

Inputs:	
		analyst.raw_EHR_pt_prob_list
		analyst.C_crosswalk_uid_to_MRN	

Outputs:	analyst.G_EHR_pt_prob_list

*/
DROP TABLE IF EXISTS analyst.G_EHR_pt_prob_list;
CREATE TABLE analyst.G_EHR_pt_prob_list
SELECT
	cw.uid    
    , STR_TO_DATE(DATE_OF_ENTRY, '%d-%b-%Y') DATE_OF_ENTRY
	, DX_ID, DX_NAME
FROM ( SELECT * FROM analyst.raw_EHR_pt_prob_list) ppl
LEFT JOIN analyst.C_crosswalk_uid_to_MRN cw
ON ppl.MRN = cw.MRN;