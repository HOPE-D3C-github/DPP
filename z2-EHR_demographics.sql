/* 
Summary: Update the race_tall table to one race per participant, remove PHI from raw_EHR_demographics, combine race and demographics, write to DB analyst schema

1. Creating a patient-level race dataset (Race categorized as "Multiracial" if more than 1 of the updated race categories)
2. Join raw demographics to uid crosswalk & patient-level race data
3. Remove PHI variables from demographics  (**NOTE: Additional removal may be necessary**)

Inputs:
	analyst.z0_crosswalk_uid_to_MRN
	analyst.z1_race_tall
    analyst.raw_EHR_demographics

Outputs:	analyst.z2_demographics

*/

DROP TABLE IF EXISTS analyst.z2_demographics;
CREATE TABLE analyst.z2_demographics

/* STEP 1. Creating a patient-level race dataset (Race categorized as "Multiracial" if more than 1 of the updated race categories) */
with pt_race as (SELECT
	uid
    , IF( count(1) = 1, race, "Multiracial") pt_race 
FROM analyst.z1_race_tall
GROUP BY uid)
-- SELECT * FROM pt_race

/* STEP 2. Join raw demographics to uid crosswalk & patient-level race data */
, demog_2 as (SELECT
	cw.uid
    , pr.pt_race
    , dg.*
FROM (SELECT * FROM analyst.raw_EHR_demographics) dg
LEFT JOIN
	(SELECT MRN, uid FROM analyst.z0_crosswalk_uid_to_MRN) cw
ON dg.MRN = cw.MRN
LEFT JOIN 
	(SELECT * FROM pt_race) pr
ON cw.uid = pr.uid)

-- SELECT * FROM demog_2;

/* STEP 3. Remove PHI variables from demographics (**NOTE: Additional removal may be necessary**)*/
SELECT 
	uid, pt_race
    , year(STR_TO_DATE(BIRTH_DATE, '%d-%b-%Y')) birth_year
    , SEX, LANGUAGE, ETHNICITY
    , STR_TO_DATE(LAST_PCP_ENCOUNTER_DT, '%d-%b-%Y') last_pcp_encounter_dt
    ,HEIGHT, HEIGHT_UNITS, WEIGHT, WEIGHT_UNITS, BMI, CITY, ZIP

FROM demog_2
; 