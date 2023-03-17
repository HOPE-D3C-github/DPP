/* 
Summary: Update the race_tall table to one race per participant, remove PHI from raw_EHR_demographics, combine race and demographics, write to DB analyst schema

1. Creating a patient-level race dataset (Race categorized as "Multiracial" if more than 1 of the updated race categories)

*/


/* STEP 1. Creating a patient-level race dataset (Race categorized as "Multiracial" if more than 1 of the updated race categories) */
with pt_race as (SELECT
	uid
    , IF( count(1) = 1, race, "Multiracial") pt_race 
FROM analyst.z1_race_tall
GROUP BY uid)


SELECT * FROM pt_race

; 