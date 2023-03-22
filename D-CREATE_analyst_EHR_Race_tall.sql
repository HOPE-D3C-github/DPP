/*
Summary: This script will process the EHR race data by:
1. Replace MRN with UID
2. De-duplicating data
3. Combining categories to updated race categories
4. Creating a tall race dataset and writing to DB Analyst Schema
5*. Creating a patient-level race dataset (Race categorized as "Multiracial" if more than 1 of the updated race categories), joining to demographics data, and writing to DB Analyst Schema **On the next script**
*/

DROP TABLE IF EXISTS analyst.D_EHR_race_tall;
CREATE TABLE analyst.D_EHR_race_tall

/* STEP 1. Replace MRN with UID */
with race_2 as (
	SELECT uid, NAME FROM 
		(SELECT * FROM analyst.raw_EHR_race) rr
		LEFT JOIN 
			(SELECT * FROM analyst.C_crosswalk_uid_to_MRN) cw
		ON rr.MRN = cw.MRN)

/* STEP 2. De-duplicating data */
, dedup_race as (SELECT distinct uid, NAME as original_race FROM race_2 group by uid, NAME) /* 2172 rows */

/* STEP 3. Combining categories to updated race categories and de-duplicate on updated race */
, updated_race as (SELECT distinct
	uid
    , CASE original_race
		WHEN 'Native Hawaiian' THEN 'Pacific Islander'
        WHEN 'Native Hawaiian and Other Pacific Islander' THEN 'Pacific Islander'
        WHEN 'Other Pacific Islander' THEN 'Pacific Islander'
        ELSE original_race
    END AS race 
FROM dedup_race)

/* STEP 4. Final Select statement to write the tall race dataset to the DB analyst scheme */
SELECT * FROM updated_race;