use analyst;


with test as (SELECT 
	uid
    , phase1_rand_hrts_GMT 
    , CONVERT_TZ(phase1_rand_hrts_GMT, '+00:00','-06:00') test_tz
    , CASE 
		WHEN timestamp(phase1_rand_hrts_GMT) BETWEEN timestamp('2022-03-20 00:00:01') AND timestamp('2022-04-01 00:00:0') THEN CONVERT_TZ(phase1_rand_hrts_GMT, '+00:00','-07:00')
        ELSE CONVERT_TZ(phase1_rand_hrts_GMT, '+00:00','-06:00')
	END as test_case_tz     
FROM analyst.H_wide_outcomes_4_analysis)

SELECT 
	*
    , unix_timestamp(phase1_rand_hrts_GMT) unix_gmt
    , unix_timestamp(test_tz) unix_6
    , unix_timestamp(test_case_tz) unix_variable
    -- , DATE_FORMAT(phase1_rand_hrts_GMT, '%m/%d/%Y %T') str_phase1 
    -- , DATE_FORMAT(test_case_tz, '%m/%d/%Y %T') str_variable
    -- , convert_to_AmDenv(phase1_rand_hrts_GMT) unix_var_fnc
FROM test;



SELECT 
	uid
    , phase1_rand_hrts_GMT
    , analyst.convert_to_AmDenv(phase1_rand_hrts_GMT) phase1_rand_hrts_AmDenv
    , phase2_rand_hrts_GMT
    , analyst.convert_to_AmDenv(phase2_rand_hrts_GMT) phase2_rand_hrts_AmDenv
    , total_responseYes_hrts_GMT
    , analyst.convert_to_AmDenv(total_responseYes_hrts_GMT) total_responseYes_hrts_AmDenv
FROM analyst.H_wide_outcomes_4_analysis













