/* Summary: Create a function that can convert datetimes from GMT to a string representing the timestamp in "America/Denver" timezone, accounting for daylight savings.

UTC offset 2021:
America/Denver: 	2021-03-14 02:00 through 2021-11-07 01:59 = -6
GMT: 				2021-03-14 09:00 through 2021-11-07 07:59 = -6

America/Denver: 	2021-11-07 02:00 through 2022-03-13 01:59 = -7
GMT:				2022-11-07 08:00 through 2022-03-13 08:59 = -7

UTC offset 2022:
America/Denver:		2022-03-13 02:00 through 2022-11-06 01:59  = -06:00
GMT:				2022-03-13 09:00 through 2022-11-06 07:59  = -06:00

America/Denver:		2022-11-06 02:00 through 2023-03-12 01:59 = -07:00
GMT:				2022-11-06 08:00 through 2023-03-12 08:59 = -07:00

UTC offset 2023:
America/Denver:		2023-03-12 02:00 through 2023-11-05 01:59 = -06:00
GMT:				2023-03-12 09:00 through 2023-11-05 07:59 = -06:00

*/

DROP FUNCTION IF EXISTS analyst.convert_to_AmDenv;
DELIMITER //
CREATE FUNCTION analyst.convert_to_AmDenv (dt_GMT datetime)
RETURNS TEXT
DETERMINISTIC
BEGIN
	RETURN
		CASE    
			WHEN (timestamp(dt_GMT) BETWEEN timestamp('2021-03-14 09:00') AND timestamp('2021-11-07 07:59')) OR 
					(timestamp(dt_GMT) BETWEEN timestamp('2022-03-13 09:00') AND timestamp('2022-11-06 07:59')) OR 
					(timestamp(dt_GMT) BETWEEN timestamp('2023-03-12 09:00') AND timestamp('2023-11-05 07:59'))
				THEN DATE_FORMAT( CONVERT_TZ(dt_GMT, '+00:00','-06:00'), '%m/%d/%Y %T')
			WHEN timestamp(dt_GMT) BETWEEN timestamp('2022-11-07 08:00') AND timestamp('2022-03-13 08:59') OR
					timestamp(dt_GMT) BETWEEN timestamp('2022-11-06 08:00') AND timestamp('2023-03-12 08:59')
				THEN DATE_FORMAT( CONVERT_TZ(dt_GMT, '+00:00','-07:00'), '%m/%d/%Y %T')
			ELSE NULL 
		END;
END//
DELIMITER ;
