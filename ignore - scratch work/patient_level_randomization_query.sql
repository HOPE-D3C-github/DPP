select 
	p1.patient_cd
    , p1.randomization_value			firsthalf_randomization    
    , p23.randomization_value			firsthalf_msg_randomization
    , p1.randomization_date				firsthalf_randomization_date
    , p4.randomization_value			secondhalf_randomization
    , p56.randomization_value			secondhalf_msg_randomization
    , p4.randomization_date				secondhalf_randomization_date
	from
	(select *
		from bmi_randomization_schedule_dpp 
			where randomization_group_cd = 'Phase1Treatment' and
				patient_cd is not null) p1
	left join      
		(select *
			from bmi_randomization_schedule_dpp 
				where randomization_group_cd in ('Phase2Treatment', 'Phase3Treatment') and
					patient_cd is not null) p23
		on p23.patient_cd = p1.patient_cd      
	left join 
		( select * 
			from bmi_randomization_schedule_dpp 
				where randomization_group_cd in ('Phase4_1Treatment', 'Phase4_2Treatment') and
					patient_cd is not null) p4
		on p4.patient_cd = p1.patient_cd     
	left join
		( select *
			from bmi_randomization_schedule_dpp 
				where randomization_group_cd in ('Phase5_1Treatment', 'Phase6_1Treatment') and
					patient_cd is not null) p56
		on p56.patient_cd = p1.patient_cd;
                    
                    
                    
                    