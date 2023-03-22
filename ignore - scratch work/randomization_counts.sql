use workflow;
select randomization_group_cd, randomization_value, count(1) from bmi_randomization_schedule_dpp where patient_cd is not null group by randomization_group_cd, randomization_value;