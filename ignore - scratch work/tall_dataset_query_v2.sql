use workflow;


SELECT 
    BUSINESS_KEY_ patient_cd, pd.NAME_ wf_name, un.*
FROM
    (SELECT 
        dt.PROC_INST_ID_,
            'ACT_HI_DETAIL' AS source_tbl,
            ai.ACT_ID_,
            NAME_,
            VAR_TYPE_,
            COALESCE(DOUBLE_, LONG_, TEXT_) var_value,
            TIME_ START_TIME_,
            TIME_ END_TIME_,
            ai.TRANSACTION_ORDER_,
            NULL AS DURATION_
    FROM
        ACT_HI_DETAIL dt
    LEFT JOIN (SELECT 
        PROC_INST_ID_, ID_, ACT_ID_, TRANSACTION_ORDER_
    FROM
        ACT_HI_ACTINST) ai ON ai.PROC_INST_ID_ = dt.PROC_INST_ID_
        AND ai.ID_ = dt.ACT_INST_ID_ UNION ALL SELECT 
        PROC_INST_ID_,
            'ACT_HI_ACTINST' AS source,
            ACT_ID_,
            ACT_NAME_ NAME_,
            NULL AS VAR_TYPE_,
            NULL AS var_value,
            START_TIME_,
            END_TIME_,
            TRANSACTION_ORDER_,
            DURATION_
    FROM
        ACT_HI_ACTINST) un
        LEFT JOIN
    (SELECT 
        PROC_INST_ID_, BUSINESS_KEY_, PROC_DEF_ID_
    FROM
        ACT_HI_PROCINST) pi ON pi.PROC_INST_ID_ = un.PROC_INST_ID_
        LEFT JOIN
    (SELECT 
        ID_, NAME_
    FROM
        ACT_RE_PROCDEF) pd ON pd.ID_ = pi.PROC_DEF_ID_
ORDER BY patient_cd , START_TIME_ , END_TIME_ , TRANSACTION_ORDER_ , source_tbl
	;


