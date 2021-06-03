# -*- coding: utf-8 -*-
# @Time    : 2020/12/21
# @Author  : Needn
# @Software: VS Code
from pyneedn.etllib.spark import *

# load tables
spark_table_list = [
    ["stg", "s_cash"],
    ["edw", "d_store"],
    ["edw", "d_payment_method"]
 
]


 # load_d_payment_method
def load_d_payment_method(spark_etl):
    
    # MAX sid,kid
    sql_text = "SELECT MAX(payment_method_kid) AS max_kid, MAX(payment_method_sid) AS max_sid FROM d_payment_method"
    
    ds_max_ids = spark_etl.execute_spark_sql(sql_text, is_return_dataset = True)

    max_kid = str(ds_max_ids[0]["max_kid"] or 0)
    max_sid = str(ds_max_ids[0]["max_sid"] or 0)
    print("MAX payment_method_kid: " + max_kid)
    print("MAX payment_method_sid: " + max_sid)

    sql_text = """
    SELECT 
        {max_sid} + ROW_NUMBER() OVER(ORDER BY cash_no) AS payment_method_sid, cash_no  AS payment_method_no,
        cash_name AS payment_method_name,
        CAST(CURRENT_TIMESTAMP AS DATE) AS eff_from_date, CAST('9999-12-31' AS DATE) AS eff_to_date, TRUE AS is_current
    FROM s_cash AS s
    WHERE NOT EXISTS 
    (
        SELECT * FROM d_payment_method AS d
        WHERE s.cash_no = d.payment_method_no
    )
    """
    format_dict = {"max_kid" : max_kid, "max_sid" : max_sid}
    spark_etl.execute_spark_sql(sql_text, view_name = "new_payment_method", sql_format_dict = format_dict)


    sql_text = """
    SELECT 
        payment_method_kid, payment_method_sid, payment_method_no, payment_method_name,
    eff_from_date, eff_to_date, is_current, run_id, run_timestamp AS run_datetime
    FROM d_payment_method AS d
    WHERE NOT EXISTS 
    (
        SELECT * FROM s_cash   AS s
        WHERE s.cash_no  = d.payment_method_no
    ) OR is_current = FALSE
    """
    spark_etl.execute_spark_sql(sql_text, view_name = "keep_payment_method", sql_format_dict = format_dict)

    
    sql_text = """
    SELECT 
        payment_method_kid, payment_method_sid, payment_method_no, payment_method_name,
        eff_from_date, eff_to_date, is_current, run_id, run_timestamp AS run_datetime
    FROM d_payment_method AS d
    WHERE EXISTS 
    (
        SELECT * FROM s_cash  AS s
        WHERE s.cash_no = d.payment_method_no
    ) AND is_current = TRUE
    """

    spark_etl.execute_spark_sql(sql_text, view_name = "exist_payment_method")

    sql_text = """
    SELECT 
        payment_method_kid, payment_method_sid, payment_method_no, payment_method_name,
    eff_from_date, eff_to_date, is_current, run_id,  run_datetime
    FROM exist_payment_method AS d
    WHERE EXISTS 
    (
        SELECT * FROM s_cash    AS s
        WHERE s.cash_no   = d.payment_method_no
        AND s.cash_name = d.payment_method_name
    ) 
    """
    spark_etl.execute_spark_sql(sql_text, view_name = "no_change_payment_method")


    sql_text = """
    SELECT 
        e.payment_method_sid, s.cash_no as payment_method_no, s.cash_name as payment_method_name,
        CAST(CURRENT_TIMESTAMP AS DATE) AS eff_from_date, CAST('9999-12-31' AS DATE) AS eff_to_date, TRUE AS is_current
    FROM 
    s_cash     AS s
    INNER JOIN
    exist_payment_method AS e
    ON s.cash_no = e.payment_method_no
    WHERE NOT EXISTS 
    (
        SELECT * FROM no_change_payment_method AS n
        WHERE s.cash_no = n.payment_method_no
    ) 
    """
    spark_etl.execute_spark_sql(sql_text, view_name = "new_change_payment_method")

    sql_text = """
    SELECT 
        e.payment_method_kid, e.payment_method_sid, e.payment_method_no, e.payment_method_name,
        e.eff_from_date, CAST(CURRENT_TIMESTAMP AS DATE)  AS eff_to_date, FALSE AS is_current, run_id, run_datetime
    FROM 
    exist_payment_method AS e
    WHERE NOT EXISTS 
    (
        SELECT * FROM no_change_payment_method AS n
        WHERE e.payment_method_sid = n.payment_method_sid
    ) 
    """
    spark_etl.execute_spark_sql(sql_text, view_name = "old_changed_payment_method")


    sql_text = """
    WITH i AS
    (
        SELECT payment_method_sid,payment_method_no,payment_method_name,eff_from_date,eff_to_date,is_current FROM new_payment_method
        UNION ALL
        SELECT payment_method_sid,payment_method_no,payment_method_name,eff_from_date,eff_to_date,is_current FROM new_change_payment_method
    )
    SELECT {max_kid} + ROW_NUMBER() OVER(ORDER BY payment_method_sid) AS payment_method_kid,
        payment_method_sid,payment_method_no,payment_method_name,eff_from_date,eff_to_date,is_current,'{PIPELINE_RUN_ID}' AS run_id,current_timestamp AS run_datetime
    FROM i

    UNION ALL
    SELECT payment_method_kid,payment_method_sid,payment_method_no,payment_method_name,eff_from_date,eff_to_date,is_current,run_id, run_datetime FROM no_change_payment_method
    UNION ALL
    SELECT payment_method_kid,payment_method_sid,payment_method_no,payment_method_name,eff_from_date,eff_to_date,is_current,run_id, run_datetime FROM keep_payment_method
    UNION ALL
    SELECT payment_method_kid,payment_method_sid,payment_method_no,payment_method_name,eff_from_date,eff_to_date,is_current,run_id, run_datetime FROM old_changed_payment_method

    """
    format_dict = {"max_kid" : max_kid, "PIPELINE_RUN_ID" : spark_etl.PIPELINE_RUN_ID}
    spark_etl.execute_spark_sql(sql_text, is_save_as_result = True, sql_format_dict = format_dict)

    # overwirte target
    sql_text = """INSERT OVERWRITE TABLE d_payment_method SELECT * FROM result"""
    spark_etl.execute_spark_sql(sql_text)

# launch ETL
etl_function_list = [load_d_payment_method]
etl_spark_session = SparkETL.get_spark_session("dimension_loading")
spark_etl = SparkETL("ETL demo", etl_spark_session, "CLIENT")
spark_etl.process_etl(spark_table_list, *etl_function_list)