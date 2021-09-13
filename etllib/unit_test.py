# -*- coding: utf-8 -*-
# @Time    : 2020/12/21
# @Author  : Needn
# @Software: VS Code
from etllib.spark import *

# load tables
spark_table_list = []

def test_load(spark_etl):

    sql_text = "CREATE TABLE IF NOT EXISTS n1 (ID INT) USING PARQUET LOCATION 'n1'"
    spark_etl.execute_spark_sql(sql_text)

    sql_text = "INSERT OVERWRITE TABLE n1 VALUES(1),(2)"
    spark_etl.execute_spark_sql(sql_text)

    sql_text = "SELECT n1.ID AS ID1, n2.ID AS ID2 FROM n1 AS n1 CROSS JOIN n1 AS n2"
    spark_etl.execute_spark_sql(sql_text, view_name="n12")
    
    sql_format_dict = {"derived_col": "Hello"}
    sql_text = "SELECT *, '{derived_col}' AS VALUE  FROM n12"
    spark_etl.execute_spark_sql(sql_text, view_name="n12", is_show=True, sql_format_dict=sql_format_dict)
    


# launch ETL
if __name__ == "__main__":
    etl_function_list = [test_load]
    etl_spark_session = SparkETL.get_spark_session("test_load")
    spark_etl = SparkETL(etl_pipeline_name="ETL demo", spark=etl_spark_session, deploy_mode="CLIENT", is_databricks=False)
    spark_etl.process_etl(spark_table_list, *etl_function_list)