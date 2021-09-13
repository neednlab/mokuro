# -*- coding: utf-8 -*-
# @Time    : 2020/12/21
# @Author  : Needn
import tempfile
from etllib.common import *
from pyspark.sql import SparkSession


class SparkETL:

    # global
    error_messages = []
    pipeline_run_id = "no pipeline run id"
    run_function_names = "All"
    begin_load_date = "0"
    table_root_path = "/mnt/datawarehouse/"
    etl_pipeline_name = "Spark ETL"
    spark_session = None
    deploy_mode = "CLIENT"
    is_databricks = False

    def __init__(self, etl_pipeline_name, spark, deploy_mode, is_databricks):
      self.etl_pipeline_name = etl_pipeline_name
      self.spark_session = spark
      self.deploy_mode = deploy_mode
      self.is_databricks = is_databricks


    # print spark sql
    def print_sql(self, sql_text):
        logging.info("Genergates SQL script:" + "\n"  + ("=" * 100) + "\n" + sql_text + "\n" + ("=" * 100))


    # load files into spark data frame
    def load_spark_table(self, db_name, table_name, prefix=""):
        
        if db_name == "stg":
            df = self.spark_session.read.csv(self.get_table_path(db_name, table_name), header=True)
        elif db_name in ["edw", "dm"]:
            df = self.spark_session.read.parquet(self.get_table_path(db_name, table_name))
        else:
            raise Exception("Database name must be stg/edw/dm")
        prefix = prefix + "_" if prefix else ""
        df.createOrReplaceTempView(prefix + table_name)

    def get_table_path(self, db_name, table_name):
        result_path = self.table_root_path + db_name + "/" + table_name + "/"
        return result_path

    
    def execute_spark_sql(self, sql_text, view_name = "", is_cached = False, is_return_dataset = False, is_save_as_result = False, is_show=False,sql_format_dict = {}):
        """
        SQL patterns
        1. no return value
        2. create a temp view (if [view_name] is given)
        3. return a dataframe (if [is_return_dataset] is set to True)
        4. save as file and create a "result" view (if [is_save_as_result] is set to True)
        """
        # generate dynamic SQL
        if sql_format_dict:
            sql_text = sql_text.format(**sql_format_dict)
        self.print_sql(sql_text)

        df = self.spark_session.sql(sql_text)

        if is_save_as_result:
            if self.is_databricks:
                temp_dir = '/tmp/' + str(uuid.uuid4())
                self.get_dbutils().fs.mkdirs(temp_dir)
            else:
                temp_dir = os.path.join(tempfile.mkdtemp(), "result")
            df.write.parquet(path=temp_dir, mode="overwrite")
            self.spark_session.read.parquet(temp_dir).createOrReplaceTempView("result")
        elif view_name:
            df.createOrReplaceTempView(view_name)
            if is_cached:
                self.spark_session.catalog.cacheTable(view_name)
        elif is_return_dataset:
            return df.collect()

        if is_show:
            df.show(50)
        

    # call etl function
    def call_etl_function(self, function_name, error_counter):
        try:
            logging.info("Execute function [" + function_name.__name__ + "]")
            function_name(self)
            
        except Exception as ex:
            message = "error in [{fn_name}]: {ex}"
            message = message.format(fn_name = function_name.__name__, ex = ex)
            #global error_messages
            self.error_messages.append(message)
            return error_counter + 1
        else:       
            logging.info(function_name.__name__  + " completed")        
            return error_counter

    # Check ETL function
    # Raise all exceptions finally
    def check_etl_function(self, error_counter):
        if error_counter > 0:
            message = "ETL failed module numbers: " + str(error_counter)
            for err in self.error_messages:
                logging.error(err)
            raise Exception(message)
        else:
            logging.info("All etl modules completed")

    # get spark session
    @classmethod
    def get_spark_session(cls, etl_moduel_name, existing_spak_session=None, timezone="Asia/Shanghai", spark_config=None):
        
        # start spark sesssion if running with CLIENT mode
        # .config("spark.sql.autoBroadcastJoinThreshold", -1) 
        if not existing_spak_session:
            spark = SparkSession \
                .builder \
                .appName(etl_moduel_name) \
                .config("spark.sql.parquet.writeLegacyFormat", True) \
                .config("spark.sql.session.timeZone", timezone) \
                .getOrCreate()

            if spark_config:
                for config_key, config_value in spark_config.items():
                    spark.conf.set(config_key, config_value)

        return (spark if not existing_spak_session else existing_spak_session)
        

    # process ETL
    def process_etl(self, spark_table_list, *etl_functions):

        if self.deploy_mode == "CLUSTER":
            self.pipeline_run_id = self.get_notebook_parameter_value("pipeline_run_id") or "no pipeline run id"
            self.run_function_names = self.get_notebook_parameter_value("run_function_names") or "All"
            self.begin_load_date = self.get_notebook_parameter_value("begin_load_date") or "0"

        # number of exceptions
        error_counter = 0

        # spark_table_list
        # Ex:
        # [
        #    ["edw", "d_calendar"]
        # ]
        for table in spark_table_list:
            self.load_spark_table(table[0], table[1])
            logging.info("load table [" + table[0] + "." + table[1] + "]")

        # call etl funtions
        # use LIST to control execution sequence
        # create a dictionary
        etl_function_dict = {}
        for fun in etl_functions:
            etl_function_dict[fun.__name__] = fun

        # run function manually if p2 != All
        if self.run_function_names != "All":
            etl_functions = []
            for function_name in self.run_function_names.split(","):
                etl_functions.append(etl_function_dict.get(function_name))

        for fun in etl_functions:
            error_counter = self.call_etl_function(fun, error_counter)

        # check all errors
        self.check_etl_function(error_counter)
        # spark.stop()

    # Databricks area
    # get databricks dbutils
    def get_dbutils(self):
        from pyspark.dbutils import DBUtils
        return DBUtils(self.spark_session)

    # get base_parameters of notebook
    def get_notebook_parameter_value(self, parameter_name):

        try:
            dbutils = self.get_dbutils()
            return dbutils.widgets.get(parameter_name)   
        except:
            logging.error("Cannot not find parameter [" + parameter_name + "]")

if __name__ == "__main__":
    print(sys.path)