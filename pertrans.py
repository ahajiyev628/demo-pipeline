import os
import json
import sys
from datetime import datetime, date, timedelta

from pyspark.sql import SparkSession, functions as F
from pyspark import SparkConf
from pyspark.sql.types import *


os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-11.0.19.0.7-4.el8.x86_64'

sparkConf = SparkConf()
sparkConf.setAppName("dataloader_oic_atransfer")
# sparkConf.set("spark.driver.maxResultSize", "2g")


spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()
sc = spark.sparkContext

dwh_config = sys.argv[1]
dwh_properties = json.loads(dwh_config)

jdbc_properties_log = json.loads(os.getenv("DWH_AIRFLOW_CONFIG"))

xcom_data_str = sys.argv[2]
print('INFO XCOM DATA STR:', xcom_data_str)

airflow_run_date = sys.argv[3]
print('INFO AIRFLOW RUN DATE: ', airflow_run_date)

dag_id = 'DAG_AZC_FİLES_LOAD_TO_ORACLE'
task_id = 'pertrans'


def log_insert(file_name, parse_status, df_count, load_count, start_time, end_time, process_name, file_date, error_desc=None):
    print("LOG INSERT ORACLE TABLE", datetime.now())
    insert_date = datetime.now() + timedelta(hours=4)
    summary_df = spark.createDataFrame(
        [(file_name, parse_status, df_count, load_count, error_desc, insert_date, insert_date.year, start_time, end_time, process_name, file_date)],
        StructType([
            StructField("FILE_NAME", StringType(), True),
            StructField("PARSE_STATUS", StringType(), True),
            StructField("FILE_ROW_CNT", IntegerType(), True),
            StructField("PARSE_ROW_CNT", IntegerType(), True),
            StructField("ERROR_DESC", StringType(), True),
            StructField("INSERT_DATE", TimestampType(), True),
            StructField("INSERT_YEAR", IntegerType(), True),
            StructField("START_DT", TimestampType(), True),
            StructField("END_DT", TimestampType(), True),
            StructField("JOB_NAME", StringType(), True),
            StructField("FILE_DATE", DateType(), True)
        ])
    )

    db_log_table = "dwh_intf.ibar_dwh_parsing_log"
    summary_df.write.jdbc(url=dwh_properties["url"], table=db_log_table, mode="append", properties=dwh_properties)
    print(f'Successfully inserted log summary for {file_name}')


def log_insert_delta(file_name, del_table, start_date, end_date, df_count, dag_id, task_id, task_state, task_error_message=None):
    log_df = spark.createDataFrame(
        [(file_name, del_table, start_date, end_date, df_count, dag_id, task_id, task_state, task_error_message)],
        StructType([
            StructField("source_table_name", StringType(), True),
            StructField("hms_table_name", StringType(), True),
            StructField("start_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
            StructField("hms_count", IntegerType(), True),
            StructField("dag_id", StringType(), True),
            StructField("task_id", StringType(), True),
            StructField("task_state", StringType(), True),
            StructField("task_error_message", StringType(), True)
        ])
    )
    log_df = log_df.withColumn("report_date", F.current_date())

    log_df.select(F.col("report_date"),
                  F.col("source_table_name"),
                  F.col("hms_table_name"),
                  F.col("start_time"),
                  F.col("end_time"),
                  F.col("hms_count"),
                  F.col("dag_id"),
                  F.col("task_id"),
                  F.col("task_state"),
                  F.col("task_error_message"))

    log_df.write.jdbc(url=dwh_properties["url"], table='dwh.oracle_to_delta_daily_log', mode='append', properties=dwh_properties)
    print(f'Successfully inserted delta_log summary for {file_name}')


def parse_db(file_name):
    start_date = datetime.now() + timedelta(hours=4)
    file_date = datetime.strptime(file_name[8:18], '%Y-%m-%d').date()
    date_filter=file_date
    db_table = "dwh_intf.ibar_dwh_bkm_per_trn"
    del_table = "sd_acrd.ibar_bkm_per_trn"
    print(file_name, datetime.now())
    try:
        file_rdd = sc.textFile(f's3a://bigdatastore/Azericard_files/{file_name}')
        final_line = file_rdd.filter(lambda line: "</OPERATIONS>" in line).take(1)[0]
        print(final_line)
        if final_line.__contains__('</OPERATIONS>'):
            print('File is good', file_name)

            df = spark.read \
                .format("xml") \
                .option('inferSchema', False) \
                .option('rootTag', 'OPERATIONS') \
                .option("rowTag", "TRN") \
                .load(f's3a://bigdatastore/Azericard_files/{file_name}')

            ins_dttm = date.today() + timedelta(hours=4)
            df_selected = df.select(
                F.to_date(F.col('FLD_001'), "yyyyMMdd").alias('STTL_DATE'),
                F.to_date(F.col('FLD_002'), "yyyyMMdd").alias('TRN_DATE'),
                F.to_timestamp(F.concat(F.col('FLD_002'), F.col('FLD_003')), 'yyyyMMddHHmmss').alias('TRN_TIME'),
                F.col('FLD_004').alias('APPROVAL_CODE'),
                F.col('FLD_005').alias('SERVICE_BRAND'),
                F.col('FLD_006').alias('ISSUER_PRODUCT'),
                F.col('FLD_007').alias('SERVICE_REFERENCE'),
                F.col('FLD_008').alias('CARD_NUMBER'),
                F.col('FLD_009').alias('ACQ_REF_NO'),
                F.col('FLD_010').alias('RTR_REF_NO'),
                F.col('FLD_011').alias('AMOUNT'),
                F.col('FLD_012').alias('CCY'),
                F.col('FLD_013').alias('ACQ_FEE'),
                F.col('FLD_014').alias('ACQ_FEE_CCY'),
                F.col('FLD_015').alias('ACQ_FEE_SIGN'),
                F.col('FLD_016').alias('ISS_FEE'),
                F.col('FLD_017').alias('ISS_FEE_SIGN'),
                F.col('FLD_018').alias('ISS_FEE_CCY'),
                F.col('FLD_019').alias('MMC'),
                F.col('FLD_020').alias('MERCHANT_NAME'),
                F.col('FLD_021').alias('MERCHANT_ID'),
                F.col('FLD_022').alias('TERMINAL_ID'),
                F.col('FLD_023').alias('TRN_TYPE'),
                F.col('FLD_024').alias('CARD_INPUT'),
                F.col('FLD_025').alias('CARD_PRE_IND'),
                F.col('FLD_026').alias('OPER_ENV'),
                F.col('FLD_027').alias('TRN_LIFECYCLE'),
                F.col('FLD_028').alias('ACQ_ID_ACQ'),
                F.col('FLD_029').alias('ACQ_ID_ISS'),
                F.lit(file_name).alias('FILE_NAME'),
                F.lit(ins_dttm).alias('INSERT_DATE'),
                F.lit(file_date).alias('FILE_DATE'))

            # print(f'test: {df_selected.rdd.getNumPartitions()}')
            df_selected.cache()
            df_selected_cnt = df_selected.count()
            source_count=df_selected.count()


            # WRITE ORACLE


            df_selected.write.option("truncate", "false").jdbc(url=dwh_properties["url"], table=db_table, mode="append", properties=dwh_properties)
            end_date = datetime.now() + timedelta(hours=4)
            print('Succesfully inserted Info: ', end_date)
            print('LOG INSERT VALUES: ', file_name,'Success', df_selected_cnt, df_selected_cnt, start_date.year, start_date, end_date, 'BKM', file_date)
            log_insert(file_name, 'Success', df_selected_cnt, df_selected_cnt,  start_date, end_date, 'BKM', file_date)

            count_query_oracle_count = f"select count(1) AS oracle_count from {db_table} where  insert_date= to_date('{date_filter}', 'YYYY-MM-DD')"
            df_cnt_oracle_count = spark.read \
            .format("jdbc") \
            .option("url", jdbc_properties_log["url"]) \
            .option("driver", jdbc_properties_log["driver"]) \
            .option("dbtable", f"({count_query_oracle_count})") \
            .option("user", jdbc_properties_log["user"]) \
            .option("password", jdbc_properties_log["password"]) \
            .load()
            df_cnt_oracle_count.show()
            target_count = df_cnt_oracle_count.collect()[0][0] 

            count_state = "success" if source_count == target_count else "fail"

            plsql_block_log = f""" insert into dwh.airflow_daily_log (report_date, source_name,target_name,start_dt,end_dt,source_count,target_count,count_state)  values (sysdate, '{task_id}'||'/'||'{file_name}','{db_table}','{date_filter}','{date_filter}',{source_count},{target_count},'{count_state}') """
            count_query_log = """(select count(*) from dual) t1"""
            df_cnt = spark.read \
            .format("jdbc") \
            .option("url", jdbc_properties_log["url"]) \
            .option("driver", jdbc_properties_log["driver"]) \
            .option("sessionInitStatement", plsql_block_log) \
            .option("dbtable", count_query_log) \
            .option("user", jdbc_properties_log["user"]) \
            .option("password", jdbc_properties_log["password"]) \
            .load()
            df_cnt.show(5)

            # WRITE DELTA


            df_selected = df_selected.withColumn('AMOUNT', F.col('AMOUNT').cast(DecimalType(38, 10))) \
                .withColumn('ACQ_FEE', F.col('ACQ_FEE').cast(DecimalType(38, 10))) \
                .withColumn('ISS_FEE', F.col('ISS_FEE').cast(DecimalType(38, 10)))

            df_selected.write.format("delta") \
                .partitionBy("TRN_DATE") \
                .mode("append") \
                .saveAsTable(del_table)

            end_date = datetime.now() + timedelta(hours=4)
            print('Data successfully inserted to Delta', datetime.now())
            print('LOG INSERT VALUES: ', file_name, del_table, start_date, end_date, df_selected_cnt, dag_id, task_id, 'Success')
            log_insert_delta(file_name, del_table, start_date, end_date, df_selected_cnt, dag_id, task_id, 'Success')


            delta_df = spark.sql(f"""select count(1) as c from   {del_table} where  insert_date= date'{date_filter}' """)
            target_delta_count = delta_df.collect()[0]["c"]


            count_state = "success" if source_count == target_delta_count else "fail"

            plsql_block_log = f""" insert into dwh.airflow_daily_log (report_date, source_name,target_name,start_dt,end_dt,source_count,target_count,count_state)  values (sysdate, '{task_id}'||'/'||'{file_name}','{del_table}','{date_filter}','{date_filter}',{source_count},{target_delta_count},'{count_state}') """
            count_query_log = """(select count(*) from dual) t1"""
            df_cnt = spark.read \
            .format("jdbc") \
            .option("url", jdbc_properties_log["url"]) \
            .option("driver", jdbc_properties_log["driver"]) \
            .option("sessionInitStatement", plsql_block_log) \
            .option("dbtable", count_query_log) \
            .option("user", jdbc_properties_log["user"]) \
            .option("password", jdbc_properties_log["password"]) \
            .load()
            df_cnt.show(5)




        else:
            print('File is Broken Info:', file_name)
            raise Exception(f'File is Broken Info: {file_name}')

    except Exception as e:
        print('Exception Info: ', e)
        end_date = datetime.now() + timedelta(hours=4)
        log_insert(file_name, 'Fail', 0, 0, start_date, end_date, 'BKM', file_date, str(e)[:2000] + str(e)[-2000:])

        plsql_block_log = f""" insert into dwh.airflow_daily_log  (report_date, source_name,target_name,start_dt,end_dt)  values ( sysdate, '{task_id}'||'/'||'{file_name}','{db_table}','{date_filter}','{date_filter}') """
        count_query_log = """(select count(*) from dual) t1"""
        df_cnt = spark.read \
        .format("jdbc") \
        .option("url", jdbc_properties_log["url"]) \
        .option("driver", jdbc_properties_log["driver"]) \
        .option("sessionInitStatement", plsql_block_log) \
        .option("dbtable", count_query_log) \
        .option("user", jdbc_properties_log["user"]) \
        .option("password", jdbc_properties_log["password"]) \
        .load()
        df_cnt.show(5) 
        raise 

for file_name in xcom_data_str.split(','):
    if file_name.upper().startswith('PERTRANS') and file_name.endswith('.xml'):
        print('parsing begin:', file_name)
        parse_db(file_name)
