import sys
import findspark

findspark.find()
findspark.init()
import json

import os
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql.functions import col
import logging

# Take variables from .env file
load_dotenv()

# Config logger
logging.basicConfig(filename='logs.txt',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger('etl_logger')

from utils import create_pyspark_df_from_excel, write_to_db_table, read_df_from_table, SparkSessionClass


cwd = os.getcwd()

spark_jars = f"{cwd}/jars/spark-mssql-connector_2.12-1.2.0.jar, " \
             f"{cwd}/jars/mssql-jdbc-9.4.0.jre8.jar"

### CREATE SPARK SESSION

spark = SparkSessionClass().create_spark_session(app_name='PysparkApp', spark_jars=spark_jars)
print('Session Created!')

logger.info('PySpark Session Created!')

### CREATE DF FROM EXCEL
file_path = f'{cwd}/storagefiles/kodyfikator-2.xlsx'

columns_names_list = ['first_level', 'second_level', 'third_level', 'fourth_level', 'extra_level', 'category',
                      'object_name']

excel_df = create_pyspark_df_from_excel(spark=spark, file_path=file_path, columns_names_list=columns_names_list)
print('PySpark DF OK.')
# excel_df.show()

### WRITE TO MS SQL SERVER TABLE


jdbc_ms_url = os.getenv('MSSQL_JDBC_URL')
mssql_driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
user_mss = os.getenv('MSSQL_USER')
password_mss = os.getenv('MSSQL_PASSWORD')

# excel_df.show()
#
# write_to_db_table(df=excel_df,
#                   url=jdbc_ms_url,
#                   dbtable='dbo.ms_codes',
#                   user=user_mss,
#                   password=password_mss,
#                   driver=mssql_driver,
#                   mode='overwrite')
# print('End.')

### READ MS SQL SERVER TABLE

df_from_mss = read_df_from_table(spark=spark, url=jdbc_ms_url, dbtable='dbo.ms_codes',user=user_mss, password=password_mss, driver=mssql_driver)
df_from_mss.show()