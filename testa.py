import sys

import findspark
import json

findspark.find()
findspark.init()
import os
from pyspark.sql import SparkSession, Row
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql.functions import col
import logging
import awswrangler as wr

# Take variables from .env file
load_dotenv()

# Config logger
logging.basicConfig(filename='logs.txt',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger('etl_logger')

import boto3
import base64
from botocore.exceptions import ClientError

from utils import get_secret, write_to_db_table, read_df_from_table, create_pyspark_df_from_excel, SparkSessionClass


aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
secret_name_postgres = os.getenv('SECRET_NAME_POSTGRES')
secret_name_mysql = os.getenv('SECRET_NAME_MYSQL')
region_name = os.getenv('REGION_NAME')

postgres_secrets = get_secret(secret_name=secret_name_postgres, region_name=region_name,
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)

mysql_secrets = get_secret(secret_name=secret_name_mysql, region_name=region_name, aws_access_key_id=aws_access_key_id,
                           aws_secret_access_key=aws_secret_access_key)


### DON'T FORGET TO CHANGE PATHS!!!

# Spark Session Creation

cwd = os.getcwd()

spark_jars = f"{cwd}/jars/mysql-connector-java-8.0.30.jar, " \
             f"{cwd}/jars/spark-mssql-connector_2.12-1.2.0.jar, " \
             f"{cwd}/jars/mssql-jdbc-9.4.0.jre8.jar, " \
             f"{cwd}/jars/postgresql-42.4.0.jar"

spark = SparkSessionClass().create_spark_session(app_name='PysparkApp', spark_jars=spark_jars)
print('Session Created!')
logger.info('PySpark Session Created!')

# Dataframe (df) creation

file_path = f'{cwd}/storagefiles/kodyfikator-2.xlsx'

columns_names_list = ['first_level', 'second_level', 'third_level', 'fourth_level', 'extra_level', 'category',
                      'object_name']

df = create_pyspark_df_from_excel(spark=spark, file_path=file_path, columns_names_list=columns_names_list)
print('PySpark DF OK.')

## TO POSTGRESQL

user_postgres = postgres_secrets['username']  # credentials
password_postgres = postgres_secrets['password']
jdbc_url_postgres = f"jdbc:postgresql://{postgres_secrets['host']}/{postgres_secrets['dbname']}"
driver_postgres = "org.postgresql.Driver"

## WRITE DATA TO POSTGRESQL DB TABLE

# write_to_db_table(df=df, url=jdbc_url_postgres, dbtable='codes_pg', user=user_postgres, password=password_postgres,
#                   driver='org.postgresql.Driver', mode='overwrite')
#
# logger.info('Data has been written to Postgresql DB table!')

## READ data from postgres db

# rdf_pg = read_df_from_table(spark=spark, url=jdbc_url_postgres, dbtable='codes_pg', user=user_postgres, password=password_postgres,
#                             driver=driver_postgres)
# rdf_pg.show()

### TO MYSQL

user_mysql = mysql_secrets['username']  # credentials
password_mysql = mysql_secrets['password']
jdbc_url_mysql = f"jdbc:mysql://{mysql_secrets['host']}:3306/{mysql_secrets['dbname']}"
driver_mysql = 'com.mysql.cj.jdbc.Driver'


## WRITE DATA TO MYSQL DB TABLE

# write_to_db_table(df=df, url=jdbc_url_mysql, dbtable='codes_ms', user=user_mysql, password=password_mysql,
#                   driver='com.mysql.cj.jdbc.Driver', mode='overwrite')
# logger.info('Data has been written to MySQL DB table!')


### READ data from mysql table

# rdf_mysql = read_df_from_table(spark=spark, url=jdbc_url_mysql, dbtable='codes_ms', user=user_mysql, password=password_mysql,
#                                driver=driver_mysql)
#
#
# rdf_mysql.show()
#
# df.show()


### SYNC DATA


def check_sync(mysql_table: str, pg_table: str):
    """
    This function compare tables from two databases and in case of definition of some rows it
    inserts missing rows to another database. In case of wrong scheme the script just stops and says to check scheme.
    Fields like user, passwords and drivers should be filled above for correct work!!!
    """

    # mysql df
    rdf_mysql = read_df_from_table(spark=spark, url=jdbc_url_mysql, dbtable=mysql_table, user=user_mysql, password=password_mysql,
                                   driver=driver_mysql)

    rdf_mysql = rdf_mysql.drop('id')

    # postgres df
    rdf_pg = read_df_from_table(spark=spark, url=jdbc_url_postgres, dbtable=pg_table, user=user_postgres,
                                password=password_postgres,
                                driver=driver_postgres)
    rdf_pg = rdf_pg.drop('id')

    if rdf_mysql.schema == rdf_pg.schema:
        print('Schema check OK.')
        logger.info('Schema check OK.')

    else:
        print('Schemas are not the same! Make identical schemas before checking data!')
        logger.warning('Schemas are not the same! Make identical schemas before checking data!')

        sys.exit()

    ## Checking different rows
    df_mysql_subtract_pg = rdf_mysql.subtract(rdf_pg)  # dataframe rows are in mysql but not in posgres;
    df_pg_subtract_mysql = rdf_pg.subtract(rdf_mysql)  # dataframe rows are in postgres but not in mysql;

    row_different_in_mysql_count = df_mysql_subtract_pg.count()
    row_different_in_posgres_count = df_pg_subtract_mysql.count()

    if row_different_in_mysql_count == 0 and row_different_in_posgres_count == 0:
        print("Data rows are identical")
        logger.info('Data rows are identical!')

    elif row_different_in_mysql_count > 0 and row_different_in_posgres_count == 0:
        # if mysql table has missed rows in postgresql table
        print('Preparing to insert missed data into Postgres table...')
        logger.info('Preparing to insert missed data into Postgres table...')

        # create sorted by id dataframe of missing rows
        df_rows_not_in_postgres_from_mysql = df_mysql_subtract_pg.sort(col('id').asc())
        write_to_db_table(df=df_rows_not_in_postgres_from_mysql,
                          url=jdbc_url_postgres,
                          dbtable=pg_table,
                          user=user_postgres,
                          password=password_postgres,
                          driver=driver_postgres,
                          mode='append')
        print('END adding data to POSTGRES table')
        logger.info('END (adding data to POSTGRES table)')

    elif row_different_in_posgres_count > 0 and row_different_in_mysql_count == 0:
        # if postgresql table has missed rows in mysql table
        print('Preparing to insert missed data into MySQL table...')
        logger.info('Preparing to insert missed data into MySQL table...')

        # create sorted by id dataframe of missing rows
        df_rows_not_in_mysql_from_postgres = df_pg_subtract_mysql.sort(col('id').asc())
        write_to_db_table(df=df_rows_not_in_mysql_from_postgres,
                          url=jdbc_url_mysql,
                          dbtable=mysql_table,
                          user=user_mysql,
                          password=password_mysql,
                          driver=driver_mysql,
                          mode='append')
        print('END (adding data to MYSQL table)')
        logger.info('END (adding data to MYSQL table)')

    else:  # if postgresql table has missed rows in mysql table
        print('Different rows in both tables... Preparing data...')
        logger.info('Different rows in both tables... Preparing data...')

        # create sorted by id dataframe of missing rows in postgres
        df_rows_not_in_postgres_from_mysql = df_mysql_subtract_pg.sort(col('id').asc())
        write_to_db_table(df=df_rows_not_in_postgres_from_mysql,
                          url=jdbc_url_postgres,
                          dbtable=pg_table,
                          user=user_postgres,
                          password=password_postgres,
                          driver=driver_postgres,
                          mode='append')
        print('END adding data to POSTGRES table')
        logger.info('END (adding data to POSTGRES table)')

        # create sorted by id dataframe of missing rows in mysql
        df_rows_not_in_mysql_from_postgres = df_pg_subtract_mysql.sort(col('id').asc())
        write_to_db_table(df=df_rows_not_in_mysql_from_postgres,
                          url=jdbc_url_mysql,
                          dbtable=mysql_table,
                          user=user_mysql,
                          password=password_mysql,
                          driver=driver_mysql,
                          mode='append')
        print('END (adding data to MYSQL table)')
        logger.info('END (adding data to MYSQL table)')

        print('SUCCESS!!!')


mysql_table1 = 'codes_ms'
pg_table1 = 'codes_pg'

check_sync(mysql_table=mysql_table1, pg_table=pg_table1)


# ## DATA FROM POSTGRESQL TO REDSHIFT DB
#
# import redshift_connector
#
# ## UNCOMMENT TO read data from postgres and create pandas dataframe
#
# # rdf_pg = read_df_from_table(spark=spark, url=jdbc_url_postgres, dbtable='codes_one', user=user_postgres, password=password_postgres,
# #                             driver=driver_postgres)
# # pd_rdf = rdf_pg.toPandas()
#
# ## REDSHIFT SECRETS MANAGEMENT
#
# secret_name_redshift = os.getenv('SECRET_NAME_REDSHIFT')
#
# redshift_secrets = get_secret(secret_name=secret_name_redshift, region_name=region_name,
#                               aws_access_key_id=aws_access_key_id,
#                               aws_secret_access_key=aws_secret_access_key)
# ## Creating boto3 session
# session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
#                                 aws_secret_access_key=aws_secret_access_key,
#                                 region_name=os.getenv('REGION_NAME'))
# s3_bucket1 = os.getenv('S3_BUCKET1')
#
# ## Converting pandas dataframe with data from postgres to parquet file to s3
#
# # wr.s3.to_parquet(
# #     df=pd_rdf,
# #     path=f's3://{s3_bucket1}/pgpandas.parquet',
# # )
#
# s3_parquet_path = f"s3://{s3_bucket1}/pgpandas.parquet"
#
# df_parq = wr.s3.read_parquet(path=s3_parquet_path)
#
# ## CHOOSE ONE TYPE OF CONNECTION
#
# print('connection creation....')
#
# ## First way with credential type connection
#
# # conn = redshift_connector.connect(
# #     host=redshift_secrets['host'],
# #     database='read-dead',
# #     user=redshift_secrets['username'],
# #     password=redshift_secrets['password'])
#
# ## Second way with Glue Connection
# conn = wr.redshift.connect(connection=os.getenv('GLUE_CONNECTION'), boto3_session=session, )
#
# print('connected.')
#
# ## Using .copy method from aws wrangler to insert data from s3 to redshift db table (change table name if you wish)
#
# # wr.redshift.copy(df=df_parq, path=f's3://{s3_bucket1}/nova1.parquet', con=conn, table='pusher3', schema="public",
# #                  primary_keys=['id'], mode='overwrite')
# # print('done')
# # conn.close()
