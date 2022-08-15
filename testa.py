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


def get_secret(secret_name: str, region_name: str, aws_access_key_id: str, aws_secret_access_key: str) -> dict:
    """
    This function creates boto3 session and gets secret variables json from AWS secrets manager. Returns dictionary.
    """

    # Create a Secrets Manager client
    session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key)
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            return json.loads(get_secret_value_response['SecretString'])
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])


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


class SparkSessionClass():

    def create_spark_session(self, app_name: str, spark_jars: str, master: str = None) -> None:
        """
        Gets of creates a spark session for ths specific app_name

        :param app_name: The name of the application for spark
        :param master: The name of the master (optional)
        :param spark_jars: Any additional jar files to be loaded by spark (separated by ",")
                            For example: spark_jars = r"C:\mysql-connector-java-8.0.30.jar, C:\postgresql-42.4.0.jar"

        """
        print('Creating PySpark session...')
        builder = SparkSession.builder.appName(app_name)

        if master is not None:
            builder = builder.master(master)

        if spark_jars is not None:
            builder = builder.config('spark.jars', spark_jars)
        self.spark_session = builder.getOrCreate()
        return self.spark_session


def create_pyspark_df_from_excel(file_path: str, columns_names_list: list):
    """
    Creates PySpark dataframe from excel file.

    :param: file_path: path to excel file (example 'C:\MyProjects\excel_file.xlsx)
    :param: columns_names_list: names for table columns (example ['first_level', 'second_level', 'third_level',...])
    """
    # Dataframe creation
    pandas_df = pd.read_excel(file_path, header=2)[:-1]

    if columns_names_list is not None:
        # Rename columns
        pandas_df.columns = columns_names_list

    # Transformation to str for avoiding type errors
    headers_list = list(pandas_df)
    pandas_df[headers_list] = pandas_df[headers_list].astype(str)
    print('Pandas DF OK.')

    # Convert Pandas dataframe to PySpark dataframe
    df = spark.createDataFrame(pandas_df)
    df = df.replace({'nan': None})
    df = df.filter(df.object_name.isNotNull())
    return df


def write_to_db_table(df, url: str, dbtable: str, user: str, password: str, driver: str, mode: str) -> None:
    """
    Writes data from PySpark dataframe to database table.

    :param: df: Pyspark Dataframe created previously
    :param: url: 'jdbc:'{postgresql/mysql}'://{host}/{database_name}'
    :param: user: db username'
    :param: password: db password'
    :param: driver: example 'org.postgresql.Driver'
    :param: mode: example 'append' / 'overwrite'...
    """
    if mode == 'overwrite':
        df.write.format("jdbc") \
            .mode("overwrite") \
            .option("url", url) \
            .option("dbtable", dbtable) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", driver) \
            .option("truncate", "true") \
            .save()
        print('DATA HAS BEEN ADDED TO DATABASE with overwrite mode!')

    elif mode == 'append':
        df.write.format("jdbc") \
            .mode("append") \
            .option("url", url) \
            .option("dbtable", dbtable) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", driver) \
            .save()
        print('DATA HAS BEEN ADDED TO DATABASE with append mode!')


def read_df_from_table(url: str, dbtable: str, user: str, password: str, driver: str):
    """
    Reads data from database table (returns PySpark dataframe).

    :param: df: Pyspark Dataframe created previously
    :param: url: 'jdbc:'{postgresql/mysql}'://{host}/{database_name}'
    :param: user: db username'
    :param: password: db password'
    :param: driver: example 'org.postgresql.Driver'
    """

    jdbcDF = spark.read.format("jdbc"). \
        options(
        url=url,
        dbtable=dbtable,
        user=user,
        password=password,
        driver=driver).load()
    return jdbcDF


### DON'T FORGET TO CHANGE PATHS!!!

# Spark Session Creation

cwd = os.getcwd()

spark_jars = f"{cwd}/jars/mysql-connector-java-8.0.30.jar, " \
             f"{cwd}/jars/postgresql-42.4.0.jar"

spark = SparkSessionClass().create_spark_session(app_name='PysparkApp', spark_jars=spark_jars)
print('Session Created!')
logger.info('PySpark Session Created!')
# Dataframe (df) creation

file_path = f'{cwd}/storagefiles/kodyfikator-2.xlsx'

columns_names_list = ['first_level', 'second_level', 'third_level', 'fourth_level', 'extra_level', 'category',
                      'object_name']

# df = create_pyspark_df_from_excel(file_path=file_path, columns_names_list=columns_names_list)
# print('PySpark DF OK.')

## TO POSTGRESQL

user_postgres = postgres_secrets['username']  # credentials
password_postgres = postgres_secrets['password']
jdbc_url_postgres = f"jdbc:postgresql://{postgres_secrets['host']}/{postgres_secrets['dbname']}"
driver_postgres = "org.postgresql.Driver"

## WRITE DATA TO POSTGRESQL DB TABLE

# write_to_db_table(df=df, url=jdbc_url_postgres, dbtable='codes_pg', user=user_postgres, password=password_postgres,
#                   driver='org.postgresql.Driver', mode='append')

# logger.info('Data has been written to Postgresql DB table!')

## READ data from postgres db

# rdf_pg = read_df_from_table(url=jdbc_url_postgres, dbtable='codes_one', user=user_postgres, password=password_postgres,
#                             driver=driver_postgres)
# rdf_pg.show()

### TO MYSQL

user_mysql = mysql_secrets['username']  # credentials
password_mysql = mysql_secrets['password']
jdbc_url_mysql = f"jdbc:mysql://{mysql_secrets['host']}:3306/{mysql_secrets['dbname']}"
driver_mysql = 'com.mysql.cj.jdbc.Driver'


## WRITE DATA TO MYSQL DB TABLE

# write_to_db_table(df=df, url=jdbc_url_mysql, dbtable='codes_two', user=user_mysql, password=password_mysql,
#                   driver='com.mysql.cj.jdbc.Driver', mode='append')
# logger.info('Data has been written to MySQL DB table!')


### READ data from mysql table

# rdf_mysql = read_df_from_table(url=jdbc_url_mysql, dbtable='codes_ms', user=user_mysql, password=password_mysql,
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
    rdf_mysql = read_df_from_table(url=jdbc_url_mysql, dbtable=mysql_table, user=user_mysql, password=password_mysql,
                                   driver=driver_mysql)

    # postgres df
    rdf_pg = read_df_from_table(url=jdbc_url_postgres, dbtable=pg_table, user=user_postgres,
                                password=password_postgres,
                                driver=driver_postgres)

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

# check_sync(mysql_table=mysql_table1, pg_table=pg_table1)

## DATA FROM POSTGRESQL TO REDSHIFT DB

import redshift_connector

## UNCOMMENT TO read data from postgres and create pandas dataframe

# rdf_pg = read_df_from_table(url=jdbc_url_postgres, dbtable='codes_one', user=user_postgres, password=password_postgres,
#                             driver=driver_postgres)
# pd_rdf = rdf_pg.toPandas()

## REDSHIFT SECRETS MANAGEMENT

secret_name_redshift = os.getenv('SECRET_NAME_REDSHIFT')

redshift_secrets = get_secret(secret_name=secret_name_redshift, region_name=region_name,
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)
## Creating boto3 session
session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key,
                                region_name=os.getenv('REGION_NAME'))
s3_bucket1 = os.getenv('S3_BUCKET1')

## Converting pandas dataframe with data from postgres to parquet file to s3

# wr.s3.to_parquet(
#     df=pd_rdf,
#     path=f's3://{s3_bucket1}/pgpandas.parquet',
# )

s3_parquet_path = f"s3://{s3_bucket1}/pgpandas.parquet"

df_parq = wr.s3.read_parquet(path=s3_parquet_path)

## CHOOSE ONE TYPE OF CONNECTION

print('connection creation....')

## First way with credential type connection

# conn = redshift_connector.connect(
#     host=redshift_secrets['host'],
#     database='read-dead',
#     user=redshift_secrets['username'],
#     password=redshift_secrets['password'])

## Second way with Glue Connection
conn = wr.redshift.connect(connection=os.getenv('GLUE_CONNECTION'), boto3_session=session, )

print('connected.')

## Using .copy method from aws wrangler to insert data from s3 to redshift db table (change table name if you wish)

wr.redshift.copy(df=df_parq, path=f's3://{s3_bucket1}/nova1.parquet', con=conn, table='pusher3', schema="public",
                 primary_keys=['id'], mode='overwrite')
print('done')
conn.close()
