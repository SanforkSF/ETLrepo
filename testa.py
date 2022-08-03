import sys

import findspark

findspark.find()
findspark.init()
import os
from pyspark.sql import SparkSession, Row
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

spark_jars = r"C:\minidisc\MyProjects\practice1\jars\mysql-connector-java-8.0.30.jar, " \
             r"C:\minidisc\MyProjects\practice1\jars\postgresql-42.4.0.jar, " \
             r"C:\minidisc\MyProjects\practice1\jars\spark-excel_2.12-3.2.1_0.17.1.jar"

spark = SparkSessionClass().create_spark_session(app_name='PysparkApp', spark_jars=spark_jars)
print('Session Created!')
logger.info('PySpark Session Created!')
# Dataframe (df) creation

file_path = 'C:\minidisc\MyProjects\practice1\storagefiles\kodyfikator-2.xlsx'

columns_names_list = ['first_level', 'second_level', 'third_level', 'fourth_level', 'extra_level', 'category',
                      'object_name']

# df = create_pyspark_df_from_excel(file_path=file_path, columns_names_list=columns_names_list)
# print('PySpark DF OK.')

## TO POSTGRESQL

user_postgres = os.getenv('DB_USER_POSTGRES')  # credentials
password_postgres = os.getenv('DB_PWD_POSTGRES')
jdbc_url_postgres = os.getenv("JDBC_URL_POSTGRES")
driver_postgres = 'org.postgresql.Driver'

## WRITE DATA TO POSTGRESQL DB TABLE

# write_to_db_table(df=df, url=jdbc_url_postgres, dbtable='codes_pg', user=user_postgres, password=password_postgres,
#                   driver='org.postgresql.Driver', mode='append')

# logger.info('Data has been written to Postgresql DB table!')

## READ data from postgres db

# rdf_pg = read_df_from_table(url=jdbc_url_postgres, dbtable='codes_one', user=user_postgres, password=password_postgres,
#                             driver=driver_postgres)
# rdf_pg.show()


### TO MYSQL

user_mysql = os.getenv('DB_USER_MYSQL')
password_mysql = os.getenv('DB_PWD_MYSQL')
jdbc_url_mysql = os.getenv('JDBC_URL_MYSQL')
driver_mysql = 'com.mysql.cj.jdbc.Driver'

## WRITE DATA TO MYSQL DB TABLE

# write_to_db_table(df=df, url=jdbc_url_mysql, dbtable='codes_two', user=user_mysql, password=password_mysql,
#                   driver='com.mysql.cj.jdbc.Driver', mode='append')
# logger.info('Data has been written to MySQL DB table!')


### READ data from mysql table

# rdf_mysql = read_df_from_table(url=jdbc_url_mysql, dbtable='codes_two', user=user_mysql, password=password_mysql,
#                                driver=driver_mysql)
#

# rdf_mysql.show()

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


    else:   # if postgresql table has missed rows in mysql table
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