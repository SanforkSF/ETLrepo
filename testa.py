import findspark

findspark.find()
findspark.init()
import os
from pyspark.sql import SparkSession, Row
import pandas as pd
from dotenv import load_dotenv

# Take variables from .env file
load_dotenv()


class SparkSessionClass():

    def create_spark_session(self, app_name, spark_jars, master=None) -> None:
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


def create_pyspark_df_from_excel(file_path, columns_names_list=None):
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


def write_to_db_table(df, url, dbtable, user, password, driver):
    """
    Writes data from PySpark dataframe to database table.

    :param: df: Pyspark Dataframe created previously
    :param: url: 'jdbc:'{postgresql/mysql}'://{host}/{database_name}'
    :param: user: db username'
    :param: password: db password'
    :param: driver: example 'org.postgresql.Driver'
    """
    df.write.format("jdbc") \
        .mode("append") \
        .option("url", url) \
        .option("dbtable", dbtable) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .save()
    print('DATA HAS BEEN ADDED TO DATABASE!')


def read_df_from_table(url, dbtable, user, password, driver):
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
             r"C:\minidisc\MyProjects\practice1\jars\postgresql-42.4.0.jar "

spark = SparkSessionClass().create_spark_session(app_name='PysparkApp', spark_jars=spark_jars)
print('Session Created!')

# Dataframe (df) creation

file_path = 'C:\minidisc\MyProjects\practice1\storagefiles\kodyfikator-2.xlsx'

columns_names_list = ['first_level', 'second_level', 'third_level', 'fourth_level', 'extra_level', 'category',
                      'object_name']

df = create_pyspark_df_from_excel(file_path=file_path, columns_names_list=columns_names_list)
print('PySpark DF OK.')

## TO POSTGRESQL

user_postgres = os.getenv('DB_USER_POSTGRES')  # credentials
password_postgres = os.getenv('DB_PWD_POSTGRES')
jdbc_url_postgres = os.getenv("JDBC_URL_POSTGRES")
driver_postgres = 'org.postgresql.Driver'

## WRITE DATA TO POSTGRESQL DB TABLE

# write_to_db_table(df=df, url=jdbc_url_postgres, dbtable='codes_one', user=user_postgres, password=password_postgres,
#                   driver='org.postgresql.Driver')


## READ data from postgres db

rdf_pg = read_df_from_table(url=jdbc_url_postgres, dbtable='codes_one', user=user_postgres, password=password_postgres,
                            driver=driver_postgres)
rdf_pg.show()


### TO MYSQL

user_mysql = os.getenv('DB_USER_MYSQL')
password_mysql = os.getenv('DB_PWD_MYSQL')
jdbc_url_mysql = os.getenv('JDBC_URL_MYSQL')
driver_mysql = 'com.mysql.cj.jdbc.Driver'

## WRITE DATA TO MYSQL DB TABLE

# write_to_db_table(df=df, url=jdbc_url_mysql, dbtable='codes_two', user=user_mysql, password=password_mysql,
#                   driver='com.mysql.cj.jdbc.Driver')

### READ data from mysql table

rdf_mysql = read_df_from_table(url=jdbc_url_mysql, dbtable='codes_two', user=user_mysql, password=password_mysql,
                               driver=driver_mysql)
rdf_mysql.show()
