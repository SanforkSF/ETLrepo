import sys

import findspark
import json

findspark.find()
findspark.init()
import os
cwd = os.getcwd()
import pyspark
from delta import *
from pyspark.sql.functions import *
from utils import read_df_from_table, write_to_db_table, SparkSessionClass, get_secret

from dotenv import load_dotenv
load_dotenv()
from delta.tables import *
# ## PART 1
#
# spark_jars = f"{cwd}/jars/mysql-connector-java-8.0.30.jar, " \
#              f"{cwd}/jars/spark-mssql-connector_2.12-1.2.0.jar, " \
#              f"{cwd}/jars/mssql-jdbc-9.4.0.jre8.jar, " \
#              f"{cwd}/jars/postgresql-42.4.0.jar"
#
# spark = SparkSessionClass().create_spark_session(app_name='PysparkApp', spark_jars=spark_jars)
# print('Session Created!')
#
# ### MSSQL CREDENTIALS
#
# jdbc_ms_url = os.getenv('MSSQL_JDBC_URL')
# mssql_driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
# user_mss = os.getenv('MSSQL_USER')
# password_mss = os.getenv('MSSQL_PASSWORD')
#
# ### MSSQL DF
# df_mss = read_df_from_table(spark=spark, url=jdbc_ms_url, dbtable='dbo.ms_codes',
#                                  user=user_mss, password=password_mss, driver=mssql_driver)
#
# df_mss.write.format("parquet").mode("overwrite").save(cwd + "/tmp/df_main.parquet")
#
# data1 = spark.read.format("parquet").load(cwd + "/tmp/df_main.parquet")
# data1.show()
#
# ### POSTGRES CREDENTIALS
#
# aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
# aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
# secret_name_postgres = os.getenv('SECRET_NAME_POSTGRES')
# secret_name_mysql = os.getenv('SECRET_NAME_MYSQL')
# region_name = os.getenv('REGION_NAME')
#
# postgres_secrets = get_secret(secret_name=secret_name_postgres, region_name=region_name,
#                               aws_access_key_id=aws_access_key_id,
#                               aws_secret_access_key=aws_secret_access_key)
#
# user_postgres = postgres_secrets['username']  # credentials
# password_postgres = postgres_secrets['password']
# jdbc_url_postgres = f"jdbc:postgresql://{postgres_secrets['host']}/{postgres_secrets['dbname']}"
# driver_postgres = "org.postgresql.Driver"
#
# ### POSTGRES DF
# df_pg = read_df_from_table(spark=spark, url=jdbc_url_postgres, dbtable='codes_pg',
#                            user=user_postgres, password=password_postgres,
#                            driver=driver_postgres)
# # df_pg.show()
#
# df_pg.write.format("parquet").mode("overwrite").save(cwd + "/tmp/df_target.parquet")
#
# data2 = spark.read.format("parquet").load(cwd + "/tmp/df_target.parquet")
# data2.show()

# ## PART 2
#
### CREATING SPARK SESSION FOR DELTA
builder2 = pyspark.sql.SparkSession.builder.appName("MyApp2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

print('tryna create')
spark_delta = configure_spark_with_delta_pip(builder2).getOrCreate()
print('spark created?')

# # READING PARQUETS > CREATING DELTA TABLES
#
# df_main_parquet = spark_delta.read.format("parquet").load(cwd + "/tmp/df_main.parquet")
# df_main_parquet.write.format("delta").mode("overwrite").save(cwd + "/tmp/df_main_table")
#
# df_target_parquet = spark_delta.read.format("parquet").load(cwd + "/tmp/df_target.parquet")
# df_target_parquet.write.format("delta").mode("overwrite").save(cwd + "/tmp/df_target_table")

##  PART 3

# df_main = spark_delta.read.format("delta").load(cwd + "/tmp/df_main_table")
# # df_main.show()
# df_target = spark_delta.read.format("delta").load(cwd + "/tmp/df_target_table")
# # df_target.show()


# deltaTableMain = DeltaTable.forPath(sparkSession=spark_delta, path=cwd + "/tmp/df_main_table")
# df_main = deltaTableMain.toDF()
# df_main.show()
#
# deltaTableTarget = DeltaTable.forPath(sparkSession=spark_delta, path=cwd + "/tmp/df_target_table")
# dbt1 = deltaTableTarget.toDF()
# dbt1.show()
#
# deltaTableTarget.alias('target') \
#   .merge(
#     df_main.alias('main'),
#     'target.id = main.id'
#   ) \
#   .whenMatchedUpdate(set =
#     {
#       "first_level": "main.first_level",
#       "second_level": "main.second_level",
#       "third_level": "main.third_level",
#       "fourth_level": "main.fourth_level",
#       "extra_level": "main.extra_level",
#       "category": "main.category",
#       "object_name": "main.object_name"
#     }
#   ) \
#   .whenNotMatchedInsert(values =
#     {
#       "first_level": "main.first_level",
#       "second_level": "main.second_level",
#       "third_level": "main.third_level",
#       "fourth_level": "main.fourth_level",
#       "extra_level": "main.extra_level",
#       "category": "main.category",
#       "object_name": "main.object_name"
#     }
#   ) \
#   .execute()
# print('executed!')

### OLD NEW DFS

# old_df = spark_delta.read.format("delta").option("versionAsOf", 3).load(cwd + "/tmp/df_target_table").orderBy(col("id").desc())


# old_df = spark_delta.read.format("delta").load(cwd + "/tmp/df_main_table").orderBy(col("id").desc())
# print(old_df.count())
# old_df.filter(old_df.id < 23000).show()
#
#
# new_df = spark_delta.read.format("delta").option("versionAsOf", 3).load(cwd + "/tmp/df_target_table").orderBy(col("id").desc())
#
# print(new_df.count())
# new_df.filter(new_df.id < 23000).show()
# zxc_df = new_df.subtract(old_df).orderBy(col("id").desc())
# print(zxc_df.count())
# zxc_df.show()

# deltaTableTarget = DeltaTable.forPath(sparkSession=spark_delta, path=cwd + "/tmp/df_target_table")
# history_df = deltaTableTarget.toDF()
# history_df.show()