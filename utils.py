import boto3
import base64
from botocore.exceptions import ClientError
import pandas as pd
import awswrangler as wr
from pyspark.sql import SparkSession, Row
import os
import json

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

def create_pyspark_df_from_excel(spark, file_path: str, columns_names_list: list):
    """
    Creates PySpark dataframe from excel file.
    :param: spark: PySpark Session
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


def read_df_from_table(spark, url: str, dbtable: str, user: str, password: str, driver: str):
    """
    Reads data from database table (returns PySpark dataframe).

    :param: spark: Pyspark Session
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