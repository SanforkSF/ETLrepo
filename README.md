# ETLrepo

Repository for reading and writing data from excel files to AWS RDS databases with PySpark.
(for Ubuntu using AWS RDS Databases, AWS Secrets Manager, Docker)

Installation:

1. You will need to create or take existing credentials for your databases from AWS Secrets Manager;

You will need with parameters like these in future. You can find example in 'dotenvexample' file;
```
AWS_ACCESS_KEY_ID = 'AWS_ACCESS_KEY_ID'

AWS_SECRET_ACCESS_KEY = 'AWS_SECRET_ACCESS_KEY'

SECRET_NAME_POSTGRES = 'SECRET_NAME_POSTGRES'

SECRET_NAME_MYSQL = 'SECRET_NAME_MYSQL'

REGION_NAME = 'REGION_NAME'
```
2. Open 'testa.py' file and edit variables.

3. Insert your spark jars, excel file path, columns names list for creation SparkSession and pyspark dataframe.

example:

```
file_path = 'C:\minidisc\MyProjects\practice1\storagefiles\kodyfikator-2.xlsx'

columns_names_list = ['first_level', 'second_level', 'third_level', 'fourth_level', 'extra_level', 'category',
                      'object_name']

df = create_pyspark_df_from_excel(file_path=file_path, columns_names_list=columns_names_list)
```

4. Choose what you need to do. For example if you have to write dataframe to postgresql database table:

```   
user_postgres = os.getenv('DB_USER_POSTGRES')  # credentials
password_postgres = os.getenv('DB_PWD_POSTGRES')
jdbc_url_postgres = os.getenv("JDBC_URL_POSTGRES")
driver_postgres = 'org.postgresql.Driver'

## WRITE DATA TO POSTGRESQL DB TABLE

write_to_db_table(df=df, url=jdbc_url_postgres, dbtable='YOUR_TABLE_NAME', user=user_postgres, password=password_postgres,
                  driver='org.postgresql.Driver', mode="append")
```

Or if you want to read (create pyspark dataframe from db table and show it):
```
## READ data from postgres db

rdf_pg = read_df_from_table(url=jdbc_url_postgres, dbtable='YOUR_TABLE_NAME', user=user_postgres, password=password_postgres,
                            driver=driver_postgres, mode="append")
rdf_pg.show()
```

5. Sync tables of your postgresql and mysql databases. This function compares two tables and adds missing rows of one into another
or says that they are identical.

```
mysql_table1 = 'YOUR_MYSQL_TABLE_NAME'
pg_table1 = 'YOUR_POSTGRES_TABLE_NAME'

check_sync(mysql_table=mysql_table1, pg_table=pg_table1)

```

6. Build your Docker image.

Move to your ETLrepo folder:

```
cd ETLrepo
```
Then start image build (you can choose any image name for example 'etl-test')

```
docker build -t {YOUR_IMAGE_NAME} .
```
7. Run your Docker container
```
docker run -e AWS_ACCESS_KEY_ID='your_aws_access_key_id' \
-e AWS_SECRET_ACCESS_KEY='your_aws_secret_access_key' \
-e SECRET_NAME_POSTGRES='your_secret_name_postgres' \
-e SECRET_NAME_MYSQL='your_secret_name_mysql' \
-e REGION_NAME='aws_region_name' {YOUR_IMAGE_NAME}
```

