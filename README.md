# ETLrepo

Repository for reading and writing data from excel files to databases with PySpark.

Installation:

1. Set and activate virtual enviroment;

2. Install requirements:
```pip install -r requirements.txt```

3. Create file .env with parameters like in 'dotenvexample' file;
```
DB_USER_POSTGRES = 'username'
DB_PWD_POSTGRES = 'password'
JDBC_URL_POSTGRES = 'jdbc:{mysql/postgresql}://{HOST}/{DATABASE}'

DB_USER_MYSQL = 'username'
DB_PWD_MYSQL = 'password'
JDBC_URL_MYSQL = 'jdbc:{mysql/postgresql}://{HOST}/{DATABASE}'
```
4. Insert your spark jars, excel file path, columns names list for creation SparkSession and pyspark dataframe.

example:

```
file_path = 'C:\minidisc\MyProjects\practice1\storagefiles\kodyfikator-2.xlsx'

columns_names_list = ['first_level', 'second_level', 'third_level', 'fourth_level', 'extra_level', 'category',
                      'object_name']

df = create_pyspark_df_from_excel(file_path=file_path, columns_names_list=columns_names_list)
```

5. Choose what you need to do. For example if you have to write dataframe to postgresql database table:

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

6. Sync tables of your postgresql and mysql databases. This function compares two tables and adds missing rows of one into another
or says that they are identical.

```
mysql_table1 = 'YOUR_MYSQL_TABLE_NAME'
pg_table1 = 'YOUR_POSTGRES_TABLE_NAME'

check_sync(mysql_table=mysql_table1, pg_table=pg_table1)

```

