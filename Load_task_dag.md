install sqllite extension,sqlite viewer

In ariflow/lab create sql file that will create a table in sql data base that we will initialize

-- Create a new table named 'domains' with specified columns
CREATE TABLE IF NOT EXISTS domains (
    Domain TEXT,
    Type TEXT,
    Sponsoring_Organisation TEXT,
    Date TEXT
);

sqlite3 /workspaces/Basic-ETL-aiflow/airflow/lab/load.db < /workspaces/Basic-ETL-aiflow/airflow/lab/create_tbl.sql 
( creates a load.db database and a tbl is created when the create_tbl.sql is run.)
We will create the database in the same loation of csv file


_____To manuallly load csv file data into tbl:

change directory to the location where both database and csv are present

1. connect to database
sqlite3 load.db
2.command to set the output mode to CSV (Comma-Separated Values). This is particularly useful when you want to export query results in a CSV format.
.mode csv

3. import data from a CSV file into a specified table

 .import --skip 1 extract-data.csv domains

 .import is the command to import data.
--skip 1 tells SQLite to skip the first line of the CSV file.
extract-data.csv is the name of the CSV file you are importing from.
domains is the name of the table into which the data is being imported.

____To load thhrough airflow task 

In the dag we could have use the sql codes but it can be done with bash operator with less hassle.

bash_command="echo -e ".separator ","\n.import --skip 1 /workspaces/Basic-ETL-aiflow/airflow/lab/extract-data.csv domains" | sqlite3 /workspaces/Basic-ETL-aiflow/airflow/lab/load.db",

This command sets the field separator to a comma and imports data from extract-data.csv into the domains table in the load.db SQLite database, skipping the first row of the CSV file.

The given bash_command aims to import data from a CSV file into an SQLite database using sqlite3. Here's a breakdown of the command:

echo -e: This command will enable the interpretation of backslash escapes.
".separator ","\n.import --skip 1 /workspaces/Basic-ETL-aiflow/airflow/lab/extract-data.csv domains": This is the text to be echoed, consisting of:
.separator ",": This sets the field separator to a comma.
.import --skip 1 /workspaces/Basic-ETL-aiflow/airflow/lab/extract-data.csv domains: This imports the CSV file, skipping the first row (usually headers), into the domains table of the SQLite database.
| sqlite3 /workspaces/Basic-ETL-aiflow/airflow/lab/load.db: The output of the echo command is piped into the sqlite3 command, which executes it on the specified database.



python -W ignore /workspaces/Basic-ETL-aiflow/airflow/dags/Load_dag.py







from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow import DAG 


# Define default arguments for the DAG
default_args = {
    'owner': 'Sabuja',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'catchup': False,
    'start_date': datetime(2023, 1, 1)
}
# Define the DAG object
dag = DAG(
    dag_id='Load_task_dag', 
    default_args=default_args,
    description='hello',
    schedule_interval=None,
)
# bash operatoe (used to run the commands to createthe file)

task1 = BashOperator(
        task_id='Load_task', 
        bash_command='echo -e ".separator ","\n.import --skip 1 /workspaces/Basic-ETL-aiflow/airflow/lab/extract-data.csv domains" | sqlite3 /workspaces/Basic-ETL-aiflow/airflow/lab/load.db',
        dag=dag
    )



 