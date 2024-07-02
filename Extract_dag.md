Extracts raw data from web and loads as a csv file

bash_command='wget -c https://raw.githubusercontent.com/LinkedInLearning/hands-on-introduction-data-engineering-4395021/main/data/top-level-domain-names.csv -O /workspaces/Basic-ETL-aiflow/airflow/lab/extract-data.csv',

wget is a command-line utility for downloading files from the web. It supports downloading via HTTP, HTTPS, and FTP protocols.

https://lnkd.in/gfENQi7K- shortended url

https://raw.githubusercontent.com/LinkedInLearning/hands-on-introduction-data-engineering-4395021/main/data/top-level-domain-names.csv - link of the csv file to be downloaded

Before that create a folder as lab

$ wget -c https://raw.githubusercontent.com/LinkedInLearning/hands-on-introduction-data-engineering-4395021/main/data/top-level-domain-names.csv -O /workspaces/AIRFLOW-HANDSON/airflow/lab/extract-data.csv

wget: The command to download files from the web.
-c: This option allows you to continue getting a partially-downloaded file.
https://raw.githubusercontent.com/LinkedInLearning/hands-on-introduction-data-engineering-4395021/main/data/top-level-domain-names.csv: The URL of the file to be downloaded.
-O /workspaces/hands-on-introduction-data-engineering-4395021/lab/manual/manual-extract-data.csv: This option specifies the output file path and name where the downloaded file will be saved.

python -W ignore /workspaces/Basic-ETL-aiflow/airflow/dags/Extract_dag.py


create a folder lab in airlfow











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
    dag_id='Extract_task_dag', 
    default_args=default_args,
    description='Extracts a csv file from web using wget bash operator and then saves the csv file in the location specified',
    schedule_interval=None,
)
# bash operatoe (used to run the commands to createthe file)

task1 = BashOperator(
        task_id='Extract_task', 
        bash_command='wget -c https://raw.githubusercontent.com/LinkedInLearning/hands-on-introduction-data-engineering-4395021/main/data/top-level-domain-names.csv -O /workspaces/Basic-ETL-aiflow/airflow/lab/extract-data.csv',
        dag=dag
    )