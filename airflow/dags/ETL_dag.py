from datetime import datetime,date
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
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
    dag_id='ETL_dag', 
    default_args=default_args,
    description='Extracts csv file from web, transforma and add a date column to csv file, loads the csv data to a locally created database tabele',
    schedule_interval=None,
)

def transform_data():
    #Read in the file, and write a transformed file out
    today=date.today()
    df=pd.read_csv('/workspaces/Basic-ETL-aiflow/airflow/lab/extract-data.csv')
    generic_type_df=df[df['Type']=='generic']
    generic_type_df['Date']=today.strftime('%Y-%m-%d')
    generic_type_df.to_csv('/workspaces/Basic-ETL-aiflow/airflow/lab/extract-data.csv', index=False)

task1 = BashOperator(
        task_id='Extract_task', 
        bash_command='wget -c https://raw.githubusercontent.com/LinkedInLearning/hands-on-introduction-data-engineering-4395021/main/data/top-level-domain-names.csv -O /workspaces/Basic-ETL-aiflow/airflow/lab/extract-data.csv',
        dag=dag
    )

task2 = PythonOperator(
        task_id='Transform_task', 
        python_callable=transform_data,
        dag=dag
    )
task3 = BashOperator(
        task_id='Load_task', 
        bash_command='echo -e ".separator ","\n.import --skip 1 /workspaces/Basic-ETL-aiflow/airflow/lab/extract-data.csv domains" | sqlite3 /workspaces/Basic-ETL-aiflow/airflow/lab/load.db',
        dag=dag
    )

task1 >> task2 >> task3