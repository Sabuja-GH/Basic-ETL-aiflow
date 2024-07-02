''' Transform DAG '''
from datetime import datetime, date
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow import DAG

with DAG(
    dag_id='transform_dag',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    def transform_data():
        """Read in the file, and write a transformed file out"""
        today = date.today() 
        df = pd.read_csv('/workspaces/hands-on-introduction-data-engineering-4395021/lab/orchestrated/airflow-extract-data.csv')
        generic_type_df = df[df['Type'] == 'generic'] (selects only the rows that have 'generic' as value in 'Type' column)
        generic_type_df['Date'] = today.strftime('%Y-%m-%d') (adds new column 'Date'  Date to the dataframe)
        generic_type_df.to_csv('/workspaces/hands-on-introduction-data-engineering-4395021/lab/orchestrated/airflow-transform-data.csv', index=False) (the df is transformed again to csv and saved at the location provided)

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data, ( Specifies the Python function transform_data to be called when this task is executed.)
        dag=dag (Associates this task with the DAG object dag) 
    ) 

python -W ignore /workspaces/Basic-ETL-aiflow/airflow/dags/Transform_dag.py
  











from datetime import datetime,date
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
    dag_id='tranform_task_dag', 
    default_args=default_args,
    description='transforms the data',
    schedule_interval=None,
)
# bash operatoe (used to run the commands to createthe file)

def transform_data():
    #Read in the file, and write a transformed file out
    today=date.today()
    df=pd.read_csv('/workspaces/Basic-ETL-aiflow/airflow/lab/extract-data.csv')
    generic_type_df=df[df['Type']=='generic']
    generic_type_df['Date']=today.strftime('%Y-%m-%d')
    generic_type_df.to_csv('/workspaces/Basic-ETL-aiflow/airflow/lab/extract-data.csv', index=False)

task1 = PythonOperator(
        task_id='transform_task', 
        python_callable=transform_data,
        dag=dag
    )