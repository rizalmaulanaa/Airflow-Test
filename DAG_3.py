from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from task3.change_file import change_file

path = "/Users/rizalmaulana/Documents/airflow_workspace/airflow/dags/task3/great_expectations/checkpoints/"

default_args = {
    'owner' : 'rizal',
    'start_date' : datetime(2021, 10, 11),
    'end_date' : datetime(2021, 10, 12),
}

dag = DAG(
    dag_id='DAG-3',
    default_args=default_args,
    schedule_interval='@daily',
    start_date = datetime(2021, 10, 11),
    end_date = datetime(2021, 10, 12),
)

t1 = PythonOperator(
    task_id='change_file', 
    python_callable=change_file,
    op_kwargs={'path': path, },
    dag=dag)

t2 = BashOperator(
    task_id='val_great_expectations', 
    bash_command='task3/run_expectations.sh',
    dag=dag)

t1 >> t2