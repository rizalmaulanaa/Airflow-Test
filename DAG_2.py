from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from task2.flights import get_data_daily, get_data_monthly, get_data_annual

path = "/Users/rizalmaulana/Documents/airflow_workspace/airflow/dags/task2/"

default_args = {
    'owner': 'rizal',
    'start_date': datetime(2018, 12, 16),
    'end_date': datetime(2020, 4, 2),
}

dag = DAG(
    dag_id='DAG-2',
    default_args=default_args,
    schedule_interval='@daily',
    start_date= datetime(2018, 12, 16),
    end_date= datetime(2020, 4, 2),
)

t1 = PythonOperator(
    task_id='get_data_daily', 
    python_callable=get_data_daily,
    op_kwargs={'path': path, },
    dag=dag)

t2 = PythonOperator(
    task_id='get_data_monthly', 
    python_callable=get_data_monthly, 
    op_kwargs={'path': path, },
    dag=dag)

t3 = PythonOperator(
    task_id='get_data_annual', 
    python_callable=get_data_annual,
    op_kwargs={'path': path, },
    dag=dag)

t1 >> t2 >> t3