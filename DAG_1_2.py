from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from task1.pipeline import set_data_employees

default_args = {
    'owner': 'rizal',
    'start_date': datetime(1987, 6, 17),
    'end_date': datetime(2000, 4, 21),
}

dag = DAG(
    dag_id='DAG-1-2',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(1987, 6, 17),
    end_date=datetime(2000, 4, 21),
)

t1 = PythonOperator(
    task_id='set_data_employees', 
    python_callable=set_data_employees,
    op_kwargs={'database': 'workflow1', },
    dag=dag)

t1