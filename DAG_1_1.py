from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from task1.pipeline import set_data_sales

default_args = {
    'owner': 'rizal',
    'start_date': datetime(2009, 1, 1),
    'end_date': datetime(2009, 1, 11),
}

dag = DAG(
    dag_id='DAG-1-1',
    default_args=default_args,
    schedule_interval='@once',
    start_date=datetime(2009, 1, 1),
    end_date=datetime(2009, 1, 11),
)

t1 = PythonOperator(
    task_id='set_data_sales', 
    python_callable=set_data_sales,
    op_kwargs={'database': 'workflow0', },
    dag=dag)

t1