# dags/create_mart_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Добавляем путь к папке scripts в PYTHONPATH, чтобы импорт работал
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scripts'))

from build_mart import create_mart

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'create_analytics_mart',
    default_args=default_args,
    description='Создание и обновление витрины dmr.analytics_student',
    schedule_interval='0 2 * * *',   # каждый день в 2:00
    catchup=False,
    tags=['mart', 'student_performance'],
) as dag:

    create_mart_task = PythonOperator(
        task_id='create_student_mart',
        python_callable=create_mart,
    )

    create_mart_task