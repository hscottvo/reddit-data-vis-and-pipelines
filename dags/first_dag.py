from datetime import datetime, timedelta
import requests
import pandas as pd
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator


def sys_print():
    print(sys.path)


default_args = {"owner": "scott", "retries": 5, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="Sys-Test",
    default_args=default_args,
    description="Prints home directory",
    start_date=datetime(2022, 12, 29),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task1 = PythonOperator(task_id="sys-test", python_callable=sys_print)
    task1
