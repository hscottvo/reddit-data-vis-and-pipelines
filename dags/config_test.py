from datetime import datetime, timedelta
import requests
import pandas as pd
import os
from dotenv import dotenv_values
from apiclient.discovery import build
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

def print_config():
    config = dotenv_values("api_keys/.env") 
    print("Config: ", end="")
    print(config.keys())

default_args = {"owner": "scott", "retries": 0, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="read_config",
    default_args=default_args,
    description="Reads config file and prints it",
    start_date=datetime(2022, 12, 15),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task1 = PythonOperator(task_id="read_config_task", python_callable=print_config)