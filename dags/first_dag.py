from datetime import datetime, timedelta
import requests
import pandas as pd
import sys
import sqlalchemy
from dotenv import dotenv_values

from airflow import DAG
from airflow.operators.python import PythonOperator


def sys_print():
    config = dotenv_values("api_keys/.env")
    engine = sqlalchemy.create_engine(
        "postgresql://{}:{}@host.docker.internal:6543/test".format(
            config["POSTGRES_USER"], config["POSTGRES_PASSWORD"]
        )
    )
    engine.connect()


default_args = {"owner": "scott", "retries": 1, "retry_delay": timedelta(seconds=1)}

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
