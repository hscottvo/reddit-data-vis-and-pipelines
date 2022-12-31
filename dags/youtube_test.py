from datetime import datetime, timedelta
import requests
import pandas as pd
import os
from dotenv import dotenv_values
from apiclient.discovery import build
import json

from airflow import DAG
from airflow.operators.python import PythonOperator

def data():
    config = dotenv_values("api_keys/.env") 
    print(config)
    youtube_key = config["YOUTUBE_API_V3"]
    youtube = build("youtube", "v3", developerKey=youtube_key)

    request = youtube.channels().list(
        part="statistics,contentDetails",
        forUsername="ScottTheWoz",
        maxResults=10
    )
    
    response = request.execute()
    
    # print("response type:", type(response))
    # print("response: ")
    # print(response)
    return response

    # df = pd.DataFrame(i["statistics"] for i in response["items"])
    # df.to_csv("data/test_docker.csv")

    # with open("data/test.json", "w") as outfile:
    #     json.dump(response, outfile, indent=2)

default_args = {"owner": "scott", "retries": 5, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="read_top_views",
    default_args=default_args,
    description="Reads and saves top view from youtube (testing)",
    start_date=datetime(2022, 12, 15),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task1 = PythonOperator(task_id="read_top_views", python_callable=data)