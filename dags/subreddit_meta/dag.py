from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import pandas as pd
import praw
from dotenv import dotenv_values
from helpers import util

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from ratelimit import limits, RateLimitException, sleep_and_retry

@sleep_and_retry
@limits(calls=100, period=120)
def api_hit_helper(reddit_conn, sub: str):
    sub_count = reddit_conn.subreddit(sub).subscribers
    print(f"{sub} subscriber count: {sub_count}")


def get_sub_meta():
    config = dotenv_values("./api_keys/.env")
    reddit = praw.Reddit(
        client_id=config["REDDIT_CLIENT_ID"],
        client_secret=config["REDDIT_CLIENT_SECRET"],
        user_agent=config["REDDIT_USER_AGENT"],
    )

    hook = PostgresHook(postgres_conn_id="postgres_reddit")
    subs = hook.get_pandas_df(sql="""--sql
        select name from public.subreddits;
    """)
    print(subs.dtypes)
    print(subs.head())
    subs["sub_count"] = subs.head().apply(lambda x: api_hit_helper(reddit, x["name"]))

default_args = {"owner": "scott", "retries": 1, "retry_delay": timedelta(seconds=2)}

with DAG(
    dag_id="subreddit_metadata",
    default_args=default_args,
    start_date=datetime(2022, 12, 29),
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="call_api_per_sub",
        python_callable=get_sub_meta
    )
 
    t1