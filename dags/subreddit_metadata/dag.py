from datetime import datetime, timedelta, date
from bs4 import BeautifulSoup
import pandas as pd
import praw
from dotenv import dotenv_values
from helpers import util
import os

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# from ratelimit import limits, RateLimitException, sleep_and_retry

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]


# @sleep_and_retry
# @limits(calls=10, period=60)
def api_hit_helper(reddit_conn, subs: str):
    sub_count = reddit_conn.subreddit(subs).subscribers
    return sub_count


def get_sub_meta():
    config = dotenv_values("./api_keys/.env")
    reddit = praw.Reddit(
        client_id=config["REDDIT_CLIENT_ID"],
        client_secret=config["REDDIT_CLIENT_SECRET"],
        user_agent=config["REDDIT_USER_AGENT"],
        ratelimit_seconds=60,
    )

    df = pd.DataFrame(columns=["subreddit", "sub_count", "date"])
    # df.columns = ["subreddit", "sub_count", "date"]

    hook = PostgresHook(postgres_conn_id="postgres_reddit")
    with open(
        f"{AIRFLOW_HOME}/dags/subreddit_metadata/sql/subreddit_names.sql", "r"
    ) as query:
        subs = hook.get_pandas_df(sql=query.read())
    for sub in subs["name"]:
        print(f"{sub}: ", end="")
        try:
            sub_count = api_hit_helper(reddit, sub)
            curr_date = date.today()
            df.loc[len(df.index)] = [sub, sub_count, curr_date]
            # print(api_hit_helper(reddit, sub))

        except Exception as err:
            if str(err) == "received 404 HTTP response":
                print(f"Subreddit {sub} is forbidden")
            elif str(err) == "received 403 HTTP response":
                print(f"Subreddit {sub} is private")
            else:
                print("Finished this run")
                print(str(err))
                break
    df.to_csv("output/sub_counts.csv", index=False)
    print(df.head())


def export_subcounts():
    config = dotenv_values("./api_keys/.env")
    hook = PostgresHook(
        postgres_conn_id="postgres_reddit",
        host="host.docker.internal",
        database="reddit",
        user=config["POSTGRES_USER"],
        password=config["POSTGRES_PASSWORD"],
        port=6543,
    )
    with hook.get_conn() as connection:
        hook.copy_expert(
            """--sql
                copy 
                  public.subscriber_count 
                from stdin
                with csv header 
                delimiter as ','
            """,
            "output/sub_counts.csv",
        )
        connection.commit()


default_args = {
    "owner": "scott",
    "retries": 1,
    "retry_delay": timedelta(seconds=2),
}

with DAG(
    dag_id="subreddit_metadata",
    default_args=default_args,
    start_date=datetime(2022, 12, 29),
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="get_sub_counts", python_callable=get_sub_meta)

    t2 = PythonOperator(task_id="export_to_postgres", python_callable=export_subcounts)

    t3 = BashOperator(
        task_id="clean_directory",
        bash_command="rm ${AIRFLOW_HOME}/output/sub_counts.csv",
    )

    t1 >> t2 >> t3
