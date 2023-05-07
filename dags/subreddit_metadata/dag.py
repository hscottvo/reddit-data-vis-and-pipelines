from datetime import datetime, timedelta, date
from bs4 import BeautifulSoup
import pandas as pd
import praw
from dotenv import dotenv_values
from helpers import util
import os

from sqlalchemy import create_engine

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

    hook = PostgresHook(postgres_conn_id="postgres_cockroach")
    with open(
        f"{AIRFLOW_HOME}/dags/subreddit_metadata/sql/subreddit_names.sql", "r"
    ) as query:
        subs = hook.get_pandas_df(sql=query.read())
    for sub in subs["name"]:
        # print(f"{sub}: ", end="")
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
            elif str(err) == "Redirect to /subreddits/search":
                print(f"Subreddit {sub} does not exist")
            else:
                print("Finished this run")
                print(str(err))
                break
    util.create_dir("output")
    df.to_csv("output/sub_counts.csv", index=False)
    print(df.head())


def export_subcounts():
    config = dotenv_values("./api_keys/.env")

    df = pd.read_csv("output/sub_counts.csv")

    engine = create_engine(config["COCKROACH_ALCHEMY"])

    df.to_sql(
        "subscriber_count",
        engine,
        if_exists="replace",
        index=False,
    )


default_args = {
    "owner": "scott",
    "retries": 0,
    "retry_delay": timedelta(seconds=2),
    "schedule_interval": "@daily",
}

with DAG(
    dag_id="subreddit_metadata",
    default_args=default_args,
    start_date=datetime(2022, 12, 29),
    schedule_interval="30 2 * * *",
    catchup=False,
) as dag:
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_cockroach",
        sql="sql/create_table.sql",
    )
    hit_api = PythonOperator(task_id="get_sub_counts", python_callable=get_sub_meta)

    populate_table = PythonOperator(
        task_id="export_to_postgres", python_callable=export_subcounts
    )

    delete_csv = BashOperator(
        task_id="clean_directory",
        bash_command="rm ${AIRFLOW_HOME}/output/sub_counts.csv",
    )

    create_table >> hit_api >> populate_table >> delete_csv
