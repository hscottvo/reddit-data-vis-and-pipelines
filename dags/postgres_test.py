from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {"owner": "scott", "retries": 1, "retry_delay": timedelta(seconds=2)}


with DAG(
    dag_id="postgres_hello_world",
    default_args=default_args,
    start_date=datetime(2022, 12, 29),
    catchup=True,
) as dag:
    task1 = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="postgres_localhost",
        sql="""--sql
            create table if not exists dag_runs (
              dt date, 
              dag_id character varying,
              primary key (dt, dag_id)
            )
        """,
    )

    task2 = PostgresOperator(
        task_id="delete_data_from_table",
        postgres_conn_id="postgres_localhost",
        sql="""--sql
            delete from dag_runs where dt = '{{ ds }}' and dag_id = '{{dag.dag_id}}';
        """,
    )

    task3 = PostgresOperator(
        task_id="insert_into_table",
        postgres_conn_id="postgres_localhost",
        sql="""--sql
            insert into dag_runs (dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id }}')
        """,
    )

    task1 >> task2 >> task3