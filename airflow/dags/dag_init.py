import datetime
import logging

from airflow import DAG
from airflow.decorators import task

default_args = {
    "owner": "levy",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    dag_id="dag_init",
    doc_md=__doc__,
    schedule=None,
    start_date=datetime.datetime(2022, 3, 4),
    catchup=False,
    default_args=default_args,
    tags=["example", "params"],
) as dag:

    @task()
    def say_hello():
        logging.info("Hello")

    say_hello()
