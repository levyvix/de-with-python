import datetime
import logging

from elasticsearch import Elasticsearch

from airflow import DAG
from airflow.decorators import task

default_args = {
    "owner": "levy",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1),
}

with DAG(
    dag_id="test_elastic",
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

    @task
    def elastic_count() -> None:
        try:
            client = Elasticsearch(
                hosts="http://localhost:9200",
                basic_auth=("elastic", "B9hwRQC2"),
                # api_key="RS00SjhaTUItek9remNLbmlvVXE6d3FsRkRPWi1UWmV2WGpjTFRuSnlIQQ==",
            )
            print(client.info())
        except ConnectionError as e:
            logging.error(f"Error connecting to Elasticsearch: {e}")

    say_hello() >> elastic_count()
