import datetime as dt

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "levi",
    "start_date": dt.datetime(2024, 1, 2),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}

with DAG(dag_id="get_from_sql_to_elastic", schedule=None, catchup=False) as dag:

    @task
    def get_data() -> None:
        conn_string = "dbname='home' host='localhost' user='admin' password='l123'"

        conn = db.connect(conn_string)
        cur = conn.cursor()

        query = "select * from tabletop"

        df = pd.read_sql(query, conn)

        conn.close()
        cur.close()
        return df

    @task
    def insert_to_elastic(df: pd.DataFrame):
        client = Elasticsearch(
            hosts="http://localhost:9200",
            api_key="SHlpWjhKTUI5cmsxdUdrbXB3ZUE6bEp2UDJHMXpRck9RODJER3dxcDk3Zw==",
        )

        for i, r in df.iterrows():
            doc = r.to_json()
            res = client.index(index="frompostgres", body=doc)

            print(res)

    data = get_data()
    insert_to_elastic(data)
