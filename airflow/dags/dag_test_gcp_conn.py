import datetime as dt

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.gcs import GCSHook


@dag(dag_id="test_gcp_conn", start_date=dt.datetime(2024, 12, 10), catchup=False)
def dag_test_gcp_conn():
    @task()
    def list_buckets() -> None:
        hook = GCSHook(gcp_conn_id="galvanic-flame")
        conn = hook.get_conn()

        buckets = conn.list_buckets()
        for b in buckets:
            print(b)

    list_buckets()


dag_test_gcp_conn()
