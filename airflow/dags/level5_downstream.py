import os
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_gcs import (
    GoogleCloudStorageToGoogleCloudStorageOperator,
)
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

args = {
    "owner": "levi",
}

# Environment Variables
gcp_project_id = "galvanic-flame-384620"
gcp_conn_id = "galvanic-flame"

# Airflow Variables
settings = Variable.get("level_3_dag_settings", deserialize_json=True)
gcs_source_data_bucket = settings["gcs_source_data_bucket"]
bq_dwh_dataset = settings["bq_dwh_dataset"]

# DAG Variables
bq_datamart_dataset = "dwh_bikesharing"
parent_dag = "level_5_dag_sensor"
bq_fact_trips_daily_table_id = (
    f"{gcp_project_id}.{bq_datamart_dataset}.facts_trips_daily_partitioned"
)

sum_total_trips_table_id = (
    f"{gcp_project_id}.{bq_datamart_dataset}.sum_total_trips_daily"
)

# Macros
execution_date = "2018-03-01"
execution_date_nodash = "20180301"


with DAG(
    dag_id="level_5_downstream_dag",
    default_args=args,
    schedule_interval="0 5 * * *",
    start_date=datetime(2018, 1, 1),
    catchup=False,
) as dag:
    # input_sensor = GoogleCloudStorageObjectSensor(
    #     task_id="sensor_task",
    #     bucket=gcs_source_data_bucket,
    #     object=f"chapter-4/data/signal/{parent_dag}/{execution_date_nodash}/_SUCCESS",
    #     mode="poke",
    #     poke_interval=60,
    #     timeout=60 * 60 * 24 * 7,
    #     gcp_conn_id=gcp_conn_id,
    # )

    input_sensor = GCSObjectExistenceSensor(
        task_id="input_sensor",
        google_cloud_conn_id=gcp_conn_id,
        bucket=gcs_source_data_bucket,
        object=f"chapter-4/data/signal/staging/{parent_dag}/{execution_date_nodash}/_SUCCESS",
    )

    data_mart_sum_total_trips = BigQueryOperator(
        task_id="data_mart_sum_total_trips",
        sql=f"""SELECT  trip_date,
						SUM(total_trips) sum_total_trips
						FROM `{bq_fact_trips_daily_table_id}`
      					WHERE trip_date = DATE('{execution_date}')
      					group by trip_date
						""",
        destination_dataset_table=sum_total_trips_table_id,
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"time_partitioning_type": "DAY", "field": "trip_date"},
        create_disposition="CREATE_IF_NEEDED",
        use_legacy_sql=False,
        priority="BATCH",
        gcp_conn_id=gcp_conn_id,
    )

    send_dag_success_signal = GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id="send_dag_success_signal",
        source_bucket=gcs_source_data_bucket,
        source_object="from-git/chapter-4/code/_SUCCESS",
        destination_bucket=gcs_source_data_bucket,
        destination_object=f"data/signal/staging/{{{{ dag.dag_id }}}}/{execution_date_nodash}/_SUCCESS",
        gcp_conn_id=gcp_conn_id,
    )

    input_sensor >> data_mart_sum_total_trips >> send_dag_success_signal

if __name__ == "__main__":
    dag.cli()
