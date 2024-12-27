import datetime as dt

from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLExportInstanceOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

cloud_sql_instance = "mysql-instance-source"
project_id = "galvanic-flame-384620"
gcp_conn_id = "galvanic-flame"
bucket = f"{project_id}-data-bucket"
SQL_QUERY = "SELECT * FROM apps_db.stations"
EXPORT_URI = f"gs://{bucket}/mysql_export/from_airflow/stations/stations.csv"
export_body = {
    "exportContext": {
        "fileType": "csv",
        "uri": EXPORT_URI,
        "csvExportOptions": {"selectQuery": SQL_QUERY},
    }
}

bigquery_query = f"""
    create or replace view `{project_id}.dwh_bikesharing.temporary_stations_count` as
    select count(*) as count from `{project_id}.raw_bikesharing.stations`"""
location = "US"


@dag(
    start_date=dt.datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def level_one_dag():
    export_sql = CloudSQLExportInstanceOperator(
        task_id="export_sql",
        instance=cloud_sql_instance,
        project_id=project_id,
        gcp_conn_id=gcp_conn_id,
        body=export_body,
        validate_body=True,
        deferrable=False,
        poke_interval=10,
    )

    gcs_to_bq = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket=bucket,
        source_objects=["mysql_export/from_airflow/stations/stations.csv"],
        destination_project_dataset_table=f"{project_id}.raw_bikesharing.stations",
        schema_fields=[
            {"name": "station_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "region_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "capacity", "type": "INTEGER", "mode": "NULLABLE"},
        ],
        source_format="CSV",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        field_delimiter=",",
        encoding="UTF-8",
        gcp_conn_id=gcp_conn_id,
        deferrable=False,
        cancel_on_kill="True",
        force_rerun="True",
        project_id=project_id,
    )
    # sql query to count the number of records -> dwh_bikesharing.temporary_stations_count
    bq_query_job = BigQueryInsertJobOperator(
        task_id="bq_query_job",
        gcp_conn_id=gcp_conn_id,
        configuration={
            "query": {
                "query": bigquery_query,
                "useLegacySql": False,
            }
        },
    )

    export_sql >> gcs_to_bq >> bq_query_job


level_one_dag()
