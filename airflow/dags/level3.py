import datetime as dt

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook, parse_json_from_gcs
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)

# cloudsql export
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLExportInstanceOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

# gcs to gcs
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator


def read_json_schema_from_gcs(
    bucket_name: str, object_name: str, gcp_conn_id: str = "google_cloud_default"
):
    """Reads a JSON schema file from a GCS bucket and returns it as a Python dictionary."""
    return parse_json_from_gcs(
        gcp_conn_id=gcp_conn_id, file_uri=f"gs://{bucket_name}/{object_name}"
    )


# airflow variables
settings = Variable.get("level_3_dag_settings", deserialize_json=True)

# dag variables
gcs_source_data_bucket = settings["gcs_source_data_bucket"]

bq_raw_dataset = settings["bq_raw_dataset"]
bq_dwh_dataset = settings["bq_dwh_dataset"]

gcp_project_id = "galvanic-flame-384620"
instance_name = "mysql-instance-source"
gcp_conn_id = "galvanic-flame"

# macros
execution_date = "{{ds}}"

# Stations
station_source_object = "chapter-4/stations/stations.csv"
sql_query = "SELECT * FROM apps_db.stations"

export_mysql_body = {
    "exportContext": {
        "fileType": "csv",
        "uri": f"gs://{gcs_source_data_bucket}/{station_source_object}",
        "csvExportOptions": {"selectQuery": sql_query},
    }
}


bq_stations_table_name = "stations"
bq_stations_table_id = f"{gcp_project_id}.{bq_raw_dataset}.{bq_stations_table_name}"
bq_stations_table_schema = read_json_schema_from_gcs(
    bucket_name=gcs_source_data_bucket,
    object_name="from-git/chapter-4/code/schema/stations_schema.json",
    gcp_conn_id=gcp_conn_id,
)

# Regions
gcs_regions_source_object = "from-git/chapter-3/dataset/regions/regions.csv"
gcs_regions_target_object = "chapter-4/regions/regions.csv"
bq_regions_table_name = "regions"
bq_regions_table_id = f"{gcp_project_id}.{bq_raw_dataset}.{bq_regions_table_name}"
bq_regions_table_schema = read_json_schema_from_gcs(
    bucket_name=gcs_source_data_bucket,
    object_name="from-git/chapter-4/code/schema/regions_schema.json",
    gcp_conn_id=gcp_conn_id,
)

# Trips
bq_temporary_extract_dataset_name = "temporary_staging"
bq_temporary_extract_table_name = "trips"
bq_temporary_table_id = f"{gcp_project_id}.{bq_temporary_extract_dataset_name}.{bq_temporary_extract_table_name}"

gcs_trips_source_object = "chapter-4/trips/trips.csv"
gcs_trips_source_uri = f"gs://{gcs_source_data_bucket}/{gcs_trips_source_object}"

bq_trips_table_name = "trips"
bq_trips_table_id = f"{gcp_project_id}.{bq_raw_dataset}.{bq_trips_table_name}"
bq_trips_table_schema = read_json_schema_from_gcs(
    bucket_name=gcs_source_data_bucket,
    object_name="from-git/chapter-4/code/schema/trips_schema.json",
    gcp_conn_id=gcp_conn_id,
)

# DWH
bq_fact_trips_daily_table_name = "facts_trips_daily"
bq_fact_trips_daily_table_id = (
    f"{gcp_project_id}.{bq_dwh_dataset}.{bq_fact_trips_daily_table_name}"
)

bq_dim_stations_table_name = "dim_stations"
bq_dim_stations_table_id = (
    f"{gcp_project_id}.{bq_dwh_dataset}.{bq_dim_stations_table_name}"
)


@dag(
    start_date=dt.datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def level_three_dag():
    # load station table
    export_mysql_station = CloudSQLExportInstanceOperator(
        task_id="export_mysql_station",
        project_id=gcp_project_id,
        body=export_mysql_body,
        instance=instance_name,
        gcp_conn_id=gcp_conn_id,
    )

    gcs_to_bq_station = GCSToBigQueryOperator(
        task_id="gcs_to_bq_station",
        bucket=gcs_source_data_bucket,
        source_objects=[station_source_object],
        destination_project_dataset_table=bq_stations_table_id,
        schema_fields=bq_stations_table_schema,
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id=gcp_conn_id,
    )

    # load regions table
    gcs_to_gcs_region = GCSToGCSOperator(
        task_id="gcs_to_gcs_region",
        source_bucket=gcs_source_data_bucket,
        source_object=gcs_regions_source_object,
        destination_bucket=gcs_source_data_bucket,
        destination_object=gcs_regions_target_object,
        gcp_conn_id=gcp_conn_id,
    )
    gcs_to_bq_region = GCSToBigQueryOperator(
        task_id="gcs_to_bq_region",
        bucket=gcs_source_data_bucket,
        source_objects=[gcs_regions_target_object],
        destination_project_dataset_table=bq_regions_table_id,
        schema_fields=bq_regions_table_schema,
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id=gcp_conn_id,
    )

    # load trips table
    staging_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="staging_dataset",
        gcp_conn_id=gcp_conn_id,
        location="US",
        project_id=gcp_project_id,
        dataset_id=bq_temporary_extract_dataset_name,
        if_exists="ignore",
    )

    bq_to_bq_trips = BigQueryInsertJobOperator(
        task_id="bq_to_bq_trips",
        gcp_conn_id=gcp_conn_id,
        configuration={
            "query": {
                "query": f""" 
                
                
                create or replace table {bq_temporary_table_id}
                as
                    SELECT * FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
                        WHERE DATE(start_date) = DATE('2018-04-30')
                ;""",
                "useLegacySql": False,
            }
        },
        force_rerun=True,
        location="US",
    )

    bq_to_gcs_trips = BigQueryToGCSOperator(
        task_id="bq_to_gcs_trips",
        source_project_dataset_table=bq_temporary_table_id,
        destination_cloud_storage_uris=[gcs_trips_source_uri],
        project_id=gcp_project_id,
        export_format="CSV",
        gcp_conn_id=gcp_conn_id,
        location="US",
        force_rerun=True,
    )

    gcs_to_bq_trips = GCSToBigQueryOperator(
        task_id="gcs_to_bq_trips",
        bucket=gcs_source_data_bucket,
        source_objects=[gcs_trips_source_object],
        destination_project_dataset_table=bq_trips_table_id,
        schema_fields=bq_trips_table_schema,
        write_disposition="WRITE_APPEND",
        gcp_conn_id=gcp_conn_id,
        force_rerun=True,
    )

    # load dwh tables
    dwh_facts_trips_daily = BigQueryInsertJobOperator(
        task_id="dwh_facts_trips_daily",
        gcp_conn_id=gcp_conn_id,
        configuration={
            "query": {
                "query": f"""
                create or replace table {bq_fact_trips_daily_table_id} as 
                SELECT 
                    DATE(start_date) AS trip_date,
                    start_station_id,
                    COUNT(trip_id) AS total_trips,
                    SUM(duration_sec) AS sum_duration_sec,
                    AVG(duration_sec) AS avg_duration_sec
                FROM {bq_trips_table_id}
                WHERE DATE(start_date) = DATE('2018-04-30')
                GROUP BY DATE(start_date), start_station_id;""",
                "useLegacySql": False,
            }
        },
        force_rerun=True,
        location="US",
    )

    dwh_dim_stations = BigQueryInsertJobOperator(
        task_id="dwh_dim_stations",
        gcp_conn_id=gcp_conn_id,
        configuration={
            "query": {
                "query": f"""
                create or replace table {bq_dim_stations_table_id}
                as
                SELECT 
                    station_id,
                    stations.name AS station_name,
                    regions.name AS region_name,
                    capacity
                FROM {bq_stations_table_id} AS stations
                JOIN {bq_regions_table_id} AS regions
                ON stations.region_id = CAST(regions.region_id AS STRING);
                """,
                "useLegacySql": False,
            }
        },
        force_rerun=True,
    )

    bq_row_count_check_dwh_fact_trips_daily = BigQueryCheckOperator(
        task_id="bq_row_count_check_dwh_fact_trips_daily",
        sql=f"""
        select count(*) from {bq_fact_trips_daily_table_id}
        """,
        use_legacy_sql=False,
        gcp_conn_id=gcp_conn_id,
    )

    bq_row_count_check_dwh_dim_stations = BigQueryCheckOperator(
        task_id="bq_row_count_check_dwh_dim_stations",
        sql=f"""
    select count(*) from {bq_dim_stations_table_id}
    """,
        use_legacy_sql=False,
        gcp_conn_id=gcp_conn_id,
    )

    # load data mart
    export_mysql_station >> gcs_to_bq_station
    gcs_to_gcs_region >> gcs_to_bq_region
    staging_dataset >> bq_to_bq_trips >> bq_to_gcs_trips >> gcs_to_bq_trips
    (
        [gcs_to_bq_station, gcs_to_bq_region, gcs_to_bq_trips]
        >> dwh_facts_trips_daily
        >> bq_row_count_check_dwh_fact_trips_daily
    )
    (
        [gcs_to_bq_station, gcs_to_bq_region, gcs_to_bq_trips]
        >> dwh_dim_stations
        >> bq_row_count_check_dwh_dim_stations
    )


level_three_dag()
