import datetime as dt

import pandas as pd

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# Python function to convert CSV to JSON
def csv_to_json() -> None:
    df = pd.read_csv("/home/levi/de-with-python/data.CSV")
    print(df.head(2)["name"])
    df.to_json("/home/levi/de-with-python/fromairflow.json", orient="records")


# Default arguments for the DAG
default_args = {
    "owner": "levi",
    "start_date": dt.datetime(2020, 3, 18),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="convert_csv_to_json",
    default_args=default_args,
    schedule="0 * * * *",
    catchup=False,
) as dag:
    # Define tasks
    print_starting = BashOperator(
        task_id="print_starting", bash_command="echo 'I am reading the CSV now...'"
    )

    csv_json = PythonOperator(task_id="csv_json", python_callable=csv_to_json)

    # Define task dependencies
    print_starting >> csv_json
