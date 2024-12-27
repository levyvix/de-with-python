import pandas as pd
from google.cloud import bigquery

df = pd.DataFrame(
    {
        "id": [1, 2, 3, 4],
        "name": ["a", "b", "c", "d"],
    }
)

client = bigquery.Client(project="galvanic-flame-384620")

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)

# Include target partition in the table id:
table_id = "galvanic-flame-384620.test_dataset.partitioned_dataset$20211021"
job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)  # Make an API request
a = job.result()  # Wait for job to finish
print(a.error_result)
