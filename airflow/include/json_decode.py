import json

from google.cloud.storage import Blob, Bucket, Client

client = Client()
bucket = "galvanic-flame-384620-data-bucket"
object_name = "from-git/chapter-4/code/schema/regions_schema.json"

bucket_obj = Bucket(client=client, name=bucket)
blob = Blob(name=object_name, bucket=bucket_obj)
schema_json_string: str = blob.download_as_string().decode("utf-8")
schema_json = json.loads(schema_json_string)
print(schema_json)
