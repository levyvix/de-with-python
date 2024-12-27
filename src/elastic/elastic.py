from elasticsearch import Elasticsearch, helpers
from faker import Faker

client = Elasticsearch(
    "http://localhost:9200",
    api_key="SHlpWjhKTUI5cmsxdUdrbXB3ZUE6bEp2UDJHMXpRck9RODJER3dxcDk3Zw==",
)
fake = Faker()


actions = [
    {
        "_index": "users",
        "_source": {
            "name": fake.name(),
            "street": fake.street_address(),
            "city": fake.city(),
            "zip": fake.zipcode(),
        },
    }
    for x in range(998)
]

res = helpers.bulk(client=client, actions=actions)
