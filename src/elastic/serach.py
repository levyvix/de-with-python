from elasticsearch import Elasticsearch

client = Elasticsearch(
    hosts="http://localhost:9200",
    api_key="SHlpWjhKTUI5cmsxdUdrbXB3ZUE6bEp2UDJHMXpRck9RODJER3dxcDk3Zw==",
)


# doc = {"query": {"match": {"name": "Ronald Goodman"}}}
# res = client.search(index="users", body=doc)

# res = client.search(index="users", q="city:jamesberg")

doc = {
    "query": {
        "bool": {
            "must": {"match": {"city": "Jamesberg"}},
            "filter": {"term": {"zip": "63792"}},
        }
    }
}
res = client.search(index="users", body=doc)


for r in res["hits"]["hits"]:
    print(r["_source"])
