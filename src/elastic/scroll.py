from elasticsearch import Elasticsearch

client = Elasticsearch(
    hosts="http://localhost:9200",
    api_key="SHlpWjhKTUI5cmsxdUdrbXB3ZUE6bEp2UDJHMXpRck9RODJER3dxcDk3Zw==",
)


res = client.search(
    index="users",
    scroll="20m",
    size=500,
    body={"query": {"match_all": {}}},
)

sid = res["_scroll_id"]
size = res["hits"]["total"]["value"]

while size > 0:
    res = client.scroll(scroll_id=sid, scroll="20m")
    for doc in res["hits"]["hits"]:
        print(doc["_source"])
    sid = res["_scroll_id"]
    size = res["hits"]["total"]["value"]
