import json

with open("file.json", "r") as f:
    json_data = json.load(f)
    print(json_data["records"][0])
    print(json_data["records"][0]["name"])
