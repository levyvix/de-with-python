import json
from pathlib import Path

import pandas as pd

parent_folder = Path(__file__).parent.parent

with open(parent_folder / "file.json", "r") as file:
    json_data = json.load(file)

# AVANCADO
df_json = pd.json_normalize(json_data, record_path="records")

# print(df_json.head())

print(df_json.head(5).to_json(orient="records"))
