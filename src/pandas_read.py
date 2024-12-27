from pathlib import Path

import pandas as pd

# df = pd.read_csv(Path(__file__).parent.parent / "data.CSV")

# print(df.head())

data = {"Name": ["Paul", "Bob", "Susan", "Yolanda"], "Age": [23, 45, 18, 21]}

data_df = pd.DataFrame(data)

data_df.to_csv("fromdf.csv", index=False)
