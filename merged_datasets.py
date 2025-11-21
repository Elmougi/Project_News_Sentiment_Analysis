import json
import pandas as pd

# ------------ BBC ------------
with open("bbc_20251110_005854.json", "r", encoding="utf-8") as f:
    bbc_data = json.load(f)

bbc_rows = []
for item in bbc_data:
    bbc_rows.append({
        "source": "BBC",
        "language": "en",
        "category": item.get("category"),
        "text": item.get("summary", "") + " " + item.get("full_text", "")
    })

# ------------ YOUm7 ------------
with open("youm7_20251110_011152.json", "r", encoding="utf-8") as f:
    youm7_data = json.load(f)

youm7_rows = []
for item in youm7_data:
    youm7_rows.append({
        "source": "Youm7",
        "language": "ar",
        "category": item.get("category"),
        "text": item.get("summary", "") + " " + item.get("full_text", "")
    })

# ------------ MERGE ------------
df = pd.DataFrame(bbc_rows + youm7_rows)
df = df.dropna(subset=["text"])
df.to_csv("merged_news.csv", index=False, encoding="utf-8")

print(" Done! merged_news.csv created — rows:", len(df))


#to run the script, use the command: python merge_datasets.py
