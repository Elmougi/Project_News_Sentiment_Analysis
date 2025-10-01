import requests
import pandas as pd

GDELT_API = "https://api.gdeltproject.org/api/v2/doc/doc"
QUERY = "egypt economy"
MAXRECORDS = 50

def extract_gdelt(query=QUERY, maxrecords=MAXRECORDS):
    params = {"query": query, "format": "json", "maxrecords": maxrecords}
    r = requests.get(GDELT_API, params=params)
    r.raise_for_status()
    r.encoding = "utf-8"
    data = r.json()

    articles = pd.DataFrame(data.get("articles", []))
    articles["title"] = articles["title"].astype(str)

    return articles

if __name__ == "__main__":
    df = extract_gdelt()
    print(df[["title"]].head())   
    df.to_csv("gdelt_articles.csv", index=False, encoding="utf-8-sig")
    print("Saved to gdelt_articles.csv")
