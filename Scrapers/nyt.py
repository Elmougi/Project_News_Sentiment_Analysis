import requests
import json
import boto3
from datetime import datetime, timezone
from dotenv import load_dotenv
import os

# Load env
load_dotenv()

NYT_API_KEY = os.getenv("NYT_API_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

S3_PATH = "raw/english/newyork_times/"

s3 = boto3.client("s3", region_name=AWS_REGION)


# ---------------------------
# Normalize NYT -> Unified Schema
# ---------------------------
def normalize_nyt_item(item, category):
    title = item.get("title")
    url = item.get("url")
    summary = item.get("abstract")
    published = item.get("published_date")

    # image
    image_url = None

    if item.get("multimedia"):
        image_url = item["multimedia"][0].get("url")

    if item.get("media"):
        try:
            image_url = item["media"][0]["media-metadata"][-1]["url"]
        except:
            pass

    normalized = {
        "source": "nytimes",
        "language": "en",
        "category": category,
        "title": title,
        "url": url,
        "summary": summary,
        "full_text": None,                     # NYT API provides no full text
        "image_url": image_url,
        "published_date": published,
        "scraped_at": datetime.now(timezone.utc).isoformat()
    }

    return normalized


# ---------------------------
# Fetch NYT Top Stories
# ---------------------------
def fetch_top_stories(sections):
    articles = []
    for section in sections:
        url = f"https://api.nytimes.com/svc/topstories/v2/{section}.json"
        params = {"api-key": NYT_API_KEY}

        res = requests.get(url, params=params)
        data = res.json()

        for item in data.get("results", []):
            articles.append(
                normalize_nyt_item(item=item, category=section)
            )

    return articles


# ---------------------------
# Fetch NYT Most Popular
# ---------------------------
def fetch_most_popular():
    articles = []

    for cat in ["viewed", "shared", "emailed"]:
        for per in [1, 7]:
            url = f"https://api.nytimes.com/svc/mostpopular/v2/{cat}/{per}.json"
            params = {"api-key": NYT_API_KEY}

            res = requests.get(url, params=params)
            data = res.json()

            for item in data.get("results", []):
                category = item.get("section", "general")
                articles.append(
                    normalize_nyt_item(item=item, category=category)
                )

    return articles


# ---------------------------
# Deduplicate by URL
# ---------------------------
def dedupe_by_url(articles):
    return list({a["url"]: a for a in articles}.values())


# ---------------------------
# Save to S3
# ---------------------------
def save_to_s3(data):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"{S3_PATH}nyt_{timestamp}.json"

    s3.put_object(
        Bucket=AWS_BUCKET_NAME,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2),
        ContentType="application/json"
    )
    return key


# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":

    print("📡 Fetching NYT Top Stories...")
    top_news = fetch_top_stories(
        ["world", "home", "politics", "business", "technology"]
    )

    print("📡 Fetching NYT Most Popular...")
    popular_news = fetch_most_popular()

    print("🔄 Merging & deduplicating…")
    combined = dedupe_by_url(top_news + popular_news)

    print("📤 Uploading…")
    key = save_to_s3(combined)

    print(f"✅ Done. Total articles: {len(combined)}")
    print(f"📁 Saved to: {key}")
