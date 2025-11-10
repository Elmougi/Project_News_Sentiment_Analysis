import requests
from bs4 import BeautifulSoup
import json
import datetime
import time
import random
import boto3
import logging
from botocore.exceptions import ClientError
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ============ LOGGING CONFIG ============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ============ CONFIG ============
BBC_FEEDS = {
    "world_en": "https://feeds.bbci.co.uk/news/world/rss.xml",
    "business_en": "https://feeds.bbci.co.uk/news/business/rss.xml",
    "technology_en": "https://feeds.bbci.co.uk/news/technology/rss.xml",
    "science_en": "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml",
    "health_en": "https://feeds.bbci.co.uk/news/health/rss.xml",
    "arabic": "https://feeds.bbci.co.uk/arabic/rss.xml"
}

AWS_BUCKET_NAME = "sentiment-data-lake"
AWS_REGION = "eu-north-1"
S3_PREFIX = "raw/bbc/"
S3_SEEN_LINKS_KEY = f"{S3_PREFIX}seen_links.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "en-US,en;q=0.9,ar;q=0.8"
}

# ============ SELENIUM SETUP ============
def init_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# ============ AWS HELPERS ============
def init_s3_client():
    try:
        return boto3.client("s3", region_name=AWS_REGION)
    except Exception as e:
        logger.error(f"S3 client initialization failed: {e}")
        return None

def load_seen_links_from_s3(s3_client):
    try:
        response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=S3_SEEN_LINKS_KEY)
        data = json.loads(response["Body"].read().decode("utf-8"))
        logger.info(f"Loaded {len(data)} seen links from S3.")
        return set(data)
    except s3_client.exceptions.NoSuchKey:
        logger.info("No seen_links.json found — starting fresh.")
        return set()
    except Exception as e:
        logger.warning(f"Failed to load seen links: {e}")
        return set()

def save_seen_links_to_s3(s3_client, seen_links):
    try:
        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=S3_SEEN_LINKS_KEY,
            Body=json.dumps(list(seen_links), ensure_ascii=False, indent=2),
            ContentType="application/json"
        )
        logger.info(f"Saved {len(seen_links)} seen links to S3.")
    except Exception as e:
        logger.error(f"Failed to save seen links: {e}")

def upload_to_s3(s3_client, data, s3_key):
    try:
        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(data, ensure_ascii=False, indent=2),
            ContentType="application/json"
        )
        logger.info(f"Uploaded file to s3://{AWS_BUCKET_NAME}/{s3_key}")
        return True
    except ClientError as e:
        logger.error(f"S3 upload failed: {e}")
        return False

# ============ HELPERS ============
def detect_language_from_key(feed_key: str) -> str:
    return "ar" if "arabic" in feed_key else "en"

def extract_image_url(soup):
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        return og["content"]
    img = soup.find("img", {"data-testid": "image-block-image"})
    if img and img.get("src"):
        return img["src"]
    img_tag = soup.find("img")
    if img_tag and img_tag.get("src"):
        return img_tag["src"]
    source_tag = soup.find("source")
    if source_tag and source_tag.get("srcset"):
        return source_tag["srcset"].split(" ")[0]
    return ""

def clean_article_paragraphs(soup, lang="en"):
    for selector in ["section", "aside", "footer", "nav", "div[id*='related']", "div[data-component='related-content']"]:
        for tag in soup.select(selector):
            tag.decompose()

    stop_words = ["related", "cookies", "why you can trust bbc", "بي بي سي ليست مسؤولة", "تفضيلات القراء"]
    paragraphs = []
    for p in soup.find_all("p"):
        text = p.get_text(strip=True)
        if not text:
            continue
        if any(w in text.lower() for w in stop_words):
            break
        paragraphs.append(text)
    return " ".join(paragraphs).strip()

def extract_full_text(driver, url, lang="en"):
    try:
        driver.get(url)
        time.sleep(random.uniform(1.5, 2.5))
        soup = BeautifulSoup(driver.page_source, "lxml")
        text = clean_article_paragraphs(soup, lang)
        image_url = extract_image_url(soup)
        return text, image_url
    except Exception as e:
        logger.warning(f"Failed to extract full text: {e}")
        return "", ""

# ============ MAIN SCRAPER ============
def scrape_bbc_multilang(seen_links):
    driver = init_driver()
    all_articles = []

    for feed_key, feed_url in BBC_FEEDS.items():
        lang = detect_language_from_key(feed_key)
        category = feed_key.replace(f"_{lang}", "").replace("arabic", "arabic_general")

        try:
            rss = requests.get(feed_url, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(rss.text, "xml")
            items = soup.find_all("item")
        except Exception as e:
            logger.warning(f"Failed to fetch RSS feed {feed_url}: {e}")
            continue

        for item in items[:10]:
            title = item.title.text.strip() if item.title else ""
            link = item.link.text.strip() if item.link else ""
            summary = item.description.text.strip() if item.description else ""
            published_date = item.pubDate.text.strip() if item.pubDate else ""

            if not link or link in seen_links or "/av/" in link or "/live/" in link:
                continue
            seen_links.add(link)

            full_text, image_url = extract_full_text(driver, link, lang)
            if not full_text:
                continue

            article = {
                "source": "BBC",
                "language": lang,
                "category": category,
                "title": title,
                "url": link,
                "summary": summary,
                "full_text": full_text,
                "image_url": image_url,
                "published_date": published_date,
                "scraped_at": datetime.datetime.now(datetime.UTC).isoformat()
            }
            all_articles.append(article)
            time.sleep(random.uniform(1.5, 2.5))

    driver.quit()
    return all_articles

# ============ MAIN PIPELINE ============
def main():
    logger.info("Starting BBC scraper pipeline")
    s3_client = init_s3_client()
    if not s3_client:
        logger.error("Failed to initialize S3 client")
        return

    seen_links = load_seen_links_from_s3(s3_client)
    articles = scrape_bbc_multilang(seen_links)

    if not articles:
        logger.warning("No new articles scraped")
        return

    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{S3_PREFIX}bbc_{timestamp}.json"

    if upload_to_s3(s3_client, articles, s3_key):
        save_seen_links_to_s3(s3_client, seen_links)
        logger.info(f"BBC pipeline completed successfully with {len(articles)} new articles")
    else:
        logger.error("Upload failed")

if __name__ == "__main__":
    main()
