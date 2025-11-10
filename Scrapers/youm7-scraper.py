import time
import json
import random
import datetime
import boto3
import logging
import os
from botocore.exceptions import ClientError
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# ============ LOGGING CONFIG ============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("youm7_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============ CONFIG ============
MAX_ARTICLES_PER_SECTION = 5
SCROLLS_PER_SECTION = max(2, MAX_ARTICLES_PER_SECTION // 10)

AWS_BUCKET_NAME = "sentiment-data-lake"
AWS_REGION = "eu-north-1"
S3_PREFIX = "raw/youm7/"
S3_SEEN_LINKS_KEY = f"{S3_PREFIX}seen_links.json"

SECTIONS = {
    "عاجل": "https://www.youm7.com/Section/أخبار-عاجلة/65/1",
    "سياسة": "https://www.youm7.com/Section/سياسة/319/1",
    "اقتصاد": "https://www.youm7.com/Section/اقتصاد-وبورصة/297/1",
    "رياضة": "https://www.youm7.com/Section/رياضة/298/1",
    "فن": "https://www.youm7.com/Section/فن/48/1",
    "تكنولوجيا": "https://www.youm7.com/Section/علوم-و-تكنولوجيا/328/1",
}

# ============ AWS S3 FUNCTIONS ============
def init_s3_client():
    try:
        s3_client = boto3.client("s3", region_name=AWS_REGION)
        return s3_client
    except Exception as e:
        logger.error(f"S3 client initialization failed: {e}")
        return None


def upload_to_s3(s3_client, data, s3_key):
    try:
        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(data, ensure_ascii=False, indent=2),
            ContentType="application/json"
        )
        logger.info(f"Uploaded to s3://{AWS_BUCKET_NAME}/{s3_key}")
        return True
    except ClientError as e:
        logger.error(f"S3 upload failed: {e}")
        return False


def load_seen_links_from_s3(s3_client):
    try:
        response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=S3_SEEN_LINKS_KEY)
        data = json.loads(response["Body"].read().decode("utf-8"))
        logger.info(f"Loaded {len(data)} seen links from S3")
        return set(data)
    except s3_client.exceptions.NoSuchKey:
        logger.info("No previous seen_links.json found (first run).")
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
        logger.info(f"Saved {len(seen_links)} total seen links to S3")
    except Exception as e:
        logger.error(f"Failed to save seen links: {e}")


# ============ SELENIUM SCRAPER FUNCTIONS ============
def init_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver


def scrape_article_content(driver, link):
    try:
        driver.get(link)
        time.sleep(2)
        article_soup = BeautifulSoup(driver.page_source, "lxml")

        paragraphs = article_soup.select("div.newsStory p")
        if not paragraphs:
            paragraphs = article_soup.find_all("p")

        full_text = " ".join([p.get_text(strip=True) for p in paragraphs])
        return full_text.strip()
    except Exception as e:
        logger.warning(f"Failed to scrape article content: {e}")
        return ""


def scrape_section(driver, section_name, section_url, seen_links):
    articles = []
    base_url = "https://www.youm7.com"

    try:
        driver.get(section_url)
        time.sleep(5)

        for _ in range(SCROLLS_PER_SECTION):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(2, 3))

        soup = BeautifulSoup(driver.page_source, "lxml")
        cards = soup.find_all("div", class_="col-xs-12 bigOneSec")[:MAX_ARTICLES_PER_SECTION]

        for card in cards:
            try:
                title_tag = card.find("h3").find("a")
                title = title_tag.get_text(strip=True)
                link = title_tag.get("href")

                if not link.startswith("http"):
                    link = base_url + link

                if link in seen_links:
                    continue

                img_tag = card.find("img")
                image_url = img_tag.get("src") if img_tag else ""

                date_tag = card.find("span", class_="newsDate2")
                pub_date = date_tag.get_text(strip=True) if date_tag else ""

                desc_tag = card.find("p")
                summary = desc_tag.get_text(strip=True) if desc_tag else ""

                full_text = scrape_article_content(driver, link)
                if not full_text:
                    continue

                article_data = {
                    "source": "youm7",
                    "language": "ar",
                    "category": section_name,
                    "title": title,
                    "url": link,
                    "summary": summary,
                    "full_text": full_text,
                    "image_url": image_url,
                    "published_date": pub_date,
                    "scraped_at": datetime.datetime.now(datetime.UTC).isoformat()
                }

                articles.append(article_data)
                seen_links.add(link)

            except Exception as e:
                logger.warning(f"Failed to scrape article in {section_name}: {e}")
                continue

        logger.info(f"Section '{section_name}': scraped {len(articles)} new articles")

    except Exception as e:
        logger.error(f"Failed to scrape section '{section_name}': {e}")

    return articles


# ============ MAIN PIPELINE ============
def main():
    logger.info("Starting Youm7 scraper pipeline")

    s3_client = init_s3_client()
    if not s3_client:
        logger.error("Failed to initialize S3 client")
        return

    seen_links = load_seen_links_from_s3(s3_client)
    driver = init_driver()
    scraped_articles = []

    for section_name, section_url in SECTIONS.items():
        articles = scrape_section(driver, section_name, section_url, seen_links)
        scraped_articles.extend(articles)

    driver.quit()
    logger.info(f"Scraped {len(scraped_articles)} new articles")

    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{S3_PREFIX}youm7_{timestamp}.json"

    if scraped_articles and upload_to_s3(s3_client, scraped_articles, s3_key):
        save_seen_links_to_s3(s3_client, seen_links)
        logger.info("Pipeline completed successfully.")
    else:
        logger.warning("No new articles scraped or upload failed.")


if __name__ == "__main__":
    main()
