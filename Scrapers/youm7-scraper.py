import os
import time
import json
import random
import datetime
import boto3
import logging
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# ======================================================
# LOGGING
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("youm7_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ======================================================
# CONFIG
# ======================================================
MAX_ARTICLES_PER_SECTION = 6
SCROLLS_PER_SECTION = 2

AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")  
AWS_REGION = os.getenv("AWS_REGION")
if not AWS_BUCKET_NAME:
    raise RuntimeError("Set AWS_BUCKET_NAME env var before running")

S3_PREFIX = "raw/arabic/youm7/"
S3_SEEN_LINKS_KEY = f"{S3_PREFIX}seen_links.json"

SECTIONS = {
    "عاجل": "https://www.youm7.com/Section/أخبار-عاجلة/65/1",
    "سياسة": "https://www.youm7.com/Section/سياسة/319/1",
    "اقتصاد": "https://www.youm7.com/Section/اقتصاد-وبورصة/297/1",
    "رياضة": "https://www.youm7.com/Section/رياضة/298/1",
    "فن": "https://www.youm7.com/Section/فن/48/1",
    "تكنولوجيا": "https://www.youm7.com/Section/علوم-و-تكنولوجيا/328/1",
}

# Noise phrases to strip from full_text
NOISE_PATTERNS = [
    "تم التصميم والتطوير بواسطة",
    "الموضوعات المتعلقة",
    "موضوعات متعلقة",
    "اقرأ أيضا",
    "اقرأ ايضا",
    "تابعونا",
]


# ======================================================
# AWS FUNCTIONS
# ======================================================
def init_s3():
    return boto3.client("s3", region_name=AWS_REGION)


def load_seen_links(s3):
    try:
        obj = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=S3_SEEN_LINKS_KEY)
        links = json.loads(obj["Body"].read().decode("utf-8"))
        return set(links)
    except:
        return set()


def save_seen_links(s3, seen):
    s3.put_object(
        Bucket=AWS_BUCKET_NAME,
        Key=S3_SEEN_LINKS_KEY,
        Body=json.dumps(list(seen), ensure_ascii=False, indent=2),
        ContentType="application/json"
    )


# ======================================================
# SELENIUM DRIVER
# ======================================================
def init_driver():
    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts
    )
    return driver


# ======================================================
# CLEANERS
# ======================================================
def clean_text(txt):
    if not txt:
        return ""

    for n in NOISE_PATTERNS:
        txt = txt.replace(n, "")

    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


# ======================================================
# SCRAPE FULL ARTICLE CONTENT
# ======================================================
def scrape_full_article(driver, url):
    try:
        driver.get(url)
        time.sleep(2)

        soup = BeautifulSoup(driver.page_source, "lxml")

        # Prefer og:image
        og = soup.find("meta", property="og:image")
        image_page = og["content"] if og else ""

        container = soup.select_one("div.newsStory") or soup
        paras = container.find_all("p")

        text_body = " ".join([p.get_text(strip=True) for p in paras])
        text_body = clean_text(text_body)

        if len(text_body) < 40:
            return text_body, image_page

        return text_body, image_page

    except Exception as e:
        logger.error(f"Full article scrape failed: {e}")
        return "", ""


# ======================================================
# SCRAPE SECTION PAGE
# ======================================================
def scrape_section(driver, name, url, seen):
    driver.get(url)
    time.sleep(3)

    # Scroll for dynamic loading
    for _ in range(SCROLLS_PER_SECTION):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

    soup = BeautifulSoup(driver.page_source, "lxml")

    cards = soup.find_all("div", class_="col-xs-12 bigOneSec")
    cards = cards[:MAX_ARTICLES_PER_SECTION]

    base = "https://www.youm7.com"
    results = []

    for card in cards:
        try:
            title_tag = card.find("h3").find("a")
            title = title_tag.get_text(strip=True)
            link = title_tag["href"]

            if not link.startswith("http"):
                link = base + link

            if link in seen:
                continue

            # Card image
            img_tag = card.find("img")
            card_image = img_tag["src"] if img_tag else ""

            # Card summary
            desc = card.find("p")
            summary = desc.get_text(strip=True) if desc else ""

            # FIXED DATE
            date_tag = card.find("span", class_="newsDate2")
            pub_date = date_tag.get_text(strip=True) if date_tag else ""

            # Scrape full text & article clean image
            full_text, article_image = scrape_full_article(driver, link)

            if len(full_text) < 50:
                continue

            img_final = article_image if article_image else card_image

            article = {
                "source": "youm7",
                "language": "ar",
                "category": name,
                "title": title,
                "url": link,
                "summary": summary,
                "full_text": full_text,
                "image_url": img_final,
                "published_date": pub_date,
                "scraped_at": datetime.datetime.utcnow().isoformat()
            }

            results.append(article)
            seen.add(link)

        except Exception as e:
            logger.warning(f"Error scraping card: {e}")
            continue

    return results


# ======================================================
# MAIN PIPELINE
# ======================================================
def main():
    s3 = init_s3()
    seen = load_seen_links(s3)

    driver = init_driver()
    all_articles = []

    for name, url in SECTIONS.items():
        print(f"\nScraping: {name}")
        arts = scrape_section(driver, name, url, seen)
        all_articles.extend(arts)

    driver.quit()

    if all_articles:
        ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        key = f"{S3_PREFIX}youm7_{ts}.json"

        s3.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=key,
            Body=json.dumps(all_articles, ensure_ascii=False, indent=2),
            ContentType="application/json"
        )

        save_seen_links(s3, seen)

        print(f"\nSaved {len(all_articles)} articles.")
    else:
        print("NO NEW ARTICLES SCRAPED.")


if __name__ == "__main__":
    main()
