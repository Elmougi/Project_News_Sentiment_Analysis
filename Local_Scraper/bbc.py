import requests
from bs4 import BeautifulSoup
import json
import datetime
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ----------------
BBC_FEEDS = {
    "world_en": "https://feeds.bbci.co.uk/news/world/rss.xml",
    "business_en": "https://feeds.bbci.co.uk/news/business/rss.xml",
    "technology_en": "https://feeds.bbci.co.uk/news/technology/rss.xml",
    "science_en": "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml",
    "health_en": "https://feeds.bbci.co.uk/news/health/rss.xml",
    "arabic": "https://feeds.bbci.co.uk/arabic/rss.xml"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "en-US,en;q=0.9,ar;q=0.8"
}

OUTPUT_FILE = f"bbc_multilang_{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d_%H%M%S')}.json"

# ---------------- SELENIUM SETUP ----------------
def init_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# ---------------- HELPERS ----------------
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
    except Exception:
        return "", ""

# ---------------- MAIN SCRAPER ----------------
def scrape_bbc_multilang():
    driver = init_driver()
    all_articles = []
    seen = set()

    for feed_key, feed_url in BBC_FEEDS.items():
        lang = detect_language_from_key(feed_key)
        category = feed_key.replace(f"_{lang}", "").replace("arabic", "arabic_general")

        print(f"\n🌍 Fetching RSS feed for '{feed_key}' ({lang}) ...")
        rss = requests.get(feed_url, headers=HEADERS)
        soup = BeautifulSoup(rss.text, "xml")
        items = soup.find_all("item")
        print(f"  → Found {len(items)} items")

        for item in items[:10]:
            title = item.title.text.strip() if item.title else ""
            link = item.link.text.strip() if item.link else ""
            summary = item.description.text.strip() if item.description else ""
            published_date = item.pubDate.text.strip() if item.pubDate else ""

            if not link or link in seen or "/av/" in link or "/live/" in link:
                continue
            seen.add(link)

            print(f"📰 {title[:80]}...")
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

# ---------------- MAIN ----------------
def main():
    print("🚀 Starting multilingual BBC scraper...")
    articles = scrape_bbc_multilang()
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Done. Scraped {len(articles)} BBC articles.")
    print(f"💾 Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
