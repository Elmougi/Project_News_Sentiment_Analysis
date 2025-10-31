import os
import time
import json
import random
import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# ============ CONFIG ============
MAX_ARTICLES_PER_SECTION = 5  # control how many per section
SCROLLS_PER_SECTION = max(2, MAX_ARTICLES_PER_SECTION // 10)
OUTPUT_DIR = "scraped_data/youm7"
os.makedirs(OUTPUT_DIR, exist_ok=True)

SECTIONS = {
    "عاجل": "https://www.youm7.com/Section/أخبار-عاجلة/65/1",
    "سياسة": "https://www.youm7.com/Section/سياسة/319/1",
    "اقتصاد": "https://www.youm7.com/Section/اقتصاد-وبورصة/297/1",
    "رياضة": "https://www.youm7.com/Section/رياضة/298/1",
    "فن": "https://www.youm7.com/Section/فن/48/1",
    "تكنولوجيا": "https://www.youm7.com/Section/علوم-و-تكنولوجيا/328/1",
}


# ============ LOAD PREVIOUS DATA ============
existing_articles = []
existing_links = set()

latest_file = None
if os.path.exists(OUTPUT_DIR):
    files = sorted(
        [f for f in os.listdir(OUTPUT_DIR) if f.endswith(".json")],
        reverse=True
    )
    if files:
        latest_file = os.path.join(OUTPUT_DIR, files[0])
        with open(latest_file, "r", encoding="utf-8") as f:
            existing_articles = json.load(f)
            existing_links = {a["Link"] for a in existing_articles}
        print(f"[+] Loaded {len(existing_articles)} existing articles from {latest_file}")

# ============ SETUP DRIVER ============
options = Options()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--window-size=1920,1080")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

base_url = "https://www.youm7.com"
new_articles = []

# ============ SCRAPE ============
for section_name, section_url in SECTIONS.items():
    print(f"\n[+] Scraping section: {section_name} ({section_url})")
    driver.get(section_url)
    time.sleep(5)

    # Scroll dynamically
    for _ in range(SCROLLS_PER_SECTION):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(random.uniform(2, 3))

    soup = BeautifulSoup(driver.page_source, "lxml")
    cards = soup.find_all("div", class_="col-xs-12 bigOneSec")[:MAX_ARTICLES_PER_SECTION]

    for idx, card in enumerate(cards, start=1):
        try:
            title_tag = card.find("h3").find("a")
            title = title_tag.get_text(strip=True)
            link = title_tag.get("href")
            if not link.startswith("http"):
                link = base_url + link

            # Skip if already scraped
            if link in existing_links:
                print(f"   ⏩ Skipping duplicate: {title[:60]}...")
                continue

            img_tag = card.find("img")
            image_url = img_tag.get("src") if img_tag else ""

            date_tag = card.find("span", class_="newsDate2")
            pub_date = date_tag.get_text(strip=True) if date_tag else ""

            desc_tag = card.find("p")
            summary = desc_tag.get_text(strip=True) if desc_tag else ""

            # Open article page
            driver.get(link)
            time.sleep(2)
            article_soup = BeautifulSoup(driver.page_source, "lxml")
            paragraphs = article_soup.select("div.newsStory p")
            full_text = " ".join([p.get_text(strip=True) for p in paragraphs])

            if not full_text:
                paragraphs = article_soup.find_all("p")
                full_text = " ".join([p.get_text(strip=True) for p in paragraphs])

            article_data = {
                "Section": section_name,
                "Title": title,
                "Link": link,
                "Date": pub_date,
                "Summary": summary,
                "Full_Article": full_text,
                "Image": image_url
            }

            new_articles.append(article_data)
            existing_links.add(link)
            print(f"   ✔ ({idx}/{MAX_ARTICLES_PER_SECTION}) {title[:70]}...")

        except Exception as e:
            print(f"   ⚠ Error scraping article {idx}: {e}")

print("\n[+] Finished scraping all sections.")

# ============ SAVE MERGED DATA ============
all_articles = existing_articles + new_articles
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
output_path = os.path.join(OUTPUT_DIR, f"youm7_all_sections_{timestamp}.json")

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(all_articles, f, ensure_ascii=False, indent=4)

print(f"[+] Saved total of {len(all_articles)} articles (including {len(new_articles)} new).")
print(f"[+] Output file: {output_path}")


driver.quit()
