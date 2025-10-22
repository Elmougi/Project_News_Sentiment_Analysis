import requests
import pandas as pd
import time
import logging
from bs4 import BeautifulSoup
from newspaper import Article
from tqdm import tqdm
from langdetect import detect, DetectorFactory
import re
from datetime import datetime

# For consistent language detection
DetectorFactory.seed = 0

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    handlers=[
        logging.FileHandler("bilingual_news_extractor.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

GDELT_API = "https://api.gdeltproject.org/api/v2/doc/doc"

# News sources categorized by language
ARABIC_SOURCES = [
    'almasryalyoum.com', 'youm7.com', 'alarabiya.net', 'aljazeera.net',
    'akhbarelyom.com', 'albawabhnews.com', 'shorouknews.com', 'vetogate.com',
    'alahram.org.eg', 'masrawy.com', 'elwatannews.com', 'dostor.org',
    'cairo24.com', 'alwafd.news', 'gate.ahram.org.eg', 'elnabaa.net'
]

ENGLISH_SOURCES = [
    'egyptindependent.com', 'english.ahram.org.eg', 'egypttoday.com',
    'dailynewssegypt.com', 'madamasr.com', 'reuters.com', 'bbc.com',
    'cnn.com', 'theguardian.com', 'aljazeera.com', 'bloomberg.com',
    'apnews.com', 'middleeasteye.net', 'thenationalnews.com', 'arabnews.com'
]


class BilingualNewsExtractor:
    """Extractor optimized for Arabic & English news sentiment analysis"""
    
    def __init__(self, query="egypt latest news", maxrecords=100, delay=1.5, 
                 min_content_length=200, language_filter=None):
        """
        Initialize the extractor
        
        Args:
            query: Search query
            maxrecords: Maximum number of articles to fetch
            delay: Delay between requests in seconds
            min_content_length: Minimum article length for quality filtering
            language_filter: 'arabic', 'english', 'both', or None (no filter)
        """
        self.query = query
        self.maxrecords = maxrecords
        self.delay = delay
        self.min_content_length = min_content_length
        self.language_filter = language_filter
        self.timeout = 15
        
    def extract_gdelt(self):
        """Extract article metadata from GDELT API"""
        params = {
            "query": self.query,
            "format": "json",
            "maxrecords": self.maxrecords,
            "mode": "artlist"
        }
        
        # Add language parameter based on filter
        if self.language_filter == 'arabic':
            params["sourcelang"] = "ara"
        elif self.language_filter == 'english':
            params["sourcelang"] = "eng"
        # For 'both' or None, don't add sourcelang to get mixed results
        
        try:
            r = requests.get(GDELT_API, params=params, timeout=self.timeout)
            r.raise_for_status()
            r.encoding = "utf-8"
            data = r.json()
            articles = pd.DataFrame(data.get("articles", []))
            
            if articles.empty:
                logging.warning("No articles returned from GDELT")
                return articles
                
            if "title" in articles:
                articles["title"] = articles["title"].astype(str)
            
            # Add source domain for filtering
            if "url" in articles:
                articles["domain"] = articles["url"].apply(self._extract_domain)
                articles["source_language"] = articles["domain"].apply(self._classify_source_language)
            
            logging.info(f"Fetched {len(articles)} articles from GDELT")
            
            # Filter by language if specified
            if self.language_filter and self.language_filter != 'both':
                articles = self._filter_by_language(articles, self.language_filter)
                logging.info(f"After {self.language_filter} filter: {len(articles)} articles")
            
            return articles
            
        except Exception as e:
            logging.error(f"Failed to fetch from GDELT: {e}")
            return pd.DataFrame()
    
    def _extract_domain(self, url):
        """Extract domain from URL"""
        try:
            from urllib.parse import urlparse
            return urlparse(url).netloc.replace('www.', '')
        except:
            return ""
    
    def _classify_source_language(self, domain):
        """Classify source as Arabic, English, or Unknown based on domain"""
        if any(source in domain for source in ARABIC_SOURCES):
            return "arabic"
        elif any(source in domain for source in ENGLISH_SOURCES):
            return "english"
        return "unknown"
    
    def _filter_by_language(self, df, language):
        """Filter for specific language sources"""
        if df.empty or "source_language" not in df.columns:
            return df
        
        if language == 'arabic':
            mask = df["source_language"] == "arabic"
        elif language == 'english':
            mask = df["source_language"] == "english"
        else:
            return df
        
        return df[mask].copy() if mask.any() else df
    
    def scrape_article(self, url, expected_lang=None):
        """Scrape article with language-specific optimizations"""
        # Determine language for newspaper3k
        lang_code = 'ar' if expected_lang == 'arabic' else 'en'
        
        # Try newspaper3k with appropriate language
        for attempt in range(2):
            try:
                article = Article(url, language=lang_code)
                article.download()
                article.parse()
                
                content = article.text
                
                # Validate content quality
                if len(content) < self.min_content_length:
                    logging.warning(f"Content too short ({len(content)} chars): {url}")
                    if attempt == 0:
                        time.sleep(1)
                        continue
                
                return {
                    "content": content,
                    "content_length": len(content),
                    "authors": ", ".join(article.authors) if article.authors else "",
                    "publish_date": article.publish_date,
                    "top_image": article.top_image,
                    "keywords": ", ".join(article.keywords[:5]) if hasattr(article, 'keywords') else "",
                    "status": "success",
                    "extraction_method": "newspaper3k"
                }
            except Exception as e:
                logging.warning(f"newspaper3k attempt {attempt+1} failed for {url}: {e}")
                time.sleep(1)
        
        # Fallback to BeautifulSoup
        return self.scrape_fallback(url, expected_lang)
    
    def scrape_fallback(self, url, expected_lang=None):
        """Enhanced fallback scraper for both Arabic and English sites"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept-Language': 'ar,en-US;q=0.9,en;q=0.8',
                'Accept-Charset': 'utf-8'
            }
            r = requests.get(url, headers=headers, timeout=self.timeout)
            r.raise_for_status()
            
            # Detect encoding
            if 'charset' not in r.headers.get('content-type', '').lower():
                r.encoding = 'utf-8'
            
            soup = BeautifulSoup(r.content, "html.parser")
            
            # Remove unwanted elements
            for tag in soup(["script", "style", "nav", "footer", "header", 
                           "aside", "iframe", "noscript", "meta", "link", "form",
                           "button", "input", "select", "textarea"]):
                tag.decompose()
            
            # Remove common ad/social containers
            for pattern in ['ad', 'advertisement', 'social', 'share', 'comment', 
                          'related', 'recommend', 'newsletter', 'subscription']:
                for elem in soup.find_all(attrs={"class": re.compile(pattern, re.I)}):
                    elem.decompose()
                for elem in soup.find_all(attrs={"id": re.compile(pattern, re.I)}):
                    elem.decompose()
            
            # Try multiple content selectors
            content = None
            selectors = [
                "article", "main",
                {"class": re.compile(r".*article.*body.*", re.I)},
                {"class": re.compile(r".*content.*", re.I)},
                {"class": re.compile(r".*story.*", re.I)},
                {"id": re.compile(r".*content.*", re.I)},
                {"itemprop": "articleBody"}
            ]
            
            for selector in selectors:
                if isinstance(selector, str):
                    content = soup.find(selector)
                else:
                    content = soup.find(attrs=selector)
                if content:
                    break
            
            if not content:
                content = soup.body
            
            text = content.get_text(separator="\n", strip=True) if content else ""
            
            # Clean text based on expected language
            if expected_lang == 'arabic':
                text = self._clean_arabic_text(text)
            else:
                text = self._clean_english_text(text)
            
            if len(text) < self.min_content_length:
                logging.warning(f"Fallback content too short ({len(text)} chars): {url}")
            
            return {
                "content": text,
                "content_length": len(text),
                "authors": "",
                "publish_date": None,
                "top_image": "",
                "keywords": "",
                "status": "success_fallback",
                "extraction_method": "beautifulsoup"
            }
            
        except Exception as e:
            logging.error(f"Fallback scraping failed for {url}: {e}")
            return {
                "content": "",
                "content_length": 0,
                "authors": "",
                "publish_date": None,
                "top_image": "",
                "keywords": "",
                "status": f"error: {str(e)}",
                "extraction_method": "failed"
            }
    
    def _clean_arabic_text(self, text):
        """Clean and normalize Arabic text"""
        if not text:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove common Arabic noise patterns
        noise_patterns = [
            r'اقرأ أيضا.*?(?=\n|$)',
            r'للمزيد.*?(?=\n|$)',
            r'تابعنا.*?(?=\n|$)',
            r'شاهد أيضا.*?(?=\n|$)',
            r'المصدر.*?(?=\n|$)'
        ]
        for pattern in noise_patterns:
            text = re.sub(pattern, '', text)
        
        # Remove repeated diacritics
        text = re.sub(r'[\u064B-\u065F]+', '', text)
        
        return text.strip()
    
    def _clean_english_text(self, text):
        """Clean and normalize English text"""
        if not text:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove common English noise patterns
        noise_patterns = [
            r'Read more:.*?(?=\n|$)',
            r'Read also:.*?(?=\n|$)',
            r'Follow us.*?(?=\n|$)',
            r'Subscribe.*?(?=\n|$)',
            r'Advertisement.*?(?=\n|$)',
            r'\[.*?\]',  # Remove bracketed content like [Photo]
            r'Click here.*?(?=\n|$)'
        ]
        for pattern in noise_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        
        # Remove URLs
        text = re.sub(r'http[s]?://\S+', '', text)
        
        # Remove email addresses
        text = re.sub(r'\S+@\S+', '', text)
        
        return text.strip()
    
    def detect_language(self, text):
        """Detect language with confidence scores for both Arabic and English"""
        if not text or len(text.strip()) < 50:
            return "unknown", 0.0, 0.0
        
        try:
            detected_lang = detect(text)
            
            # Calculate Arabic character ratio
            arabic_chars = len(re.findall(r'[\u0600-\u06FF]', text))
            # Calculate English character ratio (letters only)
            english_chars = len(re.findall(r'[a-zA-Z]', text))
            total_chars = len(re.sub(r'\s', '', text))
            
            arabic_ratio = arabic_chars / total_chars if total_chars > 0 else 0
            english_ratio = english_chars / total_chars if total_chars > 0 else 0
            
            return detected_lang, round(arabic_ratio, 3), round(english_ratio, 3)
        except Exception as e:
            logging.warning(f"Language detection failed: {e}")
            return "unknown", 0.0, 0.0
    
    def _determine_content_language(self, arabic_ratio, english_ratio):
        """Determine primary content language based on character ratios"""
        if arabic_ratio > 0.5:
            return "arabic"
        elif english_ratio > 0.5:
            return "english"
        elif arabic_ratio > english_ratio:
            return "mixed_arabic"
        elif english_ratio > arabic_ratio:
            return "mixed_english"
        else:
            return "unknown"
    
    def scrape_all(self, df):
        """Scrape content for all articles with progress tracking"""
        if df.empty:
            logging.warning("No articles to scrape")
            return df
        
        results = []
        logging.info(f"Scraping {len(df)} articles...")
        
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Scraping"):
            url = row.get("url", "")
            expected_lang = row.get("source_language", None)
            
            if not url:
                results.append({
                    "content": "", "content_length": 0, "authors": "",
                    "publish_date": None, "top_image": "", "keywords": "",
                    "status": "no_url", "extraction_method": "none",
                    "detected_language": "unknown", "arabic_ratio": 0.0,
                    "english_ratio": 0.0, "content_language": "unknown"
                })
                continue
            
            result = self.scrape_article(url, expected_lang)
            
            # Detect language
            detected_lang, arabic_ratio, english_ratio = self.detect_language(result["content"])
            result["detected_language"] = detected_lang
            result["arabic_ratio"] = arabic_ratio
            result["english_ratio"] = english_ratio
            result["content_language"] = self._determine_content_language(arabic_ratio, english_ratio)
            
            # Flag for sentiment analysis suitability
            result["suitable_for_sentiment"] = (
                len(result["content"]) >= self.min_content_length and
                result["status"].startswith("success") and
                result["content_language"] in ["arabic", "english"]
            )
            
            results.append(result)
            time.sleep(self.delay)
        
        # Add scraped columns to dataframe
        for col in ["content", "content_length", "authors", "publish_date", 
                   "top_image", "keywords", "status", "extraction_method", 
                   "detected_language", "arabic_ratio", "english_ratio",
                   "content_language", "suitable_for_sentiment"]:
            df[col] = [r[col] for r in results]
        
        logging.info("Scraping complete")
        return df
    
    def save(self, df, base_name):
        """Save with timestamp and statistics"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file = f"{base_name}_{timestamp}.csv"
        json_file = f"{base_name}_{timestamp}.json"
        
        df.to_csv(csv_file, index=False, encoding="utf-8-sig")
        df.to_json(json_file, orient="records", force_ascii=False, indent=2)
        
        # Save statistics
        self._print_statistics(df)
        logging.info(f"Saved data to {csv_file} and {json_file}")
        
        return csv_file, json_file
    
    def _print_statistics(self, df):
        """Print detailed statistics for bilingual content analysis"""
        print("\n" + "="*70)
        print("EXTRACTION STATISTICS")
        print("="*70)
        
        total = len(df)
        successful = df["status"].str.contains("success").sum() if "status" in df else 0
        
        print(f"Total articles: {total}")
        print(f"Successfully scraped: {successful} ({successful/total*100:.1f}%)")
        
        if "content_language" in df:
            print("\nContent Language Distribution:")
            lang_counts = df["content_language"].value_counts()
            for lang, count in lang_counts.items():
                print(f"  {lang.title()}: {count} ({count/total*100:.1f}%)")
        
        if "source_language" in df:
            print("\nSource Language Distribution:")
            source_counts = df["source_language"].value_counts()
            for source, count in source_counts.items():
                print(f"  {source.title()}: {count} ({count/total*100:.1f}%)")
        
        if "arabic_ratio" in df and "english_ratio" in df:
            avg_arabic = df["arabic_ratio"].mean()
            avg_english = df["english_ratio"].mean()
            print(f"\nAverage Arabic character ratio: {avg_arabic:.2%}")
            print(f"Average English character ratio: {avg_english:.2%}")
        
        if "suitable_for_sentiment" in df:
            suitable = df["suitable_for_sentiment"].sum()
            print(f"\nSuitable for sentiment analysis: {suitable} ({suitable/total*100:.1f}%)")
            
            # Breakdown by language
            if "content_language" in df:
                suitable_df = df[df["suitable_for_sentiment"]]
                print("\n  By language:")
                for lang in ["arabic", "english"]:
                    lang_suitable = (suitable_df["content_language"] == lang).sum()
                    if lang_suitable > 0:
                        print(f"    {lang.title()}: {lang_suitable}")
        
        if "content_length" in df:
            valid_lengths = df[df["content_length"] > 0]["content_length"]
            if len(valid_lengths) > 0:
                print(f"\nContent Statistics:")
                print(f"  Average length: {valid_lengths.mean():.0f} characters")
                print(f"  Median length: {valid_lengths.median():.0f} characters")
                print(f"  Max length: {valid_lengths.max():.0f} characters")
        
        print("="*70)


if __name__ == "__main__":
    print("="*70)
    print("   BILINGUAL NEWS EXTRACTOR - Sentiment Analysis Pipeline")
    print("   Supports Arabic (Egyptian) & English News Sources")
    print("="*70)
    
    # Default queries
    default_queries = {
        'both': ["egypt news", "مصر أخبار egypt"],
        'arabic': ["مصر أخبار", "egypt news arabic"],
        'english': ["egypt news english", "egypt politics economy"]
    }
    
    print("\n📋 Language Options:")
    print("  1. Both (Arabic & English)")
    print("  2. Arabic only")
    print("  3. English only")
    print("  4. No filter (mixed)")
    
    lang_choice = input("\nSelect language option (1-4, default: 1): ").strip()
    
    language_map = {'1': 'both', '2': 'arabic', '3': 'english', '4': None}
    language_filter = language_map.get(lang_choice, 'both')
    
    # Show suggested queries
    if language_filter in default_queries:
        print(f"\n📝 Suggested queries for {language_filter or 'mixed'}:")
        for i, q in enumerate(default_queries[language_filter], 1):
            print(f"  {i}. {q}")
        default_query = default_queries[language_filter][0]
    else:
        default_query = "egypt news"
    
    query = input(f"\nEnter search query (default: '{default_query}'): ").strip()
    query = query or default_query
    
    max_records = input("Enter number of articles (default: 100): ").strip()
    max_records = int(max_records) if max_records.isdigit() else 100
    
    # Initialize extractor
    extractor = BilingualNewsExtractor(
        query=query,
        maxrecords=max_records,
        delay=1.5,
        min_content_length=200,
        language_filter=language_filter
    )
    
    # Extract metadata
    print(f"\n🔍 Fetching articles from GDELT...")
    df = extractor.extract_gdelt()
    
    if df.empty:
        print("❌ No articles found. Try a different query or language filter.")
        exit()
    
    print(f"\n📰 Found {len(df)} articles")
    
    if "source_language" in df:
        print("\n📊 Source Distribution:")
        print(df["source_language"].value_counts())
    
    if "domain" in df:
        print("\n🌐 Top Sources:")
        print(df["domain"].value_counts().head(5))
    
    print("\n📄 Sample titles:")
    print(df[["title"]].head(3))
    
    proceed = input("\nProceed to scrape full articles? (y/n): ").lower().strip()
    if proceed != "y":
        base_name = f"bilingual_news_{query.replace(' ', '_')}_metadata"
        extractor.save(df, base_name)
        print("✅ Metadata saved. Exiting.")
        exit()
    
    # Scrape full content
    print("\n⚙️ Scraping articles...")
    df_full = extractor.scrape_all(df)
    
    # Save results
    base_name = f"bilingual_news_{query.replace(' ', '_')}_full"
    csv_file, json_file = extractor.save(df_full, base_name)
    
    print(f"\n✅ Data saved successfully!")
    print(f"📁 CSV: {csv_file}")
    print(f"📁 JSON: {json_file}")
    
    # Show sample
    if "suitable_for_sentiment" in df_full:
        suitable_df = df_full[df_full["suitable_for_sentiment"]]
        if not suitable_df.empty:
            print(f"\n📊 Sample articles ready for sentiment analysis:")
            display_cols = ["title", "content_language", "arabic_ratio", 
                          "english_ratio", "content_length"]
            available_cols = [col for col in display_cols if col in suitable_df.columns]
            print(suitable_df[available_cols].head(3))
