"""
Reddit Scraper + Bilingual Query + Sentiment-ready Output
- Focus on Egyptian and Arabic subreddits + global fallback
- Bilingual query (English + Arabic) via Ollama (optional) or local translation
- Normalized text + English versions for sentiment analysis
- Optional comments fetch; CSV of English-only comments

Install (PowerShell):
  pip install praw pandas langdetect googletrans==4.0.0rc1 requests

Run:
  python twitter_scraper_mistral.py
"""

import os
import re
import sys
import time
import logging
from datetime import datetime
from typing import List, Tuple, Dict, Any

import pandas as pd
from langdetect import detect, DetectorFactory
import requests

# Your Reddit credentials (hardcoded as requested)
REDDIT_CLIENT_ID = "JD0hBAqdiswSdoKOZzx_oQ"
REDDIT_CLIENT_SECRET = "08cYP9jZAgy16FIodLVNTt2yvFXKKA"
REDDIT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"

# Deterministic language detection
DetectorFactory.seed = 42

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("reddit-scraper")

# Optional imports
def _try_import_googletrans():
    try:
        from googletrans import Translator  # type: ignore
        return Translator
    except Exception:
        return None

def _try_import_praw():
    try:
        import praw
        return praw
    except Exception:
        return None

# Ollama environment
def check_environment() -> bool:
    try:
        r = requests.get("http://localhost:11434/api/tags", timeout=3)
        return r.status_code == 200
    except Exception:
        return False

# Regex helpers
URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
EMOJI_RE = re.compile(
    "[\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF]+",
    flags=re.UNICODE,
)
AR_DIACRITICS_RE = re.compile(r"[\u0617-\u061A\u064B-\u0652\u0657-\u065F\u0670\u06D6-\u06ED]")
AR_TATWEEL_RE = re.compile(r"\u0640")
MULTISPACE_RE = re.compile(r"\s+")
AR_CHARS_RE = re.compile(r"[\u0600-\u06FF]")

def normalize_arabic(text: str) -> str:
    if not text:
        return text
    text = AR_DIACRITICS_RE.sub("", text)
    text = AR_TATWEEL_RE.sub("", text)
    text = re.sub("[إأآا]", "ا", text)
    text = re.sub("ى", "ي", text)
    text = re.sub("ؤ", "و", text)
    text = re.sub("ئ", "ي", text)
    text = re.sub("ة", "ه", text)
    return text

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = URL_RE.sub(" ", text)
    text = EMOJI_RE.sub(" ", text)
    text = text.replace("#", " ").replace("@", " ")
    text = text.translate(str.maketrans("", "", "\r\t"))
    text = MULTISPACE_RE.sub(" ", text).strip()
    return text

def detect_lang_safe(text: str) -> str:
    try:
        return detect(text) if text and text.strip() else "unknown"
    except Exception:
        return "unknown"

# Lightweight translation cache
_TRANS_CACHE: Dict[str, str] = {}

def translate_to_en(text: str, translator) -> str:
    if not text:
        return ""
    try:
        if translator:
            if text in _TRANS_CACHE:
                return _TRANS_CACHE[text]
            res = translator.translate(text, src="auto", dest="en")
            out = res.text
            if len(_TRANS_CACHE) > 2000:
                _TRANS_CACHE.clear()
            _TRANS_CACHE[text] = out
            return out
    except Exception:
        pass
    return text

# Query generation
def generate_query_and_keywords(topic: str, use_ollama: bool) -> Tuple[str, List[str]]:
    topic = topic.strip()
    if not topic:
        return "Egypt مصر", ["egypt", "مصر"]

    if use_ollama:
        try:
            prompt = f"""
For the topic "{topic}", produce:
QUERY: 2-4 search terms including English AND Arabic (no punctuation).
KEYWORDS: 6-12 related terms (mix English and Arabic) separated by commas.

Examples:
QUERY: Egypt football مصر كرة
KEYWORDS: egypt, football, soccer, ahly, zamalek, مصر, كرة, الأهلي, الزمالك
"""
            r = requests.post(
                "http://localhost:11434/api/chat",
                json={"model": "mistral", "messages": [{"role": "user", "content": prompt}], "stream": False},
                timeout=15,
            )
            r.raise_for_status()
            content = r.json()["message"]["content"]
            q_match = re.search(r"QUERY:\s*(.+)", content, re.IGNORECASE)
            k_match = re.search(r"KEYWORDS:\s*(.+)", content, re.IGNORECASE)
            query = q_match.group(1).strip() if q_match else topic
            keywords = (
                [k.strip().lower() for k in re.split(r"[,،]", k_match.group(1)) if k.strip()]
                if k_match
                else [w.lower() for w in topic.split() if len(w) > 2]
            )
        except Exception as e:
            log.warning(f"Ollama query generation failed: {e}")
            query, keywords = _fallback_query(topic)
    else:
        query, keywords = _fallback_query(topic)

    q_lower = query.lower()
    if "egypt" in q_lower and "مصر" not in query:
        query += " مصر"
    if "cairo" in q_lower and "القاهرة" not in query:
        query += " القاهرة"

    tokens = query.split()
    if len(tokens) > 4:
        query = " ".join(tokens[:4])

    seen = set()
    deduped = []
    for k in keywords:
        if k not in seen:
            seen.add(k)
            deduped.append(k)
    keywords = deduped[:15]

    log.info(f"Query: {query}")
    log.info(f"Keywords: {', '.join(keywords)}")
    return query, keywords

def _fallback_query(topic: str) -> Tuple[str, List[str]]:
    Translator = _try_import_googletrans()
    ar_tokens: List[str] = []
    if Translator:
        try:
            t = Translator()
            ar_topic = t.translate(topic, src="auto", dest="ar").text
            ar_tokens = [w for w in ar_topic.split()[:3] if w]
        except Exception:
            ar_tokens = []
    base_tokens = topic.split()[:3]
    query = " ".join(base_tokens + ar_tokens[:2]).strip() or "Egypt مصر"
    base_keywords = [w.lower() for w in topic.split() if len(w) > 2]
    if ar_tokens:
        base_keywords += ar_tokens
    if "egypt" not in [w.lower() for w in base_tokens]:
        base_keywords += ["egypt", "مصر"]
    return query, base_keywords

# Relevance backup (not used in search-only mode, kept for future use)
def is_relevant(title: str, text: str, keywords: List[str], min_matches: int = 1) -> bool:
    title_l = (title or "").lower()
    text_l = (text or "").lower()
    matches = sum(1 for k in keywords if k in title_l) + sum(1 for k in keywords if k in text_l)
    return matches >= min_matches

# Reddit client
def create_reddit_client():
    praw = _try_import_praw()
    if not praw:
        log.error("praw not installed. Run: pip install praw")
        sys.exit(1)
    # Use provided credentials directly
    client_id = REDDIT_CLIENT_ID.strip()
    client_secret = REDDIT_CLIENT_SECRET.strip()
    user_agent = REDDIT_USER_AGENT.strip() or "reddit-scraper/1.0"
    if not client_id or not client_secret:
        log.error("Missing Reddit credentials.")
        sys.exit(1)
    reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)
    try:
        reddit.read_only = True
    except Exception:
        pass
    return reddit

# Subreddits
EGYPTIAN_SUBS = ["egypt", "Cairo", "ExEgypt", "AskMiddleEast", "AlexandriaEgy"]
ARABIC_SUBS = ["arabs", "Arabs", "MiddleEastNews", "ArabWorld", "arabic"]

# Scrape
def scrape_reddit_multi_source(
    reddit,
    query: str,
    keywords: List[str],
    limit: int = 100,
    output_file: str = "reddit_posts.csv",
) -> pd.DataFrame:
    posts: List[Dict[str, Any]] = []
    seen_ids: set = set()
    fetched = 0

    phases = [
        ("Egyptian Sub", EGYPTIAN_SUBS, int(limit * 0.5), ["year", "all"]),
        ("Arabic Sub", ARABIC_SUBS, int(limit * 0.3), ["year", "all"]),
        ("Global", ["all"], limit, ["month", "year", "all"]),
    ]

    for source, subs, phase_cap, time_filters in phases:
        start_count = fetched
        for sub in subs:
            if fetched >= start_count + phase_cap:
                break
            log.info(f"[{source}] Searching r/{sub} | query='{query}'")
            subreddit = reddit.subreddit(sub)
            per_sub_found = 0
            for tf in time_filters:
                if fetched >= start_count + phase_cap:
                    break
                try:
                    per_iter = min(50, (start_count + phase_cap) - fetched)
                    for s in subreddit.search(query, limit=per_iter, sort="relevance", time_filter=tf):
                        if s.id in seen_ids:
                            continue
                        seen_ids.add(s.id)

                        title = s.title or ""
                        body = s.selftext or ""
                        created_dt = datetime.utcfromtimestamp(getattr(s, "created_utc", time.time()))
                        url = getattr(s, "url", "")
                        author = str(getattr(s, "author", "")) if getattr(s, "author", None) else ""
                        over_18 = bool(getattr(s, "over_18", False))
                        upvote_ratio = getattr(s, "upvote_ratio", None)
                        score = getattr(s, "score", 0)
                        num_comments = getattr(s, "num_comments", 0)
                        sub_name = s.subreddit.display_name if hasattr(s, "subreddit") else sub

                        title_clean = clean_text(title)
                        body_clean = clean_text(body)
                        lang_title = detect_lang_safe(title_clean)
                        lang_body = detect_lang_safe(body_clean)
                        combined = (title_clean + " " + body_clean).strip()
                        lang_combined = detect_lang_safe(combined)

                        posts.append(
                            {
                                "id": s.id,
                                "Source": source,
                                "Subreddit": sub_name,
                                "Title": title,
                                "Title_clean": title_clean,
                                "Selftext": body,
                                "Selftext_clean": body_clean,
                                "Combined_clean": combined,
                                "Combined_lang": lang_combined,
                                "Title_lang": lang_title,
                                "Selftext_lang": lang_body,
                                "Author": author,
                                "URL": url,
                                "Created_utc": int(getattr(s, "created_utc", time.time())),
                                "Created": created_dt.strftime("%Y-%m-%d %H:%M:%S"),
                                "Score": score,
                                "Num_Comments": num_comments,
                                "Is_NSFW": over_18,
                                "Upvote_Ratio": upvote_ratio,
                                "Relevance": f"Search:{tf}",
                            }
                        )
                        fetched += 1
                        per_sub_found += 1
                        if fetched >= start_count + phase_cap:
                            break
                except Exception as e:
                    log.warning(f"Search error {sub}:{tf}: {str(e)[:90]}")
                    continue
            log.info(f"Found {per_sub_found} from r/{sub}")
            time.sleep(0.6)
        log.info(f"[{source}] Collected: {fetched - start_count}")

    if not posts:
        log.error("No posts found for the topic.")
        return pd.DataFrame()

    df = pd.DataFrame(posts)

    # Sentiment-ready columns
    df["Combined_norm_ar"] = df["Combined_clean"].apply(
        lambda t: normalize_arabic(t) if AR_CHARS_RE.search(t or "") else t
    )

    Translator = _try_import_googletrans()
    translator = Translator() if Translator else None

    def to_english_row(row):
        txt = row.get("Combined_clean", "") or ""
        lang = row.get("Combined_lang", "unknown")
        if lang == "ar" or AR_CHARS_RE.search(txt):
            return translate_to_en(txt, translator)
        elif lang == "unknown":
            lang2 = detect_lang_safe(txt)
            if lang2 == "ar" or AR_CHARS_RE.search(txt):
                return translate_to_en(txt, translator)
            return txt
        else:
            return txt

    log.info("Building English text column for sentiment...")
    df["text_en"] = df.apply(to_english_row, axis=1)
    df["text_en"] = df["text_en"].apply(lambda x: MULTISPACE_RE.sub(" ", x).strip())

    # Essential columns for ML
    sentiment_cols = [
        "id", "Source", "Subreddit",
        "Title_clean", "Selftext_clean", "Combined_clean",
        "Combined_norm_ar", "Combined_lang", "text_en",
        "Score", "Num_Comments", "Upvote_Ratio", "Is_NSFW",
        "Created", "URL", "Relevance",
    ]
    available_cols = [c for c in sentiment_cols if c in df.columns]
    df_out = df[available_cols].copy()

    # Save posts CSV
    df_out.to_csv(output_file, index=False, encoding="utf-8-sig")
    log.info(f"Saved posts: {output_file} ({len(df_out)} rows)")

    # Extra: lightweight features for sentiment pipelines
    def feat_len_words(t: str) -> int: return len((t or "").split())
    def feat_len_chars(t: str) -> int: return len(t or "")
    def count_exclaim(t: str) -> int: return (t or "").count("!")
    def count_question(t: str) -> int: return (t or "").count("?")
    def has_negation_en(t: str) -> int:
        t = (t or "").lower()
        return int(any(w in t for w in [" not ", "n't", " never ", " no "]))
    def has_negation_ar(t: str) -> int:
        t = f" {(t or '')} "
        return int(any(w in t for w in [" لا ", " لن ", " لم ", " ما "]))

    df_out["len_words"] = df_out["Combined_clean"].apply(feat_len_words)
    df_out["len_chars"] = df_out["Combined_clean"].apply(feat_len_chars)
    df_out["bangs"] = df_out["Combined_clean"].apply(count_exclaim)
    df_out["questions"] = df_out["Combined_clean"].apply(count_question)
    df_out["neg_en"] = df_out["text_en"].apply(has_negation_en)
    df_out["neg_ar"] = df_out["Combined_norm_ar"].apply(has_negation_ar)

    ml_csv = output_file.replace(".csv", "_ml_minimal.csv")
    ml_cols = ["id", "text_en", "Combined_lang", "len_words", "len_chars", "bangs", "questions", "neg_en", "neg_ar", "URL", "Created"]
    df_out[ml_cols].to_csv(ml_csv, index=False, encoding="utf-8-sig")
    log.info(f"Saved sentiment-ready minimal CSV: {ml_csv} ({len(df_out)} rows)")

    return df

# Comments (English-only)
def fetch_comments_english_only(
    reddit,
    post_ids: List[str],
    max_comments_per_post: int = 30,
    output_file: str = "reddit_comments_en.csv",
) -> pd.DataFrame:
    Translator = _try_import_googletrans()
    translator = Translator() if Translator else None

    all_comments: List[Dict[str, Any]] = []
    for pid in post_ids:
        try:
            submission = reddit.submission(id=pid)
            submission.comments.replace_more(limit=0)
            count = 0
            for c in submission.comments:
                if count >= max_comments_per_post:
                    break
                body_raw = c.body or ""
                body_clean = clean_text(body_raw)
                if not body_clean:
                    continue
                lang = detect_lang_safe(body_clean)
                if lang == "en":
                    en_text = body_clean
                else:
                    en_text = translate_to_en(body_clean, translator)
                    if en_text.strip() == body_clean.strip() and lang != "en":
                        continue
                all_comments.append(
                    {
                        "post_id": pid,
                        "comment_id": c.id,
                        "author": str(c.author) if c.author else "",
                        "created": datetime.utcfromtimestamp(getattr(c, "created_utc", time.time())).strftime("%Y-%m-%d %H:%M:%S"),
                        "score": getattr(c, "score", 0),
                        "comment_en": en_text,
                    }
                )
                count += 1
            log.info(f"Comments fetched for {pid}: {count}")
        except Exception as e:
            log.warning(f"Comments error for {pid}: {str(e)[:120]}")
        time.sleep(0.3)

    if not all_comments:
        log.warning("No English comments collected.")
        return pd.DataFrame()

    dfc = pd.DataFrame(all_comments)
    dfc.to_csv(output_file, index=False, encoding="utf-8-sig")
    log.info(f"Saved English comments: {output_file} ({len(dfc)} rows)")
    return dfc

# Main
def main():
    use_ollama = check_environment()
    if use_ollama:
        log.info("Ollama detected. Using for bilingual query if possible.")
    else:
        log.info("Ollama not detected. Using local fallback.")

    reddit = create_reddit_client()

    topic = input("Enter a topic (e.g., Egyptian economy, Cairo traffic, الأهلي): ").strip() or "Egypt news مصر اخبار"
    query, keywords = generate_query_and_keywords(topic, use_ollama)

    try:
        limit = int(input("Number of posts to fetch (default 120): ") or 120)
    except ValueError:
        limit = 120

    include_comments = (input("Fetch English-only comments? (y/N): ").strip().lower() == "y")
    try:
        max_comments_per_post = int(input("Max comments per post (default 30): ") or 30)
    except ValueError:
        max_comments_per_post = 30

    base_name = re.sub(r"[^-\w\u0600-\u06FF]+", "_", topic).strip("_")
    posts_csv = f"{base_name}_posts_sentiment_ready.csv"
    comments_csv = f"{base_name}_comments_en.csv"

    log.info("Starting scraping...")
    df_posts = scrape_reddit_multi_source(
        reddit=reddit,
        query=query,
        keywords=keywords,
        limit=limit,
        output_file=posts_csv,
    )

    if not df_posts.empty and include_comments:
        post_ids = pd.Series([p.get("id") for p in df_posts.to_dict("records")]).dropna().astype(str).tolist()
        fetch_comments_english_only(
            reddit=reddit,
            post_ids=post_ids,
            max_comments_per_post=max_comments_per_post,
            output_file=comments_csv,
        )

    log.info("Done.")

if __name__ == "__main__":

    main()
