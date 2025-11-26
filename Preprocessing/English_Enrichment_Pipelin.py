
"""
English Enrichment Pipeline (Fully Local - Ollama Mistral)
- BBC rewrite using local Mistral
- Sentiment using local Mistral
- Embeddings (BAAI/bge-large-en-v1.5) for dedupe only
- Embeddings REMOVED from final output JSON
- No GPT, no API keys, fully offline
"""

import os
import json
import time
import boto3
import traceback
import ollama
from datetime import datetime, timezone
from dotenv import load_dotenv
from tqdm import tqdm
from sentence_transformers import SentenceTransformer
import numpy as np

# ----------------------------------------------------------
# CONFIG
# ----------------------------------------------------------
load_dotenv()

AWS_BUCKET = os.getenv("AWS_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

if not AWS_BUCKET:
    raise RuntimeError("AWS_BUCKET_NAME required in .env")

S3_RAW_PREFIX = "raw/english/"
S3_OUT_PREFIX = "processed/english/"

# local embeddings
EMBED_MODEL = "BAAI/bge-large-en-v1.5"

# text limits
FULL_TEXT_TRIM_CHARS = 3000
BBC_SNIPPET_CHARS    = 900

# semantic dedupe
SIM_THRESHOLD = 0.88

# initialize clients
s3 = boto3.client("s3", region_name=AWS_REGION)

print("Loading embedding model:", EMBED_MODEL)
embedder = SentenceTransformer(EMBED_MODEL)


# ----------------------------------------------------------
# S3 HELPERS
# ----------------------------------------------------------

def list_scraper_subfolders(prefix):
    folders = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=AWS_BUCKET, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            folder = cp["Prefix"].replace(prefix, "").strip("/")
            folders.append(folder)

    return sorted(folders)


def list_s3_json(prefix):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=AWS_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if "seen" in key.lower():
                continue

            if key.endswith(".json"):
                keys.append({"Key": key, "LastModified": obj["LastModified"]})

    return keys


def pick_newest(prefix):
    keys = list_s3_json(prefix)
    if not keys:
        return None
    keys.sort(key=lambda x: x["LastModified"], reverse=True)
    return keys[0]["Key"]


def newest_per_scraper():
    folders = list_scraper_subfolders(S3_RAW_PREFIX)
    newest = {}
    for folder in folders:
        key = pick_newest(f"{S3_RAW_PREFIX}{folder}/")
        if key:
            newest[folder] = key
    return newest


def load_json(key):
    obj = s3.get_object(Bucket=AWS_BUCKET, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def save_json(data, key):
    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2),
        ContentType="application/json",
    )


# ----------------------------------------------------------
# TEXT HELPERS
# ----------------------------------------------------------

def trim_text(txt, max_chars=FULL_TEXT_TRIM_CHARS):
    if not txt:
        return txt
    if len(txt) <= max_chars:
        return txt

    half = max_chars // 2
    head = txt[:half]
    tail = txt[-half:]

    if "\n" in head:
        head = head.rsplit("\n", 1)[0]
    if "\n" in tail:
        tail = tail.split("\n", 1)[-1]

    return head + "\n\n... (trimmed) ...\n\n" + tail


# ----------------------------------------------------------
# NORMALIZATION
# ----------------------------------------------------------

def normalize_item(it):
    if not isinstance(it, dict):
        return None
    
    return {
        "source": it.get("source"),
        "language": it.get("language", "en"),
        "category": it.get("category"),
        "title": it.get("title"),
        "url": it.get("url"),
        "summary": it.get("summary"),
        "full_text": trim_text(it.get("full_text") or ""),
        "image_url": it.get("image_url"),
        "published_date": it.get("published_date"),
        "scraped_at": it.get("scraped_at"),

        # to be computed
        "sentiment": None,
        
        "embedding": None,  # removed before saving
    }


# ----------------------------------------------------------
# DEDUPE
# ----------------------------------------------------------

def dedupe_by_url(items):
    seen = set()
    out = []
    for it in items:
        url = it.get("url")
        if url and url not in seen:
            seen.add(url)
            out.append(it)
    return out


def embed_local(text):
    return embedder.encode(text or "", normalize_embeddings=True)


def cosine(a, b):
    a = np.asarray(a)
    b = np.asarray(b)
    na, nb = np.linalg.norm(a), np.linalg.norm(b)
    if na == 0 or nb == 0:
        return 0.0
    return float(np.dot(a, b) / (na * nb))


def semantic_dedupe(items):
    print("[semantic_dedupe] Embedding...")

    vecs = []
    for it in tqdm(items):
        base = (it["summary"] or "") or (it["full_text"] or "") or (it["title"] or "")
        vecs.append(embed_local(base))

    vecs = np.array(vecs)
    used = [False] * len(items)
    groups = []

    for i in range(len(items)):
        if used[i]:
            continue
        used[i] = True
        group = [i]

        for j in range(i + 1, len(items)):
            if used[j]:
                continue
            if cosine(vecs[i], vecs[j]) >= SIM_THRESHOLD:
                used[j] = True
                group.append(j)

        groups.append(group)

    priority = {
        "guardian": 3,
        "theguardian": 3,
        "bbc": 2,
        "nytimes": 1,
        "newyork_times": 1,
    }

    final = []
    for group in groups:
        best = group[0]
        best_score = -1

        for idx in group:
            src = (items[idx]["source"] or "").lower()
            score = priority.get(src, 0)
            if score > best_score:
                best = idx
                best_score = score

        final.append(items[best])

    return final


# ----------------------------------------------------------
# LOCAL OLLAMA (MISTRAL) FUNCTIONS
# ----------------------------------------------------------

def mistral_chat(prompt, max_tokens=200):
    resp = ollama.chat(
        model="mistral",
        messages=[
            {"role": "system", "content":
             "You are a safe, neutral assistant. Do not add facts or graphic detail."},
            {"role": "user", "content": prompt},
        ]
    )
    return resp["message"]["content"].strip()


def rewrite_bbc_local(snippet):
    prompt = (
        "Rewrite the following text into 1–2 factual BBC-style sentences. "
        "Be concise, neutral, and avoid graphic details.\n\n"
        f"TEXT:\n{snippet}\n\nREWRITE:"
    )
    out = mistral_chat(prompt, max_tokens=120)
    return out or snippet[:250]


def sentiment_local(text):
    prompt = (
        "Analyze sentiment. Return STRICT JSON ONLY:\n"
        "{\"sentiment\":\"positive|neutral|negative\",\"score\":0.0}\n\n"
        f"TEXT:\n{text}\n\nJSON:"
    )
    out = mistral_chat(prompt, max_tokens=100)

    try:
        j = json.loads(out)
        return j.get("sentiment", "neutral"), float(j.get("score", 0.0))
    except:
        return "neutral", 0.0


# ----------------------------------------------------------
# MAIN PIPELINE
# ----------------------------------------------------------

def run():
    print("=== English Enrichment (Local Mistral) ===")

    newest = newest_per_scraper()
    if not newest:
        print("No raw files found.")
        return

    print("Newest files:")
    for scr, key in newest.items():
        print(" ", scr, "->", key)

    raw_items = []
    for folder, key in newest.items():
        data = load_json(key)
        if isinstance(data, dict):
            raw_items.append(data)
        else:
            raw_items.extend([x for x in data if isinstance(x, dict)])

    print("Loaded raw:", len(raw_items))

    items = [normalize_item(x) for x in raw_items if normalize_item(x)]
    print("Normalized:", len(items))

    items = dedupe_by_url(items)
    print("Exact dedupe:", len(items))

    items = semantic_dedupe(items)
    print("Semantic dedupe:", len(items))

    # ------------------------------------------------------
    # BBC TASKS
    # ------------------------------------------------------
    print("Extracting BBC items...")

    bbc_tasks = []
    for idx, it in enumerate(items):
        if (it["source"] or "").lower() == "bbc":
            ft = it["full_text"] or ""
            snippet = ft[:BBC_SNIPPET_CHARS].strip() or (it["title"] or "")
            bbc_tasks.append((idx, snippet))

    print("BBC items:", len(bbc_tasks))

    # ------------------------------------------------------
    # BBC REWRITE
    # ------------------------------------------------------
    print("Rewriting BBC summaries...")

    for idx, snippet in tqdm(bbc_tasks):
        try:
            items[idx]["summary"] = rewrite_bbc_local(snippet)
        except:
            items[idx]["summary"] = snippet[:250]

    # ------------------------------------------------------
    # SENTIMENT
    # ------------------------------------------------------
    print("Sentiment analysis...")

    for it in tqdm(items, desc="Sentiment"):
        text = (it["title"] or "") + "\n" + (it["summary"] or "")
        sentiment, score = sentiment_local(text)
        it["sentiment"] = sentiment
        

    # ------------------------------------------------------
    # EMBEDDINGS (used internally ONLY)
    # ------------------------------------------------------
    print("Computing embeddings...")

    for it in tqdm(items, desc="Embeddings"):
        txt = (it["summary"] or "") + " " + (it["title"] or "")
        it["embedding"] = embed_local(txt).tolist()

    # ------------------------------------------------------
    # ADD TIMESTAMP
    # ------------------------------------------------------
    now = datetime.now(timezone.utc).isoformat()
    for it in items:
        it["enriched_at"] = now

    # ------------------------------------------------------
    # REMOVE EMBEDDINGS BEFORE SAVING
    # ------------------------------------------------------
    for it in items:
        if "embedding" in it:
            del it["embedding"]

    # ------------------------------------------------------
    # SAVE OUTPUT
    # ------------------------------------------------------
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_key = f"{S3_OUT_PREFIX}english_enriched_{ts}.json"
    save_json(items, out_key)

    print("Saved enriched file:", out_key)
    print("Completed. Total enriched:", len(items))


if __name__ == "__main__":
    run()
