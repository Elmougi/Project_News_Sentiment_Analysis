"""
SQL query builders and data fetching
"""
import logging
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from database.connection import safe_read_sql, detect_table
from utils.date_utils import try_parse_datestr, unified_date_display
from config.settings import Config

logger = logging.getLogger("mokhber.queries")


def build_headlines_sql(table, cols):
    """Build SQL query for fetching headlines"""
    col_map = {}
    for c in ("id", "title", "summary", "source", "category", 
              "sentiment", "url", "language", "published_date", "scraped_at","image_url"):
        col_map[c] = c if c in cols else f"NULL AS {c}"
    
    order_col = "scraped_at" if "scraped_at" in cols else "id"
    
    sql = f"""
    SELECT {col_map['id']} AS id,
           {col_map['title']} AS title,
           {col_map['summary']} AS summary,
           {col_map['source']} AS source,
           {col_map['category']} AS category,
           {col_map['sentiment']} AS sentiment,
           {col_map['url']} AS url,
           {col_map['language']} AS language,
           {col_map['published_date']} AS published_date,
           {col_map['scraped_at']} AS scraped_at,
           {col_map['image_url']} AS image_url
    FROM {table}
    WHERE 1=1
    """
    return sql, order_col


@st.cache_data(ttl=Config.DATA_CACHE_TTL)
def fetch_headlines(table, cols, start_days=30, limit=2000, search=None):
    """Fetch and normalize headlines from database"""
    base_sql, order_col = build_headlines_sql(table, cols)
    params = {}
    
    
    where_clause = ""
    date_col = None
    
    
    if "published_date" in cols:
        date_col = "published_date"
    elif "scraped_at" in cols:
        date_col = "scraped_at"
        
    # Apply date filter 
    if date_col:
        # Calculate cutoff date
        start_dt = (datetime.now(tz=Config.CAIRO_TZ) - timedelta(days=start_days)).isoformat()
        where_clause = f" AND {date_col} >= :start_dt"
        params["start_dt"] = start_dt
    
    # Add search filter
    search_clause = ""
    if search and ("title" in cols or "summary" in cols):
        params["search"] = f"%{search}%"
        if "title" in cols and "summary" in cols:
            search_clause = " AND (title ILIKE :search OR summary ILIKE :search)"
        elif "title" in cols:
            search_clause = " AND title ILIKE :search"
        elif "summary" in cols:
            search_clause = " AND summary ILIKE :search"
    
    # Build final query
    final_sql = base_sql + where_clause + search_clause + f" ORDER BY {order_col} DESC LIMIT :limit"
    params["limit"] = int(limit)
    
    df = safe_read_sql(final_sql, params=params)
    
    if df.empty:
        return df
    
    # Normalize data
    df = normalize_dataframe(df)
    
    return df


def normalize_dataframe(df):
    """Normalize and clean DataFrame columns"""
    # Clean text fields
    df['title'] = df['title'].astype(str).fillna("").replace("nan", "")
    df['summary'] = df['summary'].astype(str).fillna("").replace("nan", "")
    df['source'] = df['source'].astype(str).fillna("Unknown")
    df['category'] = df['category'].astype(str).fillna("General")
    df['sentiment'] = df['sentiment'].astype(str).fillna("neutral")
    df['image_url'] = df['image_url'].astype(str).fillna("").replace("nan", "")
    
    # Parse dates
    def parse_row_pub(r):
        pub = r.get('published_date')
        scraped = r.get('scraped_at')
        dt_pub = try_parse_datestr(pub) if pub else None
        dt_scraped = try_parse_datestr(scraped) if scraped else None
        return dt_pub if dt_pub is not None else dt_scraped
    
    df['published_dt'] = df.apply(parse_row_pub, axis=1)
    df['published_display'] = df.apply(
        lambda r: unified_date_display(
            r.get('published_date') or "", 
            fallback=r.get('published_dt')
        ), 
        axis=1
    )
    
    return df


def apply_filters(df, filters):
    """Apply filters to DataFrame"""
    if df.empty or not filters:
        return df
    
    out = df.copy()
    
    if filters.get('sources'):
        out = out[out['source'].isin(filters['sources'])]
    
    if filters.get('categories'):
        out = out[out['category'].isin(filters['categories'])]
    
    if filters.get('languages'):
        if 'language' in out.columns:
            out = out[out['language'].isin(filters['languages'])]
    
    if filters.get('sentiments'):
        out = out[out['sentiment'].str.lower().isin([s.lower() for s in filters['sentiments']])]
    
    return out
