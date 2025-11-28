"""
Analytics page - advanced visualizations and insights
"""
import streamlit as st
import pandas as pd
import html
from typing import Dict, Any, Optional, Tuple
from datetime import datetime

from utils.text_utils import get_top_bigrams
from ui.charts import (
    create_volume_sentiment_timeline,
    create_sentiment_pie_chart,
    create_top_sources_bar,
    create_category_pie_chart,
    create_sentiment_heatmap,
    create_hourly_sentiment_chart,
    create_category_trend_chart,
    create_bigrams_bar_chart,
    create_weekday_volume_chart,
    create_weekend_pie_chart
)


# Constants
SENTIMENT_COLORS = {
    'positive': '#10B981',
    'neutral': '#9CA3AF',
    'negative': '#EF4444'
}

SENTIMENT_LABELS = {
    'positive': ('😊 Most Positive Day', 'positive articles'),
    'negative': ('😞 Most Negative Day', 'negative articles')
}


def render_analytics(viz_df: pd.DataFrame, metrics: Dict[str, Any]) -> None:
    """
    Render analytics dashboard with all visualizations.
    
    Args:
        viz_df: DataFrame containing visualization data
        metrics: Dictionary containing pre-calculated metrics
    """
    st.markdown("### 📊 Analytics Dashboard")
    
    # Data validation and preprocessing
    viz_df = _preprocess_dataframe(viz_df)
    
    if viz_df.empty:
        st.info("📊 No data available for analysis.")
        return
    
    # Render dashboard sections
    _render_all_sections(viz_df, metrics)


def _preprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess DataFrame by removing unnecessary columns and validating data.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Cleaned DataFrame
    """
    if df.empty:
        return df
    
    # Remove embedding column to prevent UI lag
    columns_to_drop = ['embedding']
    existing_columns = [col for col in columns_to_drop if col in df.columns]
    
    if existing_columns:
        df = df.drop(columns=existing_columns)
    
    return df


def _render_all_sections(viz_df: pd.DataFrame, metrics: Dict[str, Any]) -> None:
    """Render all dashboard sections in order."""
    render_sentiment_section(viz_df, metrics)
    st.markdown("<br>", unsafe_allow_html=True)
    
    render_timeseries_section(viz_df)
    st.divider()
    
    render_deep_sentiment_section(viz_df)
    st.divider()
    
    render_category_trends_section(viz_df)
    st.divider()
    
    render_distribution_section(viz_df)
    st.divider()
    
    render_content_intelligence_section(viz_df)
    st.divider()
    
    render_temporal_section(viz_df)


def render_sentiment_section(viz_df: pd.DataFrame, metrics: Dict[str, Any]) -> None:
    """Render sentiment intelligence metrics with key statistics."""
    st.markdown("#### 🎭 Sentiment Intelligence")
    
    s1, s2, s3, s4 = st.columns(4)
    
    # Render sentiment metrics
    _render_sentiment_metric(s1, metrics['positive'], "Positive", 
                            f"{metrics['pos_rate']:.1f}%", SENTIMENT_COLORS['positive'])
    _render_sentiment_metric(s2, metrics['neutral'], "Neutral", 
                            f"{metrics['neu_rate']:.1f}%", SENTIMENT_COLORS['neutral'])
    _render_sentiment_metric(s3, metrics['negative'], "Negative", 
                            f"{metrics['neg_rate']:.1f}%", SENTIMENT_COLORS['negative'])
    
    # Calculate and render average source score
    avg_src_score = _calculate_avg_source_score(viz_df)
    score_color = SENTIMENT_COLORS['positive'] if avg_src_score > 0 else SENTIMENT_COLORS['negative']
    _render_sentiment_metric(s4, f"{avg_src_score:+.1f}", "Avg Source Score", 
                            "Mean Index", score_color)


def _render_sentiment_metric(column, value: Any, label: str, 
                             subtitle: str, color: str) -> None:
    """Render a single sentiment metric card."""
    html_content = f"""
    <div class="metric-card" style="padding: 1rem;">
        <div style="font-size: 1.8rem; font-weight:800; color:{color}">{value}</div>
        <div style="font-size:0.8rem; color:#9CA3AF;">{label}</div>
        <div style="font-size:0.7rem; color:#6B7280;">{subtitle}</div>
    </div>
    """
    column.markdown(html_content, unsafe_allow_html=True)


def _calculate_avg_source_score(df: pd.DataFrame) -> float:
    """
    Calculate average sentiment score across all sources.
    
    Args:
        df: DataFrame with sentiment data
        
    Returns:
        Average sentiment score as percentage
    """
    try:
        if df.empty or 'source' not in df.columns or 'sentiment_lower' not in df.columns:
            return 0.0
        
        def calculate_source_score(group):
            if len(group) == 0:
                return 0
            pos_count = (group['sentiment_lower'] == 'positive').sum()
            neg_count = (group['sentiment_lower'] == 'negative').sum()
            return (pos_count - neg_count) / len(group) * 100
        
        scores = df.groupby('source').apply(calculate_source_score)
        return scores.mean() if not scores.empty else 0.0
    except Exception:
        return 0.0


def render_timeseries_section(viz_df: pd.DataFrame) -> None:
    """Render time series charts for volume and sentiment."""
    c1, c2 = st.columns([2, 1])
    
    with c1:
        st.markdown("**📈 Volume & Sentiment Timeline**")
        fig = create_volume_sentiment_timeline(viz_df)
        st.plotly_chart(fig, use_container_width=True)
    
    with c2:
        st.markdown("**🎭 Sentiment Share**")
        fig = create_sentiment_pie_chart(viz_df)
        st.plotly_chart(fig, use_container_width=True)


def render_deep_sentiment_section(viz_df: pd.DataFrame) -> None:
    """Render deep sentiment analysis including heatmaps and hourly patterns."""
    st.markdown("#### 🧠 Deep Sentiment Analysis")
    
    ds_c1, ds_c2 = st.columns(2)
    
    with ds_c1:
        st.markdown("**🌡️ Sentiment Heatmap (Category vs. Sentiment)**")
        fig = create_sentiment_heatmap(viz_df)
        st.plotly_chart(fig, use_container_width=True)
    
    with ds_c2:
        st.markdown("**⏰ Hourly Sentiment Rhythm**")
        fig = create_hourly_sentiment_chart(viz_df)
        st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    render_sentiment_extremes(viz_df)


def render_sentiment_extremes(viz_df: pd.DataFrame) -> None:
    """Render sentiment extremes showing most positive and negative days."""
    st.markdown("#### ⚡ Sentiment Extremes")
    
    most_pos_date, most_pos_val = _find_extreme_sentiment_day(viz_df, 'positive')
    most_neg_date, most_neg_val = _find_extreme_sentiment_day(viz_df, 'negative')
    
    se_c1, se_c2 = st.columns(2)
    
    with se_c1:
        _render_extreme_day_card(most_pos_date, most_pos_val, 'positive')
    
    with se_c2:
        _render_extreme_day_card(most_neg_date, most_neg_val, 'negative')


def _find_extreme_sentiment_day(df: pd.DataFrame, 
                                sentiment: str) -> Tuple[Optional[datetime], int]:
    """
    Find the day with the most articles of a given sentiment.
    
    Args:
        df: DataFrame with sentiment data
        sentiment: 'positive' or 'negative'
        
    Returns:
        Tuple of (date, count) or (None, 0) if not found
    """
    try:
        if df.empty or 'date' not in df.columns or 'sentiment_lower' not in df.columns:
            return None, 0
        
        daily_counts = df.groupby(['date', 'sentiment_lower']).size().unstack(fill_value=0)
        
        if sentiment not in daily_counts.columns:
            return None, 0
        
        max_date = daily_counts[sentiment].idxmax()
        max_val = daily_counts[sentiment].max()
        
        return max_date, int(max_val)
    except Exception:
        return None, 0


def _render_extreme_day_card(date: Optional[datetime], count: int, 
                             sentiment_type: str) -> None:
    """Render a card showing an extreme sentiment day."""
    color = SENTIMENT_COLORS[sentiment_type]
    label, count_label = SENTIMENT_LABELS[sentiment_type]
    date_str = date.strftime('%a, %d %b') if date else "N/A"
    
    html_content = f"""
    <div class="metric-card" style="border-left: 4px solid {color};">
        <div style="color: {color}; font-weight: 700; text-transform: uppercase; 
                    font-size: 0.85rem; margin-bottom: 0.5rem;">
            {label}
        </div>
        <div class="metric-value">{date_str}</div>
        <div style="color: #9CA3AF; font-size: 0.9rem;">{count} {count_label}</div>
    </div>
    """
    st.markdown(html_content, unsafe_allow_html=True)


def render_category_trends_section(viz_df: pd.DataFrame) -> None:
    """Render category evolution trends over time."""
    st.markdown("#### 📈 Category Trends Evolution")
    fig = create_category_trend_chart(viz_df, top_n=5)
    st.plotly_chart(fig, use_container_width=True)


def render_distribution_section(viz_df: pd.DataFrame) -> None:
    """Render source and category distribution charts."""
    c3, c4 = st.columns(2)
    
    with c3:
        st.markdown("**📰 Top Sources**")
        fig = create_top_sources_bar(viz_df, top_n=10)
        st.plotly_chart(fig, use_container_width=True)
    
    with c4:
        st.markdown("**🏷️ Top Categories**")
        fig = create_category_pie_chart(viz_df, top_n=10)
        st.plotly_chart(fig, use_container_width=True)


def render_content_intelligence_section(viz_df: pd.DataFrame) -> None:
    """Render content intelligence analysis including bigrams and highlights."""
    st.markdown("#### 📝 Content Intelligence (N-Grams)")
    
    ci_c1, ci_c2 = st.columns(2)
    
    with ci_c1:
        _render_bigrams_analysis(viz_df)
    
    with ci_c2:
        st.markdown("**📰 Sentiment Extremes Feed (Newest)**")
        render_sentiment_highlights(viz_df)


def _render_bigrams_analysis(df: pd.DataFrame) -> None:
    """Render bigram frequency analysis."""
    st.markdown("**🔠 Top Contextual Bigrams (2-Word Phrases)**")
    
    if 'title' not in df.columns or df.empty:
        st.info("Not enough data for bigram analysis.")
        return
    
    titles_list = df['title'].astype(str).tolist()
    df_bigrams = get_top_bigrams(titles_list, top_n=15)
    
    if not df_bigrams.empty:
        fig = create_bigrams_bar_chart(df_bigrams)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Not enough data for bigram analysis.")


def render_sentiment_highlights(viz_df: pd.DataFrame) -> None:
    """Render positive and negative article highlights sorted by recency."""
    col_pos, col_neg = st.columns(2)
    
    # Sort by publication date (newest first)
    sorted_df = _sort_by_date(viz_df)
    
    with col_pos:
        st.markdown("##### ✅ Positive Highlights")
        _render_article_highlights(sorted_df, 'positive', 5)
    
    with col_neg:
        st.markdown("##### ⚠️ Negative Highlights")
        _render_article_highlights(sorted_df, 'negative', 5)


def _sort_by_date(df: pd.DataFrame) -> pd.DataFrame:
    """Sort DataFrame by publication date (newest first)."""
    if 'published_dt' in df.columns:
        return df.sort_values('published_dt', ascending=False)
    return df


def _render_article_highlights(df: pd.DataFrame, sentiment: str, limit: int) -> None:
    """
    Render article highlights for a specific sentiment.
    
    Args:
        df: DataFrame containing articles
        sentiment: 'positive' or 'negative'
        limit: Maximum number of articles to show
    """
    if 'sentiment_lower' not in df.columns:
        st.caption(f"No {sentiment} articles found in current selection.")
        return
    
    articles = df[df['sentiment_lower'] == sentiment].head(limit)
    
    if articles.empty:
        st.caption(f"No {sentiment} articles found in current selection.")
        return
    
    color = SENTIMENT_COLORS[sentiment]
    
    for _, row in articles.iterrows():
        _render_article_card(row, color)


def _render_article_card(row: pd.Series, border_color: str) -> None:
    """Render a single article card with title, summary, source, and date."""
    # Extract and sanitize data
    title = html.escape(str(row.get('title', 'No Title'))[:90])
    url = html.escape(str(row.get('url', '#')))
    source = html.escape(str(row.get('source', 'Unknown')))
    summary = html.escape(str(row.get('summary', ''))[:140])
    summary_display = f"{summary}..." if summary else ""
    date_display = row.get('published_display', '')
    
    html_content = f"""
    <div style="padding: 12px; border-left: 3px solid {border_color}; 
                background: rgba({_hex_to_rgb(border_color)}, 0.05); 
                margin-bottom: 10px; border-radius: 6px; 
                border: 1px solid rgba({_hex_to_rgb(border_color)}, 0.15);">
        <div style="font-weight: 700; font-size: 0.95rem; margin-bottom: 4px; line-height: 1.3;">
            <a href="{url}" target="_blank" style="text-decoration: none; color: inherit;">{title}</a>
        </div>
        <div style="font-size: 0.85rem; color: var(--text-main); margin-bottom: 8px; 
                    opacity: 0.9; line-height: 1.4;">{summary_display}</div>
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <span style="font-size: 0.75rem; color: {border_color}; font-weight: 600; 
                        background: rgba({_hex_to_rgb(border_color)}, 0.1); 
                        padding: 2px 6px; border-radius: 4px;">{source}</span>
            <span style="font-size: 0.7rem; color: var(--text-muted); 
                        font-family: monospace; opacity: 0.8;">{date_display}</span>
        </div>
    </div>
    """
    st.markdown(html_content, unsafe_allow_html=True)


def _hex_to_rgb(hex_color: str) -> str:
    """
    Convert hex color to RGB string for rgba() usage.
    
    Args:
        hex_color: Color in hex format (e.g., '#10B981')
        
    Returns:
        RGB values as comma-separated string (e.g., '16, 185, 129')
    """
    hex_color = hex_color.lstrip('#')
    try:
        r, g, b = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
        return f"{r}, {g}, {b}"
    except (ValueError, IndexError):
        return "128, 128, 128"  # Default gray


def render_temporal_section(viz_df: pd.DataFrame) -> None:
    """Render temporal activity analysis including weekday patterns."""
    st.markdown("#### 📅 Temporal Activity")
    
    wd_c1, wd_c2 = st.columns(2)
    
    with wd_c1:
        st.markdown("**Publishing Volume by Weekday**")
        fig = create_weekday_volume_chart(viz_df)
        st.plotly_chart(fig, use_container_width=True)
    
    with wd_c2:
        st.markdown("**Weekend vs. Weekday Activity**")
        fig = create_weekend_pie_chart(viz_df)
        st.plotly_chart(fig, use_container_width=True)
