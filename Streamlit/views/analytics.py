"""
Analytics page - advanced visualizations and insights
"""
import streamlit as st
import html
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


def render_analytics(df):
    st.title("📊 News Analytics Dashboard")

    # ---------------------------------------------------------
    # FIX 1: Remove 'embedding' column to prevent UI clutter
    # ---------------------------------------------------------
    if "embedding" in df.columns:
        df = df.drop(columns=["embedding"])

    if df.empty:
        st.warning("No data available.")
        return

    # Top Metrics
    total = len(df)
    pos = len(df[df['sentiment'] == 'Positive'])
    neg = len(df[df['sentiment'] == 'Negative'])
    neu = len(df[df['sentiment'] == 'Neutral'])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Articles", total)
    c2.metric("Positive", pos, delta=f"{pos/total:.1%}")
    c3.metric("Negative", neg, delta=f"-{neg/total:.1%}")
    c4.metric("Neutral", neu)

    st.divider()

    # ---------------------------------------------------------
    # Sentiment Share (Now Clean without Embeddings)
    # ---------------------------------------------------------
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Sentiment Trend")
        if "published_date" in df.columns:
            # Ensure date format
            df['published_date'] = pd.to_datetime(df['published_date'], errors='coerce')
            daily = df.groupby([df['published_date'].dt.date, 'sentiment']).size().reset_index(name='count')
            fig = px.bar(daily, x='published_date', y='count', color='sentiment', 
                         color_discrete_map={"Positive": "#00CC96", "Negative": "#EF553B", "Neutral": "#636EFA"})
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Sentiment Share")
        # Since 'embedding' is dropped, this chart (and its tooltip) will be clean
        counts = df['sentiment'].value_counts().reset_index()
        counts.columns = ['sentiment', 'count']
        fig_pie = px.pie(counts, names='sentiment', values='count', 
                         color='sentiment',
                         color_discrete_map={"Positive": "#00CC96", "Negative": "#EF553B", "Neutral": "#636EFA"})
        st.plotly_chart(fig_pie, use_container_width=True)

    st.divider()

    # ---------------------------------------------------------
    # FIX 2: Clickable Highlights (Positive & Negative)
    # ---------------------------------------------------------
    h1, h2 = st.columns(2)

    with h1:
        st.subheader("🌟 Positive Highlights")
        pos_df = df[df['sentiment'] == 'Positive'].head(5)
        for _, row in pos_df.iterrows():
            with st.container(border=True):
                # TITLE AS LINK
                st.markdown(f"#### [{row['title']}]({row['url']})")
                st.caption(f"{row['source']} • {row['published_date']}")
                st.write(row['summary'])

    with h2:
        st.subheader("⚠️ Negative Highlights")
        neg_df = df[df['sentiment'] == 'Negative'].head(5)
        for _, row in neg_df.iterrows():
            with st.container(border=True):
                # TITLE AS LINK
                st.markdown(f"#### [{row['title']}]({row['url']})")
                st.caption(f"{row['source']} • {row['published_date']}")
                st.write(row['summary'])


def render_sentiment_section(viz_df, metrics):
    """Render sentiment intelligence metrics"""
    st.markdown("#### 🎭 Sentiment Intelligence")
    
    s1, s2, s3, s4 = st.columns(4)
    
    def sent_metric(val, label, sub, color):
        return f"""
        <div class="metric-card" style="padding: 1rem;">
            <div style="font-size: 1.8rem; font-weight:800; color:{color}">{val}</div>
            <div style="font-size:0.8rem; color:#9CA3AF;">{label}</div>
            <div style="font-size:0.7rem; color:#6B7280;">{sub}</div>
        </div>
        """
    
    s1.markdown(sent_metric(
        metrics['positive'], 
        "Positive", 
        f"{metrics['pos_rate']:.1f}%", 
        "#10B981"
    ), unsafe_allow_html=True)
    
    s2.markdown(sent_metric(
        metrics['neutral'], 
        "Neutral", 
        f"{metrics['neu_rate']:.1f}%", 
        "#9CA3AF"
    ), unsafe_allow_html=True)
    
    s3.markdown(sent_metric(
        metrics['negative'], 
        "Negative", 
        f"{metrics['neg_rate']:.1f}%", 
        "#EF4444"
    ), unsafe_allow_html=True)
    
    # Calculate average sentiment by source
    avg_src_score = viz_df.groupby('source').apply(
        lambda x: ((x['sentiment_lower'] == 'positive').sum() - 
                   (x['sentiment_lower'] == 'negative').sum()) / len(x) * 100
    ).mean()
    
    sc_color = "#10B981" if avg_src_score > 0 else "#EF4444"
    s4.markdown(sent_metric(
        f"{avg_src_score:+.1f}", 
        "Avg Source Score", 
        "Mean Index", 
        sc_color
    ), unsafe_allow_html=True)


def render_timeseries_section(viz_df):
    """Render time series charts"""
    c1, c2 = st.columns([2, 1])
    
    with c1:
        st.markdown("**📈 Volume & Sentiment Timeline**")
        fig = create_volume_sentiment_timeline(viz_df)
        st.plotly_chart(fig, use_container_width=True)
    
    with c2:
        st.markdown("**🎭 Sentiment Share**")
        fig = create_sentiment_pie_chart(viz_df)
        st.plotly_chart(fig, use_container_width=True)


def render_deep_sentiment_section(viz_df):
    """Render deep sentiment analysis"""
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
    
    # Sentiment extremes
    render_sentiment_extremes(viz_df)


def render_sentiment_extremes(viz_df):
    """Render sentiment extremes section"""
    st.markdown("#### ⚡ Sentiment Extremes")
    
    # Calculate daily sentiment counts
    daily_sent_counts = viz_df.groupby(['date', 'sentiment_lower']).size().unstack(fill_value=0)
    
    most_pos_date, most_pos_val = None, 0
    most_neg_date, most_neg_val = None, 0
    
    if not daily_sent_counts.empty:
        if 'positive' in daily_sent_counts.columns:
            most_pos_date = daily_sent_counts['positive'].idxmax()
            most_pos_val = daily_sent_counts['positive'].max()
        if 'negative' in daily_sent_counts.columns:
            most_neg_date = daily_sent_counts['negative'].idxmax()
            most_neg_val = daily_sent_counts['negative'].max()
    
    se_c1, se_c2 = st.columns(2)
    
    with se_c1:
        date_str = most_pos_date.strftime('%a, %d %b') if most_pos_date else "N/A"
        st.markdown(f"""
        <div class="metric-card" style="border-left: 4px solid #10B981;">
            <div style="color: #10B981; font-weight: 700; text-transform: uppercase; font-size: 0.85rem; margin-bottom: 0.5rem;">
                😊 Most Positive Day
            </div>
            <div class="metric-value">{date_str}</div>
            <div style="color: #9CA3AF; font-size: 0.9rem;">{most_pos_val} positive articles</div>
        </div>
        """, unsafe_allow_html=True)
    
    with se_c2:
        date_str = most_neg_date.strftime('%a, %d %b') if most_neg_date else "N/A"
        st.markdown(f"""
        <div class="metric-card" style="border-left: 4px solid #EF4444;">
            <div style="color: #EF4444; font-weight: 700; text-transform: uppercase; font-size: 0.85rem; margin-bottom: 0.5rem;">
                😞 Most Negative Day
            </div>
            <div class="metric-value">{date_str}</div>
            <div style="color: #9CA3AF; font-size: 0.9rem;">{most_neg_val} negative articles</div>
        </div>
        """, unsafe_allow_html=True)


def render_category_trends_section(viz_df):
    """Render category evolution trends"""
    st.markdown("#### 📈 Category Trends Evolution")
    fig = create_category_trend_chart(viz_df, top_n=5)
    st.plotly_chart(fig, use_container_width=True)


def render_distribution_section(viz_df):
    """Render source and category distribution charts"""
    c3, c4 = st.columns(2)
    
    with c3:
        st.markdown("**📰 Top Sources**")
        fig = create_top_sources_bar(viz_df, top_n=10)
        st.plotly_chart(fig, use_container_width=True)
    
    with c4:
        st.markdown("**🏷️ Top Categories**")
        fig = create_category_pie_chart(viz_df, top_n=10)
        st.plotly_chart(fig, use_container_width=True)


def render_content_intelligence_section(viz_df):
    """Render content intelligence analysis"""
    st.markdown("#### 📝 Content Intelligence (N-Grams)")
    
    ci_c1, ci_c2 = st.columns(2)
    
    titles_list = viz_df['title'].astype(str).tolist()
    
    with ci_c1:
        st.markdown("**🔠 Top Contextual Bigrams (2-Word Phrases)**")
        df_bigrams = get_top_bigrams(titles_list, top_n=15)
        
        if not df_bigrams.empty:
            fig = create_bigrams_bar_chart(df_bigrams)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Not enough data for bigram analysis.")
    
    with ci_c2:
        st.markdown("**📰 Sentiment Extremes Feed**")
        render_sentiment_highlights(viz_df)


def render_sentiment_highlights(viz_df):
    """Render positive and negative article highlights"""
    col_pos, col_neg = st.columns(2)
    
    with col_pos:
        st.markdown("##### ✅ Positive Highlights")
        pos_arts = viz_df[viz_df['sentiment_lower'] == 'positive'].head(5)
        if not pos_arts.empty:
            for _, r in pos_arts.iterrows():
                title = html.escape(str(r['title'])[:80])
                st.markdown(f"""
                <div style="padding: 8px; border-left: 3px solid #10B981; background: rgba(16, 185, 129, 0.1); margin-bottom: 8px; border-radius: 4px;">
                    <div style="font-weight: bold; font-size: 0.9rem;">{title}</div>
                    <div style="font-size: 0.75rem; color: #9CA3AF;">{r['source']}</div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.caption("No positive articles.")
    
    with col_neg:
        st.markdown("##### ⚠️ Negative Highlights")
        neg_arts = viz_df[viz_df['sentiment_lower'] == 'negative'].head(5)
        if not neg_arts.empty:
            for _, r in neg_arts.iterrows():
                title = html.escape(str(r['title'])[:80])
                st.markdown(f"""
                <div style="padding: 8px; border-left: 3px solid #EF4444; background: rgba(239, 68, 68, 0.1); margin-bottom: 8px; border-radius: 4px;">
                    <div style="font-weight: bold; font-size: 0.9rem;">{title}</div>
                    <div style="font-size: 0.75rem; color: #9CA3AF;">{r['source']}</div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.caption("No negative articles.")


def render_temporal_section(viz_df):
    """Render temporal activity analysis"""
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
