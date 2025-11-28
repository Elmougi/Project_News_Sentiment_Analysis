import streamlit as st
import pandas as pd
import plotly.express as px
from Streamlit.ui.charts import render_sentiment_chart, render_trend_chart

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
