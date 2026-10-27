
import json
import os
import random
from datetime import datetime
from collections import Counter

import streamlit as st
import pandas as pd
import plotly.express as px
from streamlit_autorefresh import st_autorefresh

try:
    from kafka import KafkaConsumer
except ImportError:
    KafkaConsumer = None


# ---------------- PAGE CONFIG ----------------
st.set_page_config(
    page_title="Real-Time Social Media Trend Detection",
    page_icon="📈",
    layout="wide"
)

# ---------------- HEADER ----------------
st.title("📈 Real-Time Social Media Trend Detection")
st.caption(
    "An interactive dashboard for monitoring social media "
    "hashtags and identifying emerging trends."
)

st_autorefresh(interval=3000, key="dashboard_refresh")


# ---------------- SESSION STATE ----------------
if "posts" not in st.session_state:
    st.session_state.posts = []

if "counts" not in st.session_state:
    st.session_state.counts = Counter()

if "demo_index" not in st.session_state:
    st.session_state.demo_index = 0


# ---------------- SAMPLE POSTS ----------------
DEMO_POSTS = [
    {
        "text": "Big Data is transforming the digital world!",
        "hashtags": ["BigData", "AI"]
    },
    {
        "text": "Machine Learning is shaping the future.",
        "hashtags": ["MachineLearning", "AI"]
    },
    {
        "text": "Cloud computing makes applications scalable.",
        "hashtags": ["Cloud", "BigData"]
    },
    {
        "text": "Python simplifies data analytics.",
        "hashtags": ["Python", "DataScience"]
    },
    {
        "text": "Apache Spark enables fast data processing.",
        "hashtags": ["Spark", "BigData"]
    },
    {
        "text": "Technology continues to evolve every day.",
        "hashtags": ["Technology", "AI"]
    },
    {
        "text": "Real-time analytics helps discover trends.",
        "hashtags": ["Analytics", "BigData"]
    },
    {
        "text": "Data science turns information into insights.",
        "hashtags": ["DataScience", "Python"]
    },
    {
        "text": "Cloud platforms support modern applications.",
        "hashtags": ["Cloud", "Technology"]
    },
    {
        "text": "Artificial intelligence is changing industries.",
        "hashtags": ["AI", "MachineLearning"]
    }
]


# ---------------- KAFKA DATA ----------------
def fetch_posts():
    if KafkaConsumer is None:
        return [], False

    consumer = None

    try:
        consumer = KafkaConsumer(
            "social-media-posts",
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset="latest",
            enable_auto_commit=False,
            group_id=None,
            value_deserializer=lambda value: json.loads(
                value.decode("utf-8")
            ),
            consumer_timeout_ms=1000,
            request_timeout_ms=3000,
            api_version_auto_timeout_ms=3000
        )

        received_posts = []

        for message in consumer:
            if isinstance(message.value, dict):
                received_posts.append(message.value)

            if len(received_posts) >= 50:
                break

        return received_posts, True

    except Exception:
        return [], False

    finally:
        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                pass


# ---------------- LOAD DASHBOARD DATA ----------------
new_posts, kafka_connected = fetch_posts()

if kafka_connected and new_posts:
    for post in new_posts:
        post.setdefault("text", "")
        post.setdefault("timestamp", datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        ))
        post.setdefault("hashtags", [])

        st.session_state.posts.append(post)
        st.session_state.counts.update(
            [str(tag).lstrip("#") for tag in post["hashtags"]]
        )

elif not kafka_connected:
    demo_post = DEMO_POSTS[
        st.session_state.demo_index % len(DEMO_POSTS)
    ].copy()

    demo_post["timestamp"] = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    st.session_state.posts.append(demo_post)
    st.session_state.counts.update(demo_post["hashtags"])
    st.session_state.demo_index += 1

st.session_state.posts = st.session_state.posts[-500:]

posts = st.session_state.posts
counts = st.session_state.counts


# ---------------- METRICS ----------------
st.subheader("📊 Dashboard Overview")

col1, col2, col3 = st.columns(3)

col1.metric(
    "📝 Posts Received",
    len(posts)
)

col2.metric(
    "🏷️ Unique Hashtags",
    len(counts)
)

top_tag = counts.most_common(1)

col3.metric(
    "🔥 Top Trending Hashtag",
    f"#{top_tag[0][0]}" if top_tag else "N/A"
)

st.divider()


# ---------------- TRENDING HASHTAGS ----------------
st.subheader("🔥 Trending Hashtags")

if counts:
    trend_df = pd.DataFrame(
        counts.most_common(10),
        columns=["Hashtag", "Count"]
    )

    trend_df["Hashtag"] = "#" + trend_df["Hashtag"].astype(str)

    fig = px.bar(
        trend_df,
        x="Hashtag",
        y="Count",
        title="Top 10 Trending Hashtags",
        text="Count",
        color="Count",
        color_continuous_scale="Blues"
    )

    fig.update_layout(
        xaxis_title="Hashtag",
        yaxis_title="Post Count",
        showlegend=False
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Trend information will appear here.")


# ---------------- SPARK RESULTS ----------------
st.divider()
st.subheader("⚡ Apache Spark — Streaming Analytics")

spark_file = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "spark_trends.json"
)

try:
    with open(spark_file, "r", encoding="utf-8") as file:
        spark_data = json.load(file)

    if spark_data:
        spark_df = pd.DataFrame(spark_data)

        if {"hashtag", "post_count"}.issubset(spark_df.columns):
            spark_df = spark_df.sort_values(
                "post_count",
                ascending=False
            )

            st.dataframe(
                spark_df,
                use_container_width=True,
                hide_index=True
            )

            fig_spark = px.bar(
                spark_df.head(10),
                x="hashtag",
                y="post_count",
                title="Hashtag Counts — 30-Second Windows",
                color="post_count",
                text="post_count"
            )

            fig_spark.update_layout(
                xaxis_title="Hashtag",
                yaxis_title="Post Count"
            )

            st.plotly_chart(
                fig_spark,
                use_container_width=True
            )
        else:
            st.info("Spark analytics data is not available yet.")
    else:
        st.info("Spark analytics will appear here when results are available.")

except (FileNotFoundError, json.JSONDecodeError):
    st.info("Spark analytics results are not available yet.")


# ---------------- LATEST POSTS ----------------
st.divider()
st.subheader("🗞️ Latest Social Media Posts")

if posts:
    for post in reversed(posts[-20:]):
        st.markdown(f"**{post.get('text', '')}**")

        hashtags = post.get("hashtags", [])

        if hashtags:
            st.caption(
                " ".join(
                    f"#{str(tag).lstrip('#')}"
                    for tag in hashtags
                )
            )

        st.caption(post.get("timestamp", ""))
        st.divider()
else:
    st.info("Posts will appear here.")


# ---------------- FOOTER ----------------
st.caption(
    "Built with Python, Streamlit, Apache Kafka and Apache Spark."
)
