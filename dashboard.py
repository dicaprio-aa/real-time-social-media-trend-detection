
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


# ---------------- PAGE SETTINGS ----------------
st.set_page_config(
    page_title="Real-Time Social Media Trend Detection",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Real-Time Social Media Trend Detection")
st.caption(
    "Social media trend monitoring using Apache Kafka "
    "and Apache Spark Structured Streaming"
)

st_autorefresh(interval=3000, key="dashboard_refresh")


# ---------------- SESSION DATA ----------------
if "posts" not in st.session_state:
    st.session_state.posts = []

if "counts" not in st.session_state:
    st.session_state.counts = Counter()

if "demo_index" not in st.session_state:
    st.session_state.demo_index = 0


# ---------------- DEMO POSTS ----------------
DEMO_POSTS = [
    {
        "text": "Big Data is changing the world!",
        "hashtags": ["BigData", "AI"]
    },
    {
        "text": "Machine Learning is growing rapidly.",
        "hashtags": ["MachineLearning", "AI"]
    },
    {
        "text": "Cloud computing is the future.",
        "hashtags": ["Cloud", "BigData"]
    },
    {
        "text": "Python makes data analysis easier.",
        "hashtags": ["Python", "DataScience"]
    },
    {
        "text": "Spark processes streaming data fast.",
        "hashtags": ["Spark", "BigData"]
    },
    {
        "text": "Technology trends are evolving every day.",
        "hashtags": ["Technology", "AI"]
    },
    {
        "text": "Real-time analytics helps discover trends.",
        "hashtags": ["Analytics", "BigData"]
    },
    {
        "text": "Data science turns data into insights.",
        "hashtags": ["DataScience", "Python"]
    }
]


# ---------------- KAFKA CONNECTION ----------------
def fetch_posts():
    """Read available posts from the local Kafka broker."""
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

        received = []

        for message in consumer:
            post = message.value
            if isinstance(post, dict):
                received.append(post)

            if len(received) >= 50:
                break

        return received, True

    except Exception:
        # Cloud hosting cannot normally access your laptop's localhost.
        return [], False

    finally:
        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                pass


# ---------------- LOAD POSTS ----------------
new_posts, kafka_connected = fetch_posts()

if kafka_connected and new_posts:
    # Real posts received from Kafka
    for post in new_posts:
        post.setdefault("timestamp", datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        ))
        post.setdefault("text", "")
        post.setdefault("hashtags", [])

        st.session_state.posts.append(post)
        st.session_state.counts.update(post.get("hashtags", []))

    st.session_state.data_mode = "Kafka"

elif not kafka_connected:
    # Demo fallback for online hosting or unavailable Kafka
    demo = DEMO_POSTS[
        st.session_state.demo_index % len(DEMO_POSTS)
    ].copy()

    demo["timestamp"] = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    demo["hashtags"] = [
        tag.lstrip("#") for tag in demo["hashtags"]
    ]

    st.session_state.posts.append(demo)
    st.session_state.counts.update(demo["hashtags"])
    st.session_state.demo_index += 1
    st.session_state.data_mode = "Demo"

st.session_state.posts = st.session_state.posts[-500:]

posts = st.session_state.posts
counts = st.session_state.counts


# ---------------- STATUS ----------------
if st.session_state.get("data_mode") == "Kafka":
    st.success("Connected to Kafka — displaying received posts.")
else:
    st.info(
        "Demo mode: sample posts are displayed because Kafka "
        "is unavailable to this app. Online demo data is simulated."
    )


# ---------------- METRICS ----------------
col1, col2, col3 = st.columns(3)

col1.metric("Posts received", len(posts))
col2.metric("Unique hashtags", len(counts))

top_tag = counts.most_common(1)

col3.metric(
    "Top trending hashtag",
    f"#{top_tag[0][0]}" if top_tag else "Waiting for data..."
)


# ---------------- TRENDING HASHTAGS ----------------
st.subheader("🔥 Trending Hashtags")

if counts:
    trend_df = pd.DataFrame(
        counts.most_common(10),
        columns=["Hashtag", "Count"]
    )

    fig = px.bar(
        trend_df,
        x="Hashtag",
        y="Count",
        title="Top 10 Hashtags",
        color="Count",
        text="Count"
    )

    fig.update_layout(
        xaxis_title="Hashtag",
        yaxis_title="Number of Posts",
        showlegend=False
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No hashtags available yet.")


# ---------------- SPARK RESULTS ----------------
st.subheader("⚡ Apache Spark — Trend Counts")

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
                color="post_count",
                text="post_count",
                title="Hashtag Counts in 30-Second Windows"
            )

            fig_spark.update_layout(
                xaxis_title="Hashtag",
                yaxis_title="Spark Post Count",
                showlegend=False
            )

            st.plotly_chart(
                fig_spark,
                use_container_width=True
            )
        else:
            st.warning(
                "Spark JSON file does not contain the expected columns."
            )
    else:
        st.info(
            "Spark output file is empty. Run Spark streaming locally."
        )

except (FileNotFoundError, json.JSONDecodeError):
    st.info(
        "Spark output is not available on this server. "
        "Run Spark streaming on your laptop to generate "
        "spark_trends.json."
    )


# ---------------- LATEST POSTS ----------------
st.subheader("🗞️ Latest Social Media Posts")

if posts:
    for post in reversed(posts[-20:]):
        st.markdown(f"**{post.get('text', '')}**")
        hashtags = post.get("hashtags", [])
        if hashtags:
            st.caption(" ".join(f"#{tag.lstrip('#')}" for tag in hashtags))

        st.caption(post.get("timestamp", ""))
        st.divider()
else:
    st.info("Waiting for posts...")
