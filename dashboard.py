
import json
import os
import streamlit as st
import pandas as pd
import plotly.express as px

from collections import Counter
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh

# Page settings
st.set_page_config(
    page_title="Real-Time Social Media Trend Detection",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Real-Time Social Media Trend Detection")
st.caption(
    "Live hashtag monitoring powered by Apache Kafka and Apache Spark"
)

# Refresh every 3 seconds
st_autorefresh(interval=3000, key="dashboard_refresh")

# Initialize session data
if "posts" not in st.session_state:
    st.session_state.posts = []

if "counts" not in st.session_state:
    st.session_state.counts = Counter()

# Fetch new posts from Kafka
def fetch_posts():
    new_posts = []
    consumer = None

    try:
        consumer = KafkaConsumer(
            "social-media-posts",
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset="latest",
            enable_auto_commit=False,
            group_id=None,
            value_deserializer=lambda x: json.loads(
                x.decode("utf-8")
            ),
            consumer_timeout_ms=1000
        )

        for message in consumer:
            new_posts.append(message.value)

            if len(new_posts) >= 50:
                break

    except Exception as error:
        st.error(f"Kafka connection error: {error}")

    finally:
        if consumer is not None:
            consumer.close()

    return new_posts


# Receive new posts
new_posts = fetch_posts()

for post in new_posts:
    st.session_state.posts.append(post)
    st.session_state.counts.update(post.get("hashtags", []))

# Keep latest 500 posts
st.session_state.posts = st.session_state.posts[-500:]

posts = st.session_state.posts
counts = st.session_state.counts

# Dashboard metrics
col1, col2, col3 = st.columns(3)

col1.metric("Posts received", len(posts))
col2.metric("Unique hashtags", len(counts))

top_tag = counts.most_common(1)

col3.metric(
    "Top trending hashtag",
    f"#{top_tag[0][0]}" if top_tag else "Waiting for data..."
)

# Kafka hashtag chart
st.subheader("🔥 Trending Hashtags — Kafka")

if counts:
    trend_df = pd.DataFrame(
        counts.most_common(10),
        columns=["Hashtag", "Count"]
    )

    fig = px.bar(
        trend_df,
        x="Hashtag",
        y="Count",
        title="Top 10 Hashtags Received",
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
    st.info("Waiting for social media posts from Kafka...")


# Spark Structured Streaming results
st.subheader("⚡ Apache Spark — Real-Time Trend Counts")

spark_file = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "spark_trends.json"
)

try:
    with open(spark_file, "r", encoding="utf-8") as file:
        spark_data = json.load(file)

    if spark_data:
        spark_df = pd.DataFrame(spark_data)

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
        st.info("Waiting for Spark trend results...")

except (FileNotFoundError, json.JSONDecodeError):
    st.info(
        "Waiting for Spark to create spark_trends.json. "
        "Make sure Spark streaming is running."
    )


# Latest social media posts
st.subheader("🗞️ Latest Social Media Posts")

if posts:
    for post in reversed(posts[-20:]):
        st.markdown(f"**{post.get('text', '')}**")
        st.caption(post.get("timestamp", ""))
        st.divider()

else:
    st.info(
        "No posts received yet. Make sure Kafka and the producer are running."
    )
