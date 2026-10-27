
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda data: json.dumps(data).encode("utf-8")
)

posts = [
    ("AI is changing the world! #AI #Technology", ["AI", "Technology"]),
    ("Learning Big Data processing today! #BigData", ["BigData"]),
    ("Python makes coding easier! #Python #Coding", ["Python", "Coding"]),
    ("Machine learning is exciting! #AI #MachineLearning", ["AI", "MachineLearning"]),
    ("Working on a streaming project! #BigData #Python", ["BigData", "Python"]),
    ("New technology is amazing! #Technology #Innovation", ["Technology", "Innovation"]),
    ("AI tools are becoming popular! #AI #Innovation", ["AI", "Innovation"]),
    ("Data analytics is useful! #BigData #Analytics", ["BigData", "Analytics"]),
]

print("Kafka Producer Started!")

try:
    while True:
        message, hashtags = random.choice(posts)

        post = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "text": message,
            "hashtags": hashtags
        }

        producer.send("social-media-posts", value=post)
        producer.flush()

        print("Sent to Kafka:", json.dumps(post))
        time.sleep(2)

except KeyboardInterrupt:
    print("Producer stopped.")

finally:
    producer.close()
