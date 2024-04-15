import time
import json
from kafka import KafkaProducer
from newsapi import NewsApiClient
# Initialize NewsAPI client with your API key
newsapi = NewsApiClient(api_key='92c016ec03cd47569da84aec6a9b4c69')

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Define Kafka topic
topic = 'topic1'

def fetch_and_send_news():
    # Fetch news articles from NewsAPI
    top_headlines = newsapi.get_top_headlines(language='en', page_size=10)

    # Extract relevant information from news articles
    articles = top_headlines['articles']
    for article in articles:
        article_data = {
            'title': article['title'],
            'description': article['description'],
        }
        # Send article data to Kafka topic
        producer.send(topic, json.dumps(article_data).encode('utf-8'))
        print("Sent:", article_data['title'])

    # Flush producer to send messages immediately
    producer.flush()

if __name__ == "__main__":
    while True:
        fetch_and_send_news()
        # Fetch news every 60 seconds
        time.sleep(60)
