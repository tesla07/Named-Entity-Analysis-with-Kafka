"""
Creates a news api client and stream the current top headline to kafka topic.
usage: news_api.py <bootstrap-servers> <topics> <API_KEY>
       <bootstrap-servers> The Kafka "bootstrap.servers" configuration. 
       <topics> : Name of the kafka to write in to
       <API_KEY>: API_KEY for new api client
"""
from newsapi import NewsApiClient
import logging
import time
import sys
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(filename='news_api.log', level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def get_client(api_key):
    # A function to create the new api client
    api_client = NewsApiClient(api_key=api_key)
    return api_client


def fetch_top_headline(api_client, kafka_producer, topic):
    try:
        #  this function makes a get request to /v2/top-headlines endpoint
        top_headlines = api_client.get_top_headlines(country='us')
        articles = top_headlines.get('articles', [])
        for article in articles:
            title = article.get('title')
            description = article.get('description')
            if description:
                # write the news description to the kafka topic
                print(f"Message sent: {description}")
                kafka_producer.send(topic=topic, value=description.encode('utf-8'))
        kafka_producer.flush()
        print("Flushed the data")
    except Exception as e:
         logging.error("An error occurred: %s", e)
         print("Error:", e)

if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("""
            Usage: news_api.py <bootstrap-servers> <topics> <API_KEY>
            """, file=sys.stderr)
        sys.exit(-1)

    # Read the bootstarp server and topic name from command line arguments
    bootstrap_servers = sys.argv[1]
    topics = sys.argv[2]
    API_KEY = sys.argv[3]

    # Create an instance of kafka producer:
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    # producer = None
    api_client = get_client(API_KEY)

    # Keep fetching the latest top headlines for every 5 minutes
    while True:
        logging.info("Fetching latest news...")
        fetch_top_headline(api_client, producer, topics)
        logging.info("Waiting for 5 minutes before fetching again...")
        time.sleep(60)  # Wait for 1 minutes (60 seconds)
