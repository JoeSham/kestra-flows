import os
import logging
import tweepy
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta

# Get environment variables
bearer_token = os.getenv('TWITTER_BEARER_TOKEN')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME', 'main')
db_host = os.getenv('DB_HOST', 'localhost')
db_port = os.getenv('DB_PORT', '5432')  # Default to '5432' if not specified

# Set up Twitter client
client = tweepy.Client(bearer_token=bearer_token)

# Define user accounts to track
usernames = ["necnevim11", "AkciovyGURU", "MichalSemotan"]

# Calculate time window
now = datetime.utcnow()
yesterday = now - timedelta(days=1)

# Construct the database connection string
conn_str = f"dbname='{db_name}' user='{db_user}' password='{db_password}' host='{db_host}' port='{db_port}'"

logging.info(conn_str)

# Connect to the PostgreSQL database
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS public.twitter (
    id BIGINT PRIMARY KEY,
    username TEXT NOT NULL,
    tweet_text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
)
""")
conn.commit()

# Function to upsert tweet data
def upsert_tweets(tweets):
    query = """
    INSERT INTO twitter (id, username, tweet_text, created_at, updated_at)
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        tweet_text = EXCLUDED.tweet_text,
        updated_at = EXCLUDED.updated_at
    """
    data = [
        (tweet.id, username, tweet.text, tweet.created_at, now)
        for tweet, username in tweets
    ]
    execute_values(cursor, query, data)
    conn.commit()

# Fetch and store tweets
tweets_to_store = []
for username in usernames:
    # Get user ID by username
    user_response = client.get_user(username=username)
    user_id = user_response.data.id

    # Fetch tweets
    tweets_response = client.get_users_tweets(
        id=user_id,
        start_time=yesterday.isoformat("T") + "Z",
        end_time=now.isoformat("T") + "Z",
        tweet_fields=["created_at", "text"],
        max_results=100
    )

    # Append tweets to list
    if tweets_response.data:
        tweets_to_store.extend(
            [(tweet, username) for tweet in tweets_response.data]
        )

# Upsert fetched tweets into the database
upsert_tweets(tweets_to_store)

# Close the database connection
cursor.close()
conn.close()
