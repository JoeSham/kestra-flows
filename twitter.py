import os
import tweepy
import psycopg2 from psycopg2.extras
import execute_values
from datetime import datetime, timedelta

# Twitter API credentials
API_KEY = os.getenv("TWITTER_API_KEY")
API_SECRET = os.getenv("TWITTER_API_SECRET")
ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname="main",
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host="localhost",
    port=5432
)


# Fetch tweets from Twitter
def fetch_tweets(account):
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)

    # Get tweets from the last 24 hours
    since_time = datetime.utcnow() - timedelta(days=1)
    tweets = api.user_timeline(screen_name=account, since_id=since_time, tweet_mode='extended')
    return [(tweet.id_str, account, tweet.full_text, tweet.created_at, tweet.created_at) for tweet in tweets]


# Store tweets in PostgreSQL with upsert
def store_tweets(tweets):
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO public.twitter (tweet_id, account, content, tweet_created_at, tweet_updated_at, created_at, updated_at)
            VALUES %s
            ON CONFLICT (tweet_id) DO UPDATE SET
            content = EXCLUDED.content,
            account = EXCLUDED.account,
            tweet_created_at = EXCLUDED.tweet_created_at,
            tweet_updated_at = EXCLUDED.tweet_updated_at,
            updated_at = NOW()
        """, tweets)
        conn.commit()


# Main function to fetch and store tweets
def main():
    accounts = ['necnevim11', 'AkciovyGURU', 'MichalSemotan']
    all_tweets = []
    for account in accounts:
        tweets = fetch_tweets(account)
        all_tweets.extend(tweets)
        store_tweets(all_tweets)


if __name__ == '__main__':
    main()

