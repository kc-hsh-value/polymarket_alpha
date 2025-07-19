import requests
import time
from datetime import datetime, timedelta, UTC, timezone
from dotenv import load_dotenv
import os
from typing import List, Dict, Any
load_dotenv() 

# Configuration
API_KEY = os.getenv("X_API_KEY")  # TODO Replace with your TwitterAPI.io API key. You can find it in https://twitterapi.io/dashboard.
TARGET_ACCOUNTS = ["unusual_whales", "WatcherGuru", "spectatorindex","Polymarket","whalewatchpoly","rawsalerts","DeItaone", "PolymarketIntel","Megatron_ron"]  # The account you want to monitor
CHECK_INTERVAL = 7200  # Check every 2 hours (7200 seconds)
LAST_CHECKED_TIME = datetime.now(UTC) - timedelta(hours=2)  # Start by checking the last hour

def get_tweets(hours_ago: int = 2) -> List[Dict[str, Any]]:
    """
    Fetches recent tweets from a predefined list of target accounts.
    """
    API_KEY = os.getenv("X_API_KEY")
    if not API_KEY:
        print("Warning: X_API_KEY not found. Cannot fetch tweets.")
        return []
    
    until_time = datetime.now(timezone.utc)
    since_time = until_time - timedelta(hours=hours_ago)
    since_str = since_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")
    until_str = until_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")

    all_tweets: List[Dict[str, Any]] = []
    print("-" * 50)
    for target_account in TARGET_ACCOUNTS:
        print(f"Checking for tweets from @{target_account}...")
        query = f"from:{target_account} since:{since_str} until:{until_str} include:nativeretweets"
        url = "https://api.twitterapi.io/twitter/tweet/advanced_search"
        headers = {"X-API-Key": API_KEY}
        params = {"query": query, "queryType": "Latest"}
        
        next_cursor = None
        while True:
            if next_cursor:
                params["cursor"] = next_cursor
            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                tweets_page = data.get("tweets", [])
                if tweets_page:
                    all_tweets.extend(tweets_page)
                if data.get("has_next_page") and data.get("next_cursor"):
                    next_cursor = data.get("next_cursor")
                else:
                    break
            except requests.exceptions.RequestException as e:
                print(f"  Error fetching tweets for @{target_account}: {e}")
                break
    
    print("-" * 50)
    print(f"Total tweets found across all accounts: {len(all_tweets)}")
    return all_tweets



# Main monitoring loop
# def main():
#     # print(f"Starting to monitor tweets from {TARGET_ACCOUNTS}")
#     print(f"Starting to monitor tweets from accounts")
#     print(f"Checking every {CHECK_INTERVAL} seconds")
    
#     try:
#         while True:
#             tweets = get_tweets(hours_ago=2)
#             print(f"Found {len(tweets)} tweets")
#             print(f"Tweets: {tweets}")
#             time.sleep(CHECK_INTERVAL)
#     except KeyboardInterrupt:
#         print("Monitoring stopped.")

# if __name__ == "__main__":
#     main()