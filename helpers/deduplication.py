# In a new file, e.g., helpers/deduplication.py or in correlation_engine.py

from pydantic import BaseModel, Field
from typing import Any, Dict, List, cast
from langchain.chat_models import init_chat_model
from langchain.prompts import ChatPromptTemplate
import os
import json

# --- Pydantic Models ---
class DuplicateGroup(BaseModel):
    """A group of tweet IDs that report the same news event."""
    tweet_ids: List[str] = Field(description="A list of two or more tweet IDs that are semantically identical.")

class DuplicateNewsReport(BaseModel):
    """The final structured output for identifying duplicate news."""
    duplicate_groups: List[DuplicateGroup]

# --- LLM Setup ---
deduplication_model = init_chat_model(
    "gemini-2.5-flash-lite-preview-06-17", # A good model for this kind of reasoning
    model_provider="google_genai",
    google_api_key=os.getenv("GEMINI_API_KEY"),
    temperature=0.2
).with_structured_output(DuplicateNewsReport)


deduplication_prompt = ChatPromptTemplate.from_template(
"""
You are a highly efficient news desk editor. Your task is to identify and group tweets that are reporting the exact same underlying news event.

Analyze the following list of tweets, each with a unique ID. Some of them may be from different sources but describe the same event (e.g., "House passes bill" vs. "Lawmakers approve legislation").

Your goal is to return a list of groups. Each group should contain the IDs of tweets that are duplicates. If a tweet is unique and has no duplicates in the list, it should NOT appear in your output.

**Example:**
- Tweet A: "BREAKING: US House officially passes crypto Clarity Act."
- Tweet B: "The Clarity Act for crypto has just been approved by the House."
- Tweet C: "New Russian sanctions announced by the White House."

**Correct Output:**
{{
  "duplicate_groups": [
    {{ "tweet_ids": ["A", "B"] }}
  ]
}}
(Tweet C is unique, so it is not included)

---
**LIST OF TWEETS TO ANALYZE:**
{tweets_json}
"""
)

deduplication_chain = deduplication_prompt | deduplication_model


def get_tweet_engagement(tweet: dict) -> int:
    """Calculates a simple engagement score for a tweet."""
    # Weights can be tuned
    return tweet.get('likeCount', 0) + (tweet.get('retweetCount', 0) * 2) + tweet.get('replyCount', 0)

async def deduplicate_raw_tweets(raw_tweets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Takes a list of raw tweets, uses an LLM to find duplicates, and returns a clean list
    containing only the highest-engagement version of each news event.
    """
    if len(raw_tweets) < 2:
        return raw_tweets

    print(f"\n--- Running pre-processing de-duplication on {len(raw_tweets)} tweets ---")

    # 1. Prepare input for the LLM
    tweets_for_llm = [{"id": t['id'], "text": t['text']} for t in raw_tweets]

    # 2. Call the LLM to find duplicate groups
    try:
        raw_response = await deduplication_chain.ainvoke({ # <--- Store the raw response first
            "tweets_json": json.dumps(tweets_for_llm, indent=2)
        })

        # --- THE FIX IS HERE ---
        # First, a safety check for None, just in case the API call fails silently.
        if raw_response is None:
            print("  - LLM de-duplication returned a null response. Skipping.")
            return raw_tweets
        
        # Now, cast the raw response to our specific Pydantic model.
        # This tells the type checker what to expect.
        response_model = cast(DuplicateNewsReport, raw_response)
        duplicate_groups = response_model.duplicate_groups
        
    except Exception as e:
        print(f"  - LLM de-duplication failed: {e}. Skipping this step.")
        return raw_tweets # Return the original list if LLM fails

    if not duplicate_groups:
        print("  - LLM found no duplicate tweets.")
        return raw_tweets

    # 3. Process groups to find which tweets to discard
    tweets_to_discard_ids = set()
    tweets_by_id = {t['id']: t for t in raw_tweets}

    for group in duplicate_groups:
        if not group.tweet_ids or len(group.tweet_ids) < 2:
            continue

        # Find the tweet with the highest engagement score in this group
        best_tweet_in_group = max(
            [tweets_by_id[tid] for tid in group.tweet_ids],
            key=get_tweet_engagement
        )
        
        print(f"  - Duplicate group found. Keeping best tweet {best_tweet_in_group['id']} (Engagement: {get_tweet_engagement(best_tweet_in_group)})")

        # Add all OTHER tweet IDs from the group to the discard list
        for tweet_id in group.tweet_ids:
            if tweet_id != best_tweet_in_group['id']:
                tweets_to_discard_ids.add(tweet_id)
                print(f"    - Discarding tweet {tweet_id}")

    # 4. Create the final, clean list of tweets
    final_tweets = [t for t in raw_tweets if t['id'] not in tweets_to_discard_ids]
    
    print(f"--- De-duplication complete. Filtered down to {len(final_tweets)} unique tweets. ---")
    return final_tweets