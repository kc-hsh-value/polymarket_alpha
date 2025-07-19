import asyncio
import time
from datetime import datetime, timezone, timedelta

# Import your helper functions
from helpers.x import get_tweets
from helpers.polymarket import get_markets
from helpers.correlation_engine import run_correlation_engine
from helpers.database import insert_markets, insert_tweets, prune_expired_markets, setup_database
from helpers.seed import seed_database_if_empty
from helpers.embeddings import generate_embeddings
import math

from helpers.discord_bot import send_new_correlations

# --- Configuration ---
# Set the polling interval in seconds. 7200 seconds = 2 hours.
CHECK_INTERVAL_SECONDS = 7200 


async def main_loop():
    """
    The main operational loop for the agent.
    """
    # Run the setup once at the start
    setup_database()
    
    # Run the one-time seeding process if needed
    seed_database_if_empty()
    # //WIP v2.0: Bridge the knowledge gap. Use a research agent (LLM + Web Search) to enrich market context and enable true conceptual matching.
#   the way we will do it is pass every single market to the LLM, and tell it to return a structured ouput with the more infromative description 
    print("--- Starting Tweet-Market Correlation Agent ---")
    print(f"Polling for new data every {CHECK_INTERVAL_SECONDS} seconds...")

    # The rest of the loop will go here...
    while True:
        try:
            print("\n" + "="*50)
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] --- Running new cycle ---")

             # --- Step A: Maintain Market Cache ---
            print("\n[1/4] Maintaining market cache...")
            prune_expired_markets()
            # --- Step B: Fetch New Markets ---
            # Fetch markets created in the last check interval
            since_time = datetime.now(timezone.utc) - timedelta(seconds=CHECK_INTERVAL_SECONDS)
            since_str = since_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            new_markets = get_markets(start_date_min=since_str)
            if new_markets:
                print(f"Found {len(new_markets)} new markets to process.")
                # Process and add them to the DB (same logic as seeding)
                texts_to_embed = [f"Question: {m.get('question', '')}\nDescription: {m.get('description', '')}" for m in new_markets]
                new_embeddings = generate_embeddings(texts_to_embed)
                # We need to adapt insert_markets to handle the new embedding_text logic
                insert_markets(new_markets, new_embeddings) # Assuming insert_markets is adapted
                # --- Step B: Get Latest News ---
            print("\n[2/4] Fetching latest tweets...")
            check_interval_hours = math.ceil(CHECK_INTERVAL_SECONDS / 3600)
            latest_tweets = get_tweets(hours_ago=check_interval_hours)
            if latest_tweets:
                insert_tweets(latest_tweets)
             # --- Step C: Correlation Engine ---
            print("\n[3/4] Running correlation engine...")
            # TODO: Get unprocessed tweets from DB
            # TODO: Get active market embeddings from DB
            # TODO: Run semantic search and LLM refinement
            # TODO: Store successful correlations to prevent duplicates
            run_correlation_engine()

            # --- Step D: Send to Discord ---
            print("\n[4/4] Sending new correlations to Discord...")
            # TODO: Query for new, unsent correlations
            # TODO: Format and send Discord messages
            # TODO: Mark correlations as sent
            print("\n[4/4] Sending new correlations to Discord...")
            await send_new_correlations() # <-- Use await to call the async function

            print("\n--- Cycle finished. ---")
    #         # 4. Run correlation engine
            
    #         print(f"Cycle finished. Waiting for {CHECK_INTERVAL_SECONDS} seconds...")
    #         time.sleep(CHECK_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            print("\nMonitoring stopped by user.")
            break
        except Exception as e:
            print(f"An unexpected error occurred in the main loop: {e}")
            time.sleep(60) # Wait a bit before retrying after an error
    
        print(f"Waiting for {CHECK_INTERVAL_SECONDS} seconds for the next cycle...")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS) # Use asyncio.sleep


if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("\nProgram stopped by user.")
