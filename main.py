import asyncio
import time
from datetime import datetime, timezone, timedelta

# Import your helper functions
from helpers.x import get_tweets
from helpers.polymarket import get_markets
from helpers.correlation_engine import run_correlation_engine
from helpers.database import get_next_cycle_number, insert_markets, insert_tweets, log_cycle_stats, prune_expired_markets, setup_database
from helpers.seed import seed_database_if_empty
from helpers.embeddings import generate_embeddings
import math
import discord

from helpers.discord_bot import send_new_correlations
from helpers.deduplication import deduplicate_raw_tweets

# --- Configuration ---
# Set the polling interval in seconds. 7200 seconds = 2 hours.
CHECK_INTERVAL_SECONDS = 7200 


async def alpha_cycle_loop(bot_client: discord.Client):
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
        cycle_stats = {
            "cycle_number": get_next_cycle_number(),
            "start_time": datetime.now(timezone.utc),
            "end_time": None,
            "status": "PENDING",
            "tweets_fetched": 0,
            "new_markets_fetched": 0,
            "correlations_found": 0, # We can infer this later
            "messages_sent": 0,
            "notes": None
        }
        try:
            print("\n" + "="*50)
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] --- Running new cycle #{cycle_stats['cycle_number']} ---")

             # --- Step A: Maintain Market Cache ---
            print("\n[1/4] Maintaining market cache...")
            prune_expired_markets()
            # --- Step B: Fetch New Markets ---
            # Fetch markets created in the last check interval
            since_time = datetime.now(timezone.utc) - timedelta(seconds=CHECK_INTERVAL_SECONDS)
            since_str = since_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            new_markets = get_markets(start_date_min=since_str)
            cycle_stats['new_markets_fetched'] = len(new_markets) if new_markets else 0
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
            cycle_stats['tweets_fetched'] = len(latest_tweets) if latest_tweets else 0
            if latest_tweets:
                # --- NEW DE-DUPLICATION STEP ---
                unique_tweets = await deduplicate_raw_tweets(latest_tweets)
                
                # Insert ONLY the clean, de-duplicated tweets into the database
                insert_tweets(unique_tweets)
             # --- Step C: Correlation Engine ---
            print("\n[3/4] Running correlation engine...")
            # TODO: Get unprocessed tweets from DB
            # TODO: Get active market embeddings from DB
            # TODO: Run semantic search and LLM refinement
            # TODO: Store successful correlations to prevent duplicates
            # run_correlation_engine()
            await asyncio.to_thread(run_correlation_engine)

            # --- Step D: Send to Discord ---
            print("\n[4/4] Sending new correlations to Discord...")
            # TODO: Query for new, unsent correlations
            # TODO: Format and send Discord messages
            # TODO: Mark correlations as sent
            print("\n[4/4] Sending new correlations to Discord...")
            num_messages_sent = await send_new_correlations()
            cycle_stats['messages_sent'] = num_messages_sent
            cycle_stats['status'] = 'SUCCESS'

            print("\n--- Cycle finished. ---")
    #         # 4. Run correlation engine
            
    #         print(f"Cycle finished. Waiting for {CHECK_INTERVAL_SECONDS} seconds...")
    #         time.sleep(CHECK_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            print("\nMonitoring stopped by user.")
            break
        except Exception as e:
            # print(f"An unexpected error occurred in the main loop: {e}")
            # time.sleep(60) # Wait a bit before retrying after an error
            print(f"!! CRITICAL ERROR in cycle #{cycle_stats['cycle_number']}: {e}")
            import traceback
            traceback.print_exc()
            # --- LOG FAILURE ---
            cycle_stats['status'] = 'FAILED'
            cycle_stats['notes'] = str(e)
    
        # print(f"Waiting for {CHECK_INTERVAL_SECONDS} seconds for the next cycle...")
        # await asyncio.sleep(CHECK_INTERVAL_SECONDS) # Use asyncio.sleep
        finally:
            # --- COMMIT LOGS ---
            # This block runs whether the cycle succeeded or failed
            cycle_stats['end_time'] = datetime.now(timezone.utc)
            log_cycle_stats(cycle_stats)
            print(f"--- Cycle #{cycle_stats['cycle_number']} finished with status: {cycle_stats['status']} ---")
            
            print(f"Waiting for {CHECK_INTERVAL_SECONDS} seconds for the next cycle...")
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)


# if __name__ == "__main__":
#     try:
#         asyncio.run(main_loop())
#     except KeyboardInterrupt:
#         print("\nProgram stopped by user.")
