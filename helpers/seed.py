from datetime import datetime, timezone

# --- Import our new modules ---
from helpers.database import get_market_count, insert_markets
from helpers.embeddings import generate_embeddings
# ---

from helpers.x import get_tweets
from helpers.polymarket import get_markets
# from helpers.llm import ... (will be used later)

# --- Configuration ---
CHECK_INTERVAL_SECONDS = 7200

def seed_database_if_empty():
    """Checks if the database is empty and populates it with all active markets."""
    print("Checking if database requires seeding...")
    market_count = get_market_count()
    
    if market_count > 0:
        print(f"Database already seeded with {market_count} markets. Skipping.")
        return

    print("Database is empty. Starting one-time seeding process...")
    
    # 1. Fetch all currently active markets
    now_utc_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    all_markets_data = get_markets(end_date_min=now_utc_str)
    
    if not all_markets_data:
        print("Failed to fetch markets for seeding. Please check API and connection. Aborting.")
        return

    # 2. Prepare text for embedding (Question + Description)
    texts_to_embed = []
    for market in all_markets_data:
        # Combining question and description gives the model more context
        full_text = f"Question: {market.get('question', '')}\nDescription: {market.get('description', '')}"
        texts_to_embed.append(full_text)

    # 3. Generate embeddings for all markets
    market_embeddings = generate_embeddings(texts_to_embed)

    # 4. Insert into database
    if len(all_markets_data) == len(market_embeddings):
        print("Inserting markets and their embeddings into the database...")
        insert_markets(all_markets_data, market_embeddings)
    else:
        print("Error: Mismatch between number of markets and generated embeddings. Aborting insertion.")

    print("--- Seeding process complete. ---")
