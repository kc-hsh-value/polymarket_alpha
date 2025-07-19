import sqlite3
import json
import numpy as np
import io
from typing import List, Dict, Any, Tuple
from datetime import datetime, timezone

# --- Adapter for storing numpy arrays in SQLite ---
def adapt_array(arr):
    """Converts numpy array to a binary format for SQLite."""
    out = io.BytesIO()
    np.save(out, arr)
    out.seek(0)
    return sqlite3.Binary(out.read())

def convert_array(text):
    """Converts binary format from SQLite back to a numpy array."""
    out = io.BytesIO(text)
    out.seek(0)
    # Allow pickle for compatibility, though it's less of a concern for self-generated data.
    return np.load(out, allow_pickle=True)

# Register the adapter and converter for numpy arrays
sqlite3.register_adapter(np.ndarray, adapt_array)
sqlite3.register_converter("array", convert_array)

DB_FILE = "polymarket_bot.db"

def get_db_connection():
    """Establishes a connection to the SQLite database."""
    # `detect_types` is crucial for the numpy array conversion
    conn = sqlite3.connect(DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row  # Allows accessing columns by name
    return conn

def setup_database():
    """Creates the necessary tables if they don't exist."""
    print("Setting up database...")
    conn = get_db_connection()
    cursor = conn.cursor()

    # --- MARKETS Table ---
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS markets (
        id TEXT PRIMARY KEY,
        question TEXT NOT NULL,
        slug TEXT NOT NULL,
        market_url TEXT NOT NULL,           -- ADDED: The reliable URL for the market
        parent_event_id TEXT,               -- ADDED: The ID of the parent event if the market is a child event, basically a non-binary market
        image_url TEXT,
        yes_price REAL,
        no_price REAL,
        end_date_utc DATETIME NOT NULL,
        embedding_text TEXT,
        embedding array,
        is_active BOOLEAN DEFAULT 1,
        full_data_json TEXT
    )
    """)
    # Add an index for faster lookups on active markets
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_markets_active ON markets (is_active)")


    # --- TWEETS Table ---
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tweets (
        id TEXT PRIMARY KEY,
        text TEXT NOT NULL,
        tweet_url TEXT NOT NULL,
        author_name TEXT,
        author_url TEXT,
        created_at_utc DATETIME NOT NULL,
        embedding array,
        is_processed BOOLEAN DEFAULT 0,
        full_data_json TEXT
    )
    """)
    # Add an index for faster lookups on unprocessed tweets
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweets_processed ON tweets (is_processed)")


    # --- SENT_CORRELATIONS Table ---
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sent_correlations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tweet_id TEXT NOT NULL,
        market_id TEXT NOT NULL,
        relevance_score REAL,
        relevance_score_reasoning TEXT,                     -- ADDED: To store the LLM's justification
        urgency_score REAL,
        urgency_score_reasoning TEXT,                     -- ADDED: To store the LLM's justification
        sent_to_discord BOOLEAN DEFAULT 0,  -- ADDED: 0 for not sent, 1 for sent
        sent_at_utc DATETIME,               -- CHANGED: No longer has default, will be set on send
        FOREIGN KEY (tweet_id) REFERENCES tweets(id),
        FOREIGN KEY (market_id) REFERENCES markets(id),
        UNIQUE(tweet_id, market_id)
    )
    """)

    conn.commit()
    conn.close()
    print("Database setup complete.")


def get_market_count() -> int:
    """Returns the total number of markets in the database."""
    conn = get_db_connection()
    count = conn.execute("SELECT COUNT(id) FROM markets").fetchone()[0]
    conn.close()
    return count

# We'll need a function to insert markets. Let's add it now.
def insert_markets(markets_data: List[Dict[str, Any]], embeddings: List[np.ndarray]):
    """Inserts a list of markets and their embeddings into the database."""
    if not markets_data:
        return
        
    conn = get_db_connection()
    cursor = conn.cursor()
    
    records_to_insert = []
    for market, embedding in zip(markets_data, embeddings):
        try:
            # Safely extract outcome prices
            outcome_prices_json = market.get('outcomePrices', '[]')
            prices = json.loads(outcome_prices_json)
            yes_price = float(prices[0]) if len(prices) > 0 else 0.0
            no_price = float(prices[1]) if len(prices) > 1 else 0.0

            embedding_text = f"Question: {market.get('question', '')}\nDescription: {market.get('description', '')}"
            events = market.get('events')
            if isinstance(events, list) and len(events) > 0:
                parent_slug = market.get('slug') # Default to top-level slug

                if events and isinstance(events, list) and len(events) > 0:
                    # The 'ticker' in the event often matches the desired URL slug
                    event_slug = market['events'][0].get('ticker') 
                    parent_event_id = market['events'][0].get('id')
                    if event_slug:
                        parent_slug = event_slug
            
            market_url = f"https://polymarket.com/event/{parent_slug}"

            records_to_insert.append((
                market.get('id'),
                parent_event_id, 
                market.get('question'),
                market.get('slug'),
                market_url,
                market.get('image'),
                yes_price,
                no_price,
                market.get('endDate'), # Assumes 'endDate' is in a format SQLite can understand (ISO 8601)
                embedding_text,
                embedding,
                json.dumps(market)
            ))
        except (json.JSONDecodeError, IndexError, TypeError, KeyError) as e:
            print(f"Skipping market {market.get('id')} due to parsing error: {e}")
            continue

    # Use INSERT OR IGNORE to prevent errors if a market already exists.
    # It will simply skip the insertion for that row.
    cursor.executemany("""
        INSERT OR IGNORE INTO markets (
            id, parent_event_id, question, slug, market_url, image_url, yes_price, no_price, 
            end_date_utc, embedding_text, embedding, full_data_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, records_to_insert)
    
    conn.commit()
    print(f"Attempted to insert {len(records_to_insert)} markets. {cursor.rowcount} were newly added.")
    conn.close()




# In helpers/database.py

# ... (keep all existing functions) ...

def prune_expired_markets():
    """Sets the is_active flag to 0 for markets whose end date has passed."""
    conn = get_db_connection()
    cursor = conn.cursor()
    # Using CURRENT_TIMESTAMP is efficient as it's handled by SQLite directly
    cursor.execute("""
        UPDATE markets 
        SET is_active = 0 
        WHERE end_date_utc < CURRENT_TIMESTAMP AND is_active = 1
    """)
    updated_count = cursor.rowcount
    conn.commit()
    conn.close()
    if updated_count > 0:
        print(f"Pruned {updated_count} expired markets.")
    return updated_count

def insert_tweets(tweets_data: List[Dict[str, Any]]):
    """Inserts a list of new tweets into the database."""
    if not tweets_data:
        return
        
    conn = get_db_connection()
    cursor = conn.cursor()
    
    records_to_insert = []
    for tweet in tweets_data:
        # The tweet 'createdAt' format from the API needs to be parsed
        # Example: "Tue Jul 15 16:43:18 +0000 2025"
        try:
            created_at_dt = datetime.strptime(tweet['createdAt'], '%a %b %d %H:%M:%S +0000 %Y')
            created_at_iso = created_at_dt.isoformat()
        except (ValueError, KeyError):
            created_at_iso = datetime.now(timezone.utc).isoformat() # Fallback

        records_to_insert.append((
            tweet.get('id'),
            tweet.get('text'),
            tweet.get('url'),
            tweet.get('author', {}).get('name'),
            tweet.get('author', {}).get('url'),
            created_at_iso,
            json.dumps(tweet)
        ))

    cursor.executemany("""
        INSERT OR IGNORE INTO tweets (
            id, text, tweet_url, author_name, author_url, created_at_utc, full_data_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, records_to_insert)
    
    conn.commit()
    print(f"Attempted to insert {len(records_to_insert)} tweets. {cursor.rowcount} were newly added.")
    conn.close()

# We will need functions to get data for the correlation engine later.
# Let's stub them out now.

def get_unprocessed_tweets() -> List[sqlite3.Row]:
    """Fetches all tweets that have not yet been processed."""
    conn = get_db_connection()
    # Also fetch the embedding if we've pre-calculated it
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tweets WHERE is_processed = 0")
    tweets = cursor.fetchall()
    conn.close()
    return tweets


def get_active_market_data() -> List[sqlite3.Row]:
    """Fetches all active markets with their ID, embedding, question, and embedding_text."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, embedding, question, embedding_text, slug, yes_price, no_price, image_url, full_data_json 
        FROM markets 
        WHERE is_active = 1 AND embedding IS NOT NULL
    """)
    markets = cursor.fetchall()
    conn.close()
    return markets

def mark_tweet_as_processed(tweet_id: str):
    """Updates a tweet's status to processed."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE tweets SET is_processed = 1 WHERE id = ?", (tweet_id,))
    conn.commit()
    conn.close()


def store_correlation(tweet_id: str, market_id: str, relevance_score: float, relevance_score_reasoning: str, urgency_score: float, urgency_score_reasoning:str ): # Add reasoning
    """Stores a successful correlation in the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO sent_correlations (tweet_id, market_id, relevance_score, relevance_score_reasoning, urgency_score, urgency_score_reasoning) 
        VALUES (?, ?, ?, ?, ?, ?)
    """, (tweet_id, market_id, relevance_score, relevance_score_reasoning, urgency_score, urgency_score_reasoning)) # Pass reasoning
    conn.commit()
    conn.close()


def get_unsent_correlations() -> List[sqlite3.Row]:
    """Fetches all correlations that haven't been sent to Discord yet, ordered by relevance."""
    conn = get_db_connection()
    cursor = conn.cursor()
    # We want to send the highest-relevance correlations first.
    # JOINing with tweets and markets tables lets us fetch all data in one go!
    cursor.execute("""
        SELECT
            sc.id as correlation_id,
            sc.relevance_score,
            sc.relevance_score_reasoning,
            sc.urgency_score,
            sc.urgency_score_reasoning,
            t.id as tweet_id,
            t.text as tweet_text,
            t.tweet_url,
            t.embedding as tweet_embedding, -- ADD THIS
            t.author_name,
            m.question as market_question,
            m.id as market_id,  -- <<< THIS IS THE FIX. WE NEED THE MARKET'S ID.
            m.slug as market_slug,
            m.market_url as market_url,
            m.parent_event_id,
            m.yes_price,
            m.no_price,
            m.image_url as market_image
        FROM sent_correlations sc
        JOIN tweets t ON sc.tweet_id = t.id
        JOIN markets m ON sc.market_id = m.id
        WHERE sc.sent_to_discord = 0
        ORDER BY sc.relevance_score DESC, t.created_at_utc DESC
    """)
    correlations = cursor.fetchall()
    conn.close()
    return correlations

# Add a function to update market prices in the DB
def update_market_prices(market_id: str, yes_price: float, no_price: float):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE markets SET yes_price = ?, no_price = ? WHERE id = ?
    """, (yes_price, no_price, market_id))
    conn.commit()
    conn.close()

def mark_correlation_as_sent(correlation_id: int):
    """Updates a correlation's status to sent and records the timestamp."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE sent_correlations 
        SET sent_to_discord = 1, sent_at_utc = CURRENT_TIMESTAMP 
        WHERE id = ?
    """, (correlation_id,))
    conn.commit()
    conn.close()