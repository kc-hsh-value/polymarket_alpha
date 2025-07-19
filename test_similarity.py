import numpy as np
import sqlite3
from typing import List, Tuple
from helpers.database import get_db_connection, convert_array
from helpers.embeddings import generate_embeddings
from helpers.database import DB_FILE 

# We need to register the converter again in this script's scope
sqlite3.register_converter("array", convert_array)

def calculate_cosine_similarity(v1, v2):
    """Calculates the cosine similarity between two vectors."""
    # Ensure vectors are not zero to avoid division by zero
    if np.all(v1 == 0) or np.all(v2 == 0):
        return 0.0
    dot_product = np.dot(v1, v2)
    norm_v1 = np.linalg.norm(v1)
    norm_v2 = np.linalg.norm(v2)
    return dot_product / (norm_v1 * norm_v2)

def load_active_markets_from_db() -> List[Tuple[str, str, np.ndarray]]:
    """Loads all active markets and their embeddings from the database."""
    print("Loading active markets from database...")
    conn = sqlite3.connect(DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row # This is still good practice
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, question, embedding 
        FROM markets 
        WHERE is_active = 1 AND embedding IS NOT NULL
    """)
    markets = cursor.fetchall()
    conn.close()
    
    # The 'embedding' column is automatically converted to a numpy array
    # because we registered the converter.
    print(f"Loaded {len(markets)} active markets.")
    return [(row['id'], row['question'], row['embedding']) for row in markets]

def main():
    """Main interactive loop for testing similarity."""
    active_markets = load_active_markets_from_db()
    if not active_markets:
        print("No active markets found in the database. Please run the main bot script first.")
        return

    print("\n--- Interactive Similarity Search ---")
    print("Type a 'test tweet' and press Enter. Type 'exit' to quit.")

    while True:
        try:
            test_tweet = input("\n> Test Tweet: ")
            if test_tweet.lower() == 'exit':
                break
            if not test_tweet:
                continue

            # 1. Generate embedding for the input text
            test_embedding_list = generate_embeddings([test_tweet])
            if not test_embedding_list or test_embedding_list[0].size == 0:
                print("Could not generate embedding for the input text.")
                continue
            
            test_embedding = test_embedding_list[0]

            # 2. Calculate similarity against all market embeddings
            scored_markets = []
            for market_id, market_question, market_embedding in active_markets:
                if market_embedding is not None and market_embedding.size > 0:
                    score = calculate_cosine_similarity(test_embedding, market_embedding)
                    scored_markets.append((score, market_question, market_id))

            # 3. Sort by score (descending) and show the top 5
            scored_markets.sort(key=lambda x: x[0], reverse=True)

            print("\n--- Top 5 Similar Markets ---")
            for i, (score, question, market_id) in enumerate(scored_markets[:5]):
                print(f"{i+1}. Score: {score:.4f} | ID: {market_id} | Question: {question}")

        except (KeyboardInterrupt, EOFError):
            break

    print("\nExiting similarity tester.")


if __name__ == "__main__":
    main()