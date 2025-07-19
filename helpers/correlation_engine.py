from datetime import datetime, timezone
from typing import List, Dict, Any, TypedDict, Optional, Callable, cast
from langchain.chat_models import init_chat_model
from dotenv import load_dotenv
import os
from langchain.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field, ValidationError
import json
import numpy as np

from helpers.database import get_active_market_data, get_unprocessed_tweets, mark_tweet_as_processed, store_correlation
from helpers.embeddings import generate_embeddings  

load_dotenv()

class LLMMarketResponse(BaseModel):
    id: str = Field(description="The unique ID of the market.")
    question: str = Field(description="The question of the market.")
    # No need for description here, as the LLM has already seen it.

# This defines the structure for one scored correlation.
class ScoredCorrelation(BaseModel):
    market: LLMMarketResponse
    relevance_score: float = Field(description="A score from 0.0 to 1.0 indicating relevance.")
    relevance_score_reasoning: str = Field(description="A brief, one-sentence justification for the relevance score.")
    urgency_score: float = Field(description="A score from 0.0 to 1.0 indicating urgency.")
    urgency_score_reasoning: str = Field(description="A brief, one-sentence justification for the urgency score.")

# This is the top-level object the LLM must return.
class ValidatedCorrelations(BaseModel):
    """A list of markets that have been validated as truly relevant to the tweet."""
    correlations: List[ScoredCorrelation]

model = init_chat_model(
        "gemini-2.5-flash-lite-preview-06-17", 
        model_provider="google_genai",
        google_api_key=os.getenv("GEMINI_API_KEY"),
        temperature=0.5
    )

structured_model = model.with_structured_output(ValidatedCorrelations)


prompt = ChatPromptTemplate.from_template(
"""
You are an expert financial and geopolitical analyst. Your mission is to analyze a breaking news tweet and evaluate its connection to a list of potential prediction markets.

You will provide two scores for each relevant market:
1.  **Relevance Score:** How strong is the connection between the tweet's information and the market's question?
2.  **Urgency Score:** How quickly could this news impact the market? Is this actionable, breaking news?

---
**INSTRUCTIONS**

1.  **Analyze the Tweet:** Understand the core news being reported.
2.  **Evaluate Candidate Markets:** For each market provided, assess its relevance and urgency.
3.  **Provide Scores and Reasoning:** For every market you find relevant (relevance > 0.1), you MUST provide all four fields: `relevance_score`, `relevance_score_reasoning`, `urgency_score`, and `urgency_score_reasoning`.

---
**SCORING GUIDELINES: RELEVANCE**
- **0.9-1.0 (Direct):** The tweet is a direct, unambiguous statement about the market's specific question.
- **0.7-0.8 (Strongly Causal):** The tweet is about a key driver or a closely related entity that will almost certainly affect the market. (e.g., a "USDC" tweet for a "Circle" market).
- **0.5-0.6 (Thematic):** The tweet and market are in the same general topic area and could influence each other.
- **0.2-0.4 (Weak):** A tenuous or speculative connection.

**SCORING GUIDELINES: URGENCY**
- **0.9-1.0 (Immediate):** Breaking news that could move the market within minutes or hours. (e.g., "Israel attacks Iran," "Fed announces surprise rate cut").
- **0.7-0.8 (High):** Significant development that will likely impact the market soon. (e.g., "CEO resigns," "Phase 3 trial results are positive").
- **0.5-0.6 (Moderate):** Important background information or developing news that adds to a narrative. (e.g., "Polls show candidate gaining," "Inflation report is slightly higher than expected").
- **0.2-0.4 (Low):** General commentary or analysis; not time-sensitive.
- **0.0-0.1 (None):** The news has no time-based component.

---
<TWEET>
{tweet_json}
</TWEET>

---
<CANDIDATE_MARKETS>
{markets_json}
</CANDIDATE_MARKETS>
"""
)

# Create the chain using LangChain Expression Language (LCEL)
# This pipes the output of the prompt to the model, and the model's output to the parser
chain = prompt | structured_model

# A helper for cosine similarity
def calculate_cosine_similarity(v1, v2):
    dot_product = np.dot(v1, v2)
    norm_v1 = np.linalg.norm(v1)
    norm_v2 = np.linalg.norm(v2)
    if norm_v1 == 0 or norm_v2 == 0: return 0.0
    return dot_product / (norm_v1 * norm_v2)

def run_correlation_engine():
    print("--- Running Correlation Engine ---")
    
    # 1. Get work to do
    unprocessed_tweets = get_unprocessed_tweets()
    if not unprocessed_tweets:
        print("No new tweets to process.")
        return

    # 2. Get the search space (all active markets)
    active_markets = get_active_market_data()
    market_embeddings = {market['id']: market['embedding'] for market in active_markets}
    markets_by_id = {market['id']: dict(market) for market in active_markets}

    print(f"Processing {len(unprocessed_tweets)} new tweets against {len(active_markets)} active markets.")

    for tweet in unprocessed_tweets:
        tweet_id = tweet['id']
        tweet_text = tweet['text']
        print(f"\nProcessing Tweet ID: {tweet_id}")

        # 3. Generate embedding for the current tweet
        tweet_embedding = generate_embeddings([tweet_text])[0]
        if tweet_embedding.size == 0:
            print("  - Could not generate embedding for tweet. Marking as processed.")
            mark_tweet_as_processed(tweet_id)
            continue
            
        # 4. Stage 1: Fast Semantic Search
        scored_candidates = []
        for market_id, market_embedding in market_embeddings.items():
            score = calculate_cosine_similarity(tweet_embedding, market_embedding)
            scored_candidates.append((score, market_id))

        scored_candidates.sort(key=lambda x: x[0], reverse=True)
        
        # Take the top N candidates for LLM refinement
        TOP_N_CANDIDATES = 50
        top_candidates_ids = [market_id for score, market_id in scored_candidates[:TOP_N_CANDIDATES]]
        
        # Prepare the data for the LLM
        markets_for_llm = []
        for market_id in top_candidates_ids:
            market_data = markets_by_id[market_id]
            markets_for_llm.append({
                "id": market_data['id'],
                "question": market_data['question'],
                "embedding_text": market_data['embedding_text']
            })

        # 5. Stage 2: LLM Refinement and Scoring
        print(f"  - Found {len(markets_for_llm)} candidates. Sending to LLM for final validation...")
        try:
            # The chain and prompt need to be updated for this new role
            raw_response = chain.invoke({
                "markets_json": json.dumps(markets_for_llm, indent=2),
                "tweet_json": json.dumps({"text": tweet_text})
            })
            
            response_model = cast(ValidatedCorrelations, raw_response)
            if not response_model.correlations:
                print("  - LLM validated no relevant markets.")
            else:
                print(f"  - LLM validated {len(response_model.correlations)} relevant markets!")
                for correlation in response_model.correlations:
                    if correlation.relevance_score >= 0.6:
                        print(f"    - Storing correlation for Market {correlation.market.id} (Score: {correlation.relevance_score}). Reason: {correlation.relevance_score_reasoning}")
                        store_correlation(
                            tweet_id=tweet_id,
                            market_id=correlation.market.id,
                            relevance_score=correlation.relevance_score,
                            relevance_score_reasoning=correlation.relevance_score_reasoning,
                            urgency_score=correlation.urgency_score,
                            urgency_score_reasoning=correlation.urgency_score_reasoning
                        )
                    else: 
                        # print(f"the correlation score between tweet {tweet_id} and market: {correlation.market.id} is {correlation.relevance_score} < 0.6")
                        print("")
        except Exception as e:
            print(f"  !! Error during LLM processing for tweet {tweet_id}: {e}")

        # 7. Mark tweet as processed, regardless of outcome
        mark_tweet_as_processed(tweet_id)

    print("--- Correlation Engine Finished ---")
