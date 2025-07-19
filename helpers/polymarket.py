import requests
from typing import List, Dict, Any
import time
from datetime import datetime, timezone
import httpx  # Import the async-capable HTTP library
import asyncio # Import Python's core async library

def get_markets(end_date_min: str | None = None, start_date_min: str | None = None, ) -> List[Dict[str, Any]]:
    """
    Fetches all active markets from the Polymarket API using pagination.
    The function dynamically gets markets that have not yet ended
    by using the current UTC time as the minimum end date.

    Args:
        start_date_min: The minimum end date to fetch markets for.
        end_date_max: The maximum end date to fetch markets for.

    Returns:
        A list of markets.
    """
    all_markets: List[Dict[str, Any]] = []
    offset = 0
    PAGE_SIZE = 500  # The maximum number of items the API returns per page

    try:
        
        print("--- Starting to fetch all active Polymarket markets ---")

        while True:
            # Construct the URL with pagination parameters
            if start_date_min and end_date_min:
                url = f"https://gamma-api.polymarket.com/markets?start_date_min={start_date_min}&end_date_min={end_date_min}&limit={PAGE_SIZE}&offset={offset}"
            elif end_date_min and not start_date_min:
                url = f"https://gamma-api.polymarket.com/markets?end_date_min={end_date_min}&limit={PAGE_SIZE}&offset={offset}"
            elif start_date_min and not end_date_min:
                url = f"https://gamma-api.polymarket.com/markets?start_date_min={start_date_min}&limit={PAGE_SIZE}&offset={offset}"
            else:
                url = f"https://gamma-api.polymarket.com/markets?limit={PAGE_SIZE}&offset={offset}"
            print(f"Fetching page: {url}")

            response = requests.get(url)
            response.raise_for_status()  # Raise an HTTPError for bad responses

            page_data = response.json()
            
            if not page_data:
                # If the API returns an empty list, we're done.
                print("  -> Received empty page. Finished fetching.")
                break

            all_markets.extend(page_data)
            print(f"  -> Found {len(page_data)} markets on this page. Total so far: {len(all_markets)}")

            if len(page_data) < PAGE_SIZE:
                # If the number of items returned is less than our page size,
                # it means we've reached the last page.
                print("  -> Reached the last page of results.")
                break

            # Prepare for the next iteration
            offset += PAGE_SIZE
            time.sleep(0.5) # Be respectful to the API, add a small delay

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching markets: {e}")
        return [] # Return whatever we have, or an empty list on failure
    
    print(f"--- Finished fetching. Total active markets found: {len(all_markets)} ---")
    return all_markets

# markets = get_markets()
# print(markets)



# In helpers/polymarket.py - Simple version
def get_markets_by_ids_sync(market_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    all_market_data = {}
    print(f"Fetching latest data for {len(market_ids)} markets one by one...")
    for market_id in market_ids:
        try:
            url = f"https://gamma-api.polymarket.com/markets?id={market_id}"
            response = requests.get(url)
            response.raise_for_status()
            market_data = response.json()
            # The API returns a list even for one ID, so we take the first element
            if market_data:
                all_market_data[market_id] = market_data[0]
            time.sleep(0.2) # Small delay to be nice to the API
        except Exception as e:
            print(f"  - Could not fetch market {market_id}: {e}")
            continue
    return all_market_data



async def get_markets_by_ids_async(market_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Fetches the latest data for a list of specific market IDs concurrently using asyncio.
    This is much faster than making requests one by one.
    """
    if not market_ids:
        return {}
    
    all_market_data = {}
    print(f"Fetching latest data for {len(market_ids)} markets concurrently...")

    # httpx.AsyncClient allows us to make many requests over a shared connection pool.
    # The 'async with' block ensures the client is properly closed.
    async with httpx.AsyncClient() as client:
        
        # 1. Create a list of "tasks". Each task is a coroutine that represents
        #    one API call. We don't 'await' them yet.
        tasks = []
        for market_id in market_ids:
            url = f"https://gamma-api.polymarket.com/markets?id={market_id}"
            tasks.append(client.get(url))
        
        # 2. Run all the tasks concurrently using asyncio.gather.
        #    The '*' unpacks our list of tasks into arguments for the function.
        #    'return_exceptions=True' is crucial: if one request fails, it won't
        #    crash the entire batch. The exception will be returned in its place.
        responses = await asyncio.gather(*tasks, return_exceptions=True)

        # 3. Process the results. The 'responses' list is in the same order as our 'tasks' list.
        for i, response in enumerate(responses):
            market_id = market_ids[i] # Match the response to its original ID by index

            # Check if the request was successful and is a valid response object
            if isinstance(response, httpx.Response) and response.status_code == 200:
                try:
                    market_data = response.json()
                    # The API returns a list, even for one ID, so we take the first element
                    if market_data:
                        all_market_data[market_id] = market_data[0]
                except Exception as e:
                     print(f"  - Error parsing JSON for market {market_id}: {e}")
            else:
                # This will handle network errors or bad status codes (e.g., 404, 500)
                print(f"  - Could not fetch market {market_id}. Reason: {response}")

    print(f"Successfully fetched data for {len(all_market_data)} out of {len(market_ids)} markets.")
    return all_market_data