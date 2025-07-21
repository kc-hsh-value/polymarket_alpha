import itertools
import json
import os
import discord
from dotenv import load_dotenv
from typing import List, Dict, Any
import asyncio
import sys

from helpers.correlation_engine import calculate_cosine_similarity
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from .database import get_all_active_channel_ids, get_unsent_correlations, mark_correlation_as_sent, update_market_prices
from .polymarket import get_markets_by_ids_async, get_markets_by_ids_sync


CHECK_INTERVAL_SECONDS = 7200 
load_dotenv()

DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
TARGET_CHANNEL_ID = int(os.getenv("TARGET_CHANNEL_ID", 0))

def format_correlation_embed(correlation: Dict[str, Any]) -> discord.Embed:
    """Formats a correlation into a rich Discord embed."""
    # This function is perfect, no changes needed here.
    market_url = correlation['market_url']
    embed = discord.Embed(
        title=f"📈 Market: {correlation['market_question']}",
        url=market_url,
        color=discord.Color.blue()
    )
    embed.add_field(
        name=f"🐦 New Tweet from @{correlation['author_name']}",
        value=f"> {correlation['tweet_text']}\n[View Tweet]({correlation['tweet_url']})",
        inline=False
    )
    embed.add_field(
        name="🧠 Analyst's Reasoning",
        value=f"_{correlation['reasoning']}_",
        inline=False
    )
    yes_price = correlation['yes_price'] * 100
    no_price = correlation['no_price'] * 100
    embed.add_field(
        name="📊 Current Odds",
        value=f"**Yes:** {yes_price:.1f}%  |  **No:** {no_price:.1f}%",
        inline=True
    )
    embed.add_field(
        name="🎯 Relevance Score",
        value=f"**{correlation['relevance_score']:.2f} / 1.0**",
        inline=True
    )
    if correlation.get('market_image'):
        embed.set_thumbnail(url=correlation['market_image'])
    embed.set_footer(text="PolyMarket Alpha Bot")
    return embed

async def format_grouped_correlation_embed(tweet_data: Dict, market_group: List[Dict]) -> discord.Embed:
    parent_market = market_group[0]
    embed = discord.Embed(
        title=f"🐦 New Tweet from @{tweet_data['author_name']}",
        description=f"> {tweet_data['tweet_text']}\n[View Tweet]({tweet_data['tweet_url']})",
        color=discord.Color.blue()
    )
    embed.add_field(
        name=f"📈 Top Correlated Market: {parent_market['market_question']}",
        value=f"[View Market]({parent_market['market_url']}) | **Yes:** {parent_market['yes_price']*100:.1f}%",
        inline=False
    )
    embed.add_field(
        name="🧠 Analyst's Reasoning",
        value=f"_{parent_market['relevance_score_reasoning']}_",
        inline=False
    )
    if len(market_group) > 1:
        other_markets_text = []
        for other_market in market_group[1:5]:
        # for other_market in market_group:
             other_markets_text.append(f"• [{other_market['market_question']}]({other_market['market_url']}) ({other_market['yes_price']*100:.1f}%)")
        if other_markets_text:
             embed.add_field(
                name="🔗 Other Related Markets",
                value="\n".join(other_markets_text),
                inline=False
            )
    if parent_market.get('market_image'):
        embed.set_thumbnail(url=parent_market['market_image'])
    embed.set_footer(text="PolyMarket Alpha Bot")
    return embed


async def send_new_correlations():
    """The main function to fetch, group, format, and send new correlations."""
    if not TARGET_CHANNEL_ID or not DISCORD_BOT_TOKEN:
        print("Error: DISCORD_BOT_TOKEN or TARGET_CHANNEL_ID not set in .env file.")
        return

    print("Checking for new correlations to send to Discord...")
    # 1. Fetch ALL unsent correlation rows from the database
    # 1. Fetch ALL unsent correlation rows from the database
    unsent_correlation_rows = get_unsent_correlations()
    if not unsent_correlation_rows:
        print("No new correlations to send.")
        return
    correlations = [dict(row) for row in unsent_correlation_rows]
    print(f"Found {len(correlations)} correlation rows to process.")

    # 2. FEATURE: Refresh odds with Real-Time Data
    unique_market_ids = list({c['market_id'] for c in correlations})
    latest_market_data = await get_markets_by_ids_async(unique_market_ids)
    
    for c in correlations:
        if c['market_id'] in latest_market_data:
            latest_info = latest_market_data[c['market_id']]
            prices = json.loads(latest_info.get('outcomePrices', '[]'))
            if len(prices) >= 2:
                c['yes_price'] = float(prices[0])
                c['no_price'] = float(prices[1])
                update_market_prices(c['market_id'], c['yes_price'], c['no_price'])

    # 3. Group correlations by tweet_id to create "message packages"
    correlations.sort(key=lambda x: x['tweet_id'])
    message_packages = [list(group) for _, group in itertools.groupby(correlations, key=lambda x: x['tweet_id'])]

    # 4. Calculate an "Alpha Score" for each message package
    scored_packages = []
    for package in message_packages:
        package.sort(key=lambda x: x['relevance_score'], reverse=True)
        top_correlation = package[0]
        relevance = top_correlation['relevance_score']
        urgency = top_correlation['urgency_score']

         # Using the new "Market Liveness" formula
        impact = 4 * top_correlation['yes_price'] * top_correlation['no_price']

        diversity_bonus = min(len(package) / 5.0, 1.0) * 0.1
        alpha_score = (relevance * 0.5) + (urgency * 0.3) + (impact * 0.2) + diversity_bonus
        scored_packages.append({'score': alpha_score, 'package': package})

    # 5. Prioritize: Sort the packages by the calculated Alpha Score
    scored_packages.sort(key=lambda x: x['score'], reverse=True)
    print(f"\nCalculated Alpha Scores and prioritized {len(scored_packages)} potential messages.")

    # 6. FEATURE: De-duplicate news from different sources
    final_packages_to_send = []
    processed_tweet_embeddings = []
    SIMILARITY_THRESHOLD = 0.95

    for package_info in scored_packages:
        # We need the tweet embedding, make sure it's in your get_unsent_correlations() query!
        current_tweet_embedding = package_info['package'][0].get('tweet_embedding')
        if current_tweet_embedding is None: # Safety check
            final_packages_to_send.append(package_info)
            continue

        is_duplicate = False
        for processed_embedding in processed_tweet_embeddings:
            similarity = calculate_cosine_similarity(current_tweet_embedding, processed_embedding)
            if similarity > SIMILARITY_THRESHOLD:
                is_duplicate = True
                print(f"Found duplicate news event. Discarding tweet {package_info['package'][0]['tweet_id']}.")
                break
        
        if not is_duplicate:
            final_packages_to_send.append(package_info)
            processed_tweet_embeddings.append(current_tweet_embedding)
        
    
    print(f"\nAfter de-duplication, {len(final_packages_to_send)} unique messages will be sent.")

    # --- BROADCASTING LOGIC ---
    subscribed_channel_ids = get_all_active_channel_ids()
    if not subscribed_channel_ids:
        print("No subscribed channels to send messages to.")
        # Mark correlations as "sent" even if no one is subscribed to avoid resending forever
        for item in final_packages_to_send:
            for c in item['package']:
                mark_correlation_as_sent(c['correlation_id'])
        return 0
    # 7. Connect to Discord and send the final list in priority order
    num_messages = len(final_packages_to_send)
    delay = (CHECK_INTERVAL_SECONDS / 2) / (num_messages + 1) if num_messages > 0 else 0
    print(f"Sending messages with a delay of {delay:.1f} seconds between each.")
    
    # The Discord sending logic remains the same, but loops over `final_packages_to_send`
    total_sent_count = 0
    intents = discord.Intents.default()
    async with discord.Client(intents=intents) as client:
        try:
            # ... (login logic) ...
            await client.login(DISCORD_BOT_TOKEN)
            channel = await client.fetch_channel(TARGET_CHANNEL_ID)
            # ... (check channel) ...
            
            for item in final_packages_to_send:
                tweet_group = item['package']
                # ... (rest of the sending logic from before) ...
                tweet_data = tweet_group[0]
                # --- THE FIX IS HERE ---
                # Before we do the inner grouping, let's calculate the impact score
                # for every market in the group.
                for market in tweet_group:
                    market['impact_score'] = 4 * market['yes_price'] * market['no_price']
                
                # Now, use a hybrid key to sort the entire group.
                # We prioritize relevance heavily, but use impact as a powerful tie-breaker
                # and to penalize dead markets.
                tweet_group.sort(
                    key=lambda m: (m['relevance_score'], m['impact_score']),
                    reverse=True
                )

                final_market_list = []
                # ... (inner grouping logic) ...

                markets_with_parent = [m for m in tweet_group if m['parent_event_id']]
                markets_without_parent = [m for m in tweet_group if not m['parent_event_id']]
                if markets_with_parent:
                    markets_with_parent.sort(key=lambda x: x['parent_event_id'])
                    for parent_id, market_group_iter in itertools.groupby(markets_with_parent, key=lambda x: x['parent_event_id']):
                        market_group = list(market_group_iter)
                        market_group.sort(key=lambda x: x['relevance_score'], reverse=True)
                        final_market_list.append(market_group[0])
                final_market_list.extend(markets_without_parent)
                final_market_list.sort(key=lambda x: x['relevance_score'], reverse=True)

                # --- FEATURE: JUST-IN-TIME PRICE REFRESH ---
                # Before formatting the embed, get the absolute latest prices for THIS message
                market_ids_to_refresh = [m['market_id'] for m in final_market_list]
                fresh_data = await get_markets_by_ids_async(market_ids_to_refresh)
                # Update the final_market_list in-place with the freshest odds
                for market_dict in final_market_list:
                    if market_dict['market_id'] in fresh_data:
                        latest_info = fresh_data[market_dict['market_id']]
                        prices_str = latest_info.get('outcomePrices', '[]')
                        try:
                            prices = json.loads(prices_str)
                            if len(prices) >= 2:
                                market_dict['yes_price'] = float(prices[0])
                                market_dict['no_price'] = float(prices[1])
                        except (json.JSONDecodeError, IndexError, TypeError):
                            continue
                print(f"  - Refreshed prices for {len(market_ids_to_refresh)} markets for Tweet ID {tweet_data['tweet_id']}.")

                embed = await format_grouped_correlation_embed(tweet_data, final_market_list)
                for channel_id in subscribed_channel_ids:
                    try:
                        channel = client.get_channel(channel_id) or await client.fetch_channel(channel_id)
                        if channel and isinstance(channel, (discord.TextChannel, discord.Thread)):
                            await channel.send(embed=embed)
                    except discord.errors.Forbidden:
                        print(f"  - No permission to send to channel {channel_id}. Consider removing this subscription.")
                    except discord.errors.NotFound:
                        print(f"  - Channel {channel_id} not found. Consider removing this subscription.")
                    except Exception as e:
                        print(f"  - An error occurred sending to channel {channel_id}: {e}")
                
                # Mark as sent once, after broadcasting to all
                # if isinstance(channel, (discord.TextChannel, discord.Thread)):
                #     await channel.send(embed=embed)
                
                correlation_ids_to_mark = [c['correlation_id'] for c in tweet_group]
                for c_id in correlation_ids_to_mark:
                    mark_correlation_as_sent(c_id)
                total_sent_count += 1
                await asyncio.sleep(delay)


        except discord.errors.LoginFailure:
            print("Error: Failed to log in to Discord. Check your bot token.")
        except Exception as e:
            print(f"An error occurred during Discord sending: {e}")
            import traceback
            traceback.print_exc()

    print("Finished sending correlations and logged out from Discord.")
    return len(final_packages_to_send) # <-- Correct return value