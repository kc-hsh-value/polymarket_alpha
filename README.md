# Polymarket Alpha


## Goals

Create a bot that is able to correlate breaking news (currently only tweets) with polymarket events (and consequently markets). 
We should first create a discord bot
Then we will scale it into a full stack app which will also allow trading. 

## Implementation

For the implementation I began by having a simple strategy in my mind: we would fetch all the markets, we would fetch all the latest tweets and then we will tell the LLM to create the correlations. Then we will post it on discord. 
In order to achieve the goal above we setup a new python project. I have called it "x". 
x. has a main.py which is the function that runs the whole process described above. Let's split the process into 4 parts. 

1. Fetching the markets
2. Fetching the tweets
3. Corelating the markets and the tweets
4. Sending the tweet-market(s) correlation(s) on discord

For a more solid approach we have create an sqlite database. This database holds tweets, markets and sent_correlations. To handle the databae actions we have create the module helpers/database.py. There we have the database initiation code and the CRUD actions. So let's go back to the main module. 
### 1. Fetching the markets
First of all we call the function setup_database in order to create the aforementioned tables if they don't already exist. 
if the markets table is empty:
we call the function seed_database which basically fetches all the markets using polymarket API endpoints. The markets we fetch are the ones that have minimum end data the current time. Which means only the active markets. 
The markets are fetched using the get_markets function that exists in the helpoers/polymarket.py module. 
if the markets table isn't empty: 
we PRUNE the database by removing all the markets that have end_date BEFORE now. Then we fetch the new markets that got created between the last time the function got called and now (so basically the 2 hour time frame) and add them to the markets table. 

For both insert actions, we embed the title and the description of the market in order to create a vector representation of the semantic meaning, for future similarity search against the tweets

### 2. Fetching the tweets
Then in the main function what we do is fetch the tweets that got tweeted within the last 2 hour time frame. We use the function get_tweets(hours_ago:number) from helpers/x.py. In this module we use the twitterapi.io which allows us to get tweets from specific profiles in the time frame specified. Then we save them in the tweets table, with the column is_processed being 0, signaling that the tweets haven't been used in order to find  correlated markets. 

### 3. Correlating markets and tweets
For this step we have defined the module helpers/correlation_engine.py. This module contains the run_correlation_engine function. This function goes through the tweets table and finds the tweets with is_processed = 0 and then get all the active markets from the markets table. 
We iterate through the tweets and perform similarity search against the markets. we get the top 50 candidate markets for each tweet, and the tweet it self and pass them both to an llm (gemini-2.5 flash) in order to return a structured output containing a structured ouput containing individual corrrelations between one tweet and one market with details such as relevance_score and urgency_score. if the relevance_score is <0.6 we continue else we SAVE in the sent_correlations table with the sent_to_discord binary flag set to 0 and for the tweet we set the is_processed to 1. 

So in this step, we first perform similarity search for each tweet against all the markets, and for the top 50 similar markets ,we instruct an llm to tell if for each individual if it is relevant and urgent enough. then we save it to the sent_correlations table. 

### 4. Sending the tweet - market(s) correlation
For the final step we call the function send_new_correlations from helpers/discord.py. In this function we get all the correaltions with sent_to_discord being 0 and we first get all the markets in these correlations. For each market we update the yes/no share price and then we group the correlations by tweet id. So now we have tweet => market(s) structure. 
For each array of markets of an individual tweet we calculate an alpha score based on the relevance, the urgency (extracted by the llm for the individual market-tweet correlation) as well as the yes/no share price and the number of markets per tweet. Then we check for similar tweets and we remove if there are any duplicates. Finally based on the alpha score we schedule the tweet-market(s) correltions to be sent to discord within the 2 hour frame. 


