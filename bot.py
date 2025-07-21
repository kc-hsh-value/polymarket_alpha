# bot.py
import discord
import os
import asyncio
from dotenv import load_dotenv

from main import alpha_cycle_loop # We will refactor main.py into this
from helpers.database import setup_database, add_subscription
from helpers.seed import seed_database_if_empty

load_dotenv()
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")

# --- Bot Class Definition ---
# This structure manages the persistent connection and commands
class PolyMarketBot(discord.Client):
    def __init__(self, *, intents: discord.Intents):
        super().__init__(intents=intents)
        # CommandTree is the modern way to handle slash commands
        self.tree = discord.app_commands.CommandTree(self)

    async def setup_hook(self):
        # This copies the global commands to your guild.
        await self.tree.sync()

    async def on_ready(self):
        if self.user:
            print(f'Logged in as {self.user} (ID: {self.user.id})')
        else:
            print('Logged in, but self.user is None')
        print('------')
        self.loop.create_task(alpha_cycle_loop(self))

# --- Bot Instance and Intents ---
intents = discord.Intents.default()
bot = PolyMarketBot(intents=intents)

# --- Slash Command Definition ---
@bot.tree.command(name="setup", description="Set this channel to receive PolyMarket Alpha alerts.")
async def setup(interaction: discord.Interaction):
    """Command to subscribe a channel to alerts."""

    # --- THE FIX IS HERE ---
    # 1. Check if the command is being run in a server.
    #    interaction.guild will be None if it's a DM.
    if not interaction.guild or not interaction.channel:
        await interaction.response.send_message(
            "This command can only be used inside a server channel.", 
            ephemeral=True
        )
        return

    # 2. Check that the channel is a mentionable text channel.
    #    This satisfies the type checker for the .mention attribute.
    if not isinstance(interaction.channel, (discord.TextChannel, discord.Thread)):
         await interaction.response.send_message(
            "This command can only be used in a standard text channel or thread.", 
            ephemeral=True
        )
         return
    
    # --- The rest of your logic can now run safely ---
    # The type checker is now happy because it knows .guild and .channel exist
    # and that .channel has a .mention attribute.
    server_id = str(interaction.guild.id)
    channel_id = str(interaction.channel.id)
    
    try:
        add_subscription(server_id, channel_id)
        await interaction.response.send_message(
            f"✅ **Success!** This channel ({interaction.channel.mention}) will now receive alpha alerts.",
            ephemeral=True
        )
        print(f"New subscription added: Server ID {server_id}, Channel ID {channel_id}")
    except Exception as e:
        await interaction.response.send_message(
            f"❌ **Error!** Could not save subscription. Please contact the bot admin. Error: {e}",
            ephemeral=True
        )

# --- Initial Setup and Run ---
if __name__ == '__main__':
    print("Performing initial setup...")
    setup_database()
    seed_database_if_empty()
    print("Initial setup complete. Starting bot...")
    if not DISCORD_BOT_TOKEN:
        raise ValueError("DISCORD_BOT_TOKEN is not set in the environment.")
    bot.run(DISCORD_BOT_TOKEN)