# Standard library imports
import os
import time
from datetime import datetime, UTC
import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor  # <-- ADDED THIS LINE

# Local application imports
from database import init_db, save_data as db_save_data, load_data as db_load_data
from logger import bot_logger as logger
import config

# Load environment variables from .env file
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

# Ensure DISCORD_TOKEN is loaded
if not TOKEN:
    logger.error("Error: DISCORD_TOKEN not found in environment variables. Please check your .env file.")
    exit()

INITIAL_EXTENSIONS = [
    'cogs.admin',
    'cogs.tasks'
]


class MyBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.reactions = True
        intents.presences = True

        super().__init__(
            command_prefix="!",
            intents=intents,
            case_insensitive=True
        )

        # We'll use this db_executor for all database operations
        # It's defined here and passed to our database functions
        self.db_executor = ThreadPoolExecutor()

        # The new, non-blocking database functions are assigned to the bot instance
        self.init_db = init_db
        self.load_data = db_load_data
        self.save_data = db_save_data

        # Initialize these as empty until we can load them in an async function
        self.user_points = {}
        self.submissions = {}
        self.vip_posts = {}
        self.user_xp = {}
        self.weekly_quests = {}
        self.quest_submissions = {}
        self.gm_log = {}
        self.admin_points = {}
        self.referral_data = {}
        self.pending_referrals = {}
        self.active_tickets = {}
        self.mysterybox_uses = {}
        self.bot_data = {}
        self.approved_proofs = []
        self.points_history = []
        self.giveaway_winners_log = []
        self.all_time_giveaway_winners_log = []
        self.referred_users = set()
        self.processed_reactions = set()

        self.invite_cache = {}
        self.invites_before_join = {}
        self.ticket_messages_to_archive = {}

    # --- THIS FUNCTION NOW HANDLES ALL DATA LOADING ---
    async def load_all_data_from_db(self):
        """
        Loads all bot data from the database.
        This must be called in an async context, like on_ready.
        """
        self.user_points = await self.load_data(self, "user_points_table", {})
        self.submissions = await self.load_data(self, "submissions_table", {})
        self.vip_posts = await self.load_data(self, "vip_posts_table", {})
        self.user_xp = await self.load_data(self, "user_xp_table", {})
        self.weekly_quests = await self.load_data(self, "weekly_quests_table", {"week": 0, "quests": []})
        self.quest_submissions = await self.load_data(self, "quest_submissions_table", {})
        self.gm_log = await self.load_data(self, "gm_log_table", {})
        self.admin_points = await self.load_data(self, "admin_points_table", {
            "total_supply": 10000000000.0,
            "balance": 10000000000.0,
            "in_circulation": 0.0,
            "burned": 0.0,
            "my_points": 0.0,
            "treasury": 0.0
        })
        self.referral_data = await self.load_data(self, "referral_data_table", {})
        self.pending_referrals = await self.load_data(self, "pending_referrals_table", {})
        self.active_tickets = await self.load_data(self, "active_tickets_table", {})
        self.mysterybox_uses = await self.load_data(self, "mysterybox_uses_table", {})
        self.bot_data = await self.load_data(self, "bot_data_table", {})
        self.approved_proofs = await self.load_data(self, "approved_proofs_table", [])
        self.points_history = await self.load_data(self, "points_history_table", [])
        self.giveaway_winners_log = await self.load_data(self, "giveaway_logs_table", [])
        self.all_time_giveaway_winners_log = await self.load_data(self, "all_time_giveaway_logs_table", [])
        self.referred_users = set(await self.load_data(self, "referred_users_table", []))
        self.processed_reactions = set(await self.load_data(self, "processed_reactions_table", []))

        logger.info("✅ All bot data loaded from the database.")

    async def save_all_data_to_db(self):
        """
        Saves all bot data to the database.
        This is a helper function to be called by the periodic task or commands.
        """
        try:
            await self.save_data(self, "points_history_table", self.points_history)
            await self.save_data(self, "giveaway_logs_table", self.giveaway_winners_log)
            await self.save_data(self, "all_time_giveaway_logs_table", self.all_time_giveaway_winners_log)
            await self.save_data(self, "gm_log_table", self.gm_log)
            await self.save_data(self, "user_points_table", self.user_points)
            await self.save_data(self, "admin_points_table", self.admin_points)
            await self.save_data(self, "submissions_table", self.submissions)
            await self.save_data(self, "quest_submissions_table", self.quest_submissions)
            await self.save_data(self, "weekly_quests_table", self.weekly_quests)
            await self.save_data(self, "referral_data_table", self.referral_data)
            await self.save_data(self, "approved_proofs_table", self.approved_proofs)
            await self.save_data(self, "user_xp_table", self.user_xp)
            await self.save_data(self, "bot_data_table", self.bot_data)
            await self.save_data(self, "vip_posts_table", self.vip_posts)
            await self.save_data(self, "pending_referrals_table", self.pending_referrals)
            await self.save_data(self, "active_tickets_table", self.active_tickets)
            await self.save_data(self, "mysterybox_uses_table", self.mysterybox_uses)

            await self.save_data(self, "processed_reactions_table", list(self.processed_reactions))
            await self.save_data(self, "referred_users_table", list(self.referred_users))

            logger.info("✅ All bot data saved to the database.")

        except Exception as e:
            logger.error(f"❌ An error occurred while saving all data: {e}", exc_info=True)

    # --- Helper Functions (these are now local and don't need 'self.bot' passed) ---
    def ensure_user(self, user_id: str):
        self.user_points.setdefault(user_id, {"all_time_points": 0.0, "available_points": 0.0})

    def get_user_balance(self, user_id: str) -> float:
        return self.user_points.get(user_id, {}).get("available_points", 0.0)

    def admin_can_issue(self, amount: float) -> bool:
        return self.admin_points["balance"] >= amount

    def mb_get_uses_in_last_24h(self, user_id: str) -> int:
        ts_list = self.mysterybox_uses.get(user_id, [])
        cutoff = time.time() - 24 * 3600
        ts_list = [t for t in ts_list if t >= cutoff]
        self.mysterybox_uses[user_id] = ts_list
        return len(ts_list)

    def mb_add_use(self, user_id: str):
        ts_list = self.mysterybox_uses.get(user_id, [])
        ts_list.append(time.time())
        self.mysterybox_uses[user_id] = ts_list

    # --- Event Handlers and Background Tasks ---
    async def setup_hook(self):
        """This runs before the bot connects to Discord."""
        logger.info("Starting the bot...")
        for extension in INITIAL_EXTENSIONS:
            try:
                await self.load_extension(extension)
                print(f"Loaded {extension}")
            except Exception as e:
                print(f"Failed to load {extension}: {e}")
        logger.info("Cogs loaded. Bot is ready.")

    async def on_ready(self):
        """Event handler for when the bot has connected to Discord."""
        logger.info('--------------------------------')
        logger.info(f'Logged in as {self.user.name}')
        logger.info(f'Bot ID: {self.user.id}')
        logger.info('--------------------------------')

        # ✅ STEP 1: Initialize the database tables. Only needed once.
        await self.init_db(self)

        # ✅ STEP 2: Load all the data from the database.
        await self.load_all_data_from_db()

        # Now your bot state is correct and ready to be used.
        # ... rest of your on_ready code
        for guild in self.guilds:
            try:
                self.invite_cache[guild.id] = await guild.invites()
            except discord.Forbidden:
                pass

    async def manage_periodic_message(self, channel, bot_data, message_id_key, embed, pin=False):
        try:
            message_id = bot_data.get(message_id_key)

            if message_id:
                try:
                    message = await channel.fetch_message(message_id)
                    await message.edit(embed=embed)
                except discord.NotFound:
                    new_message = await channel.send(embed=embed)
                    bot_data[message_id_key] = new_message.id
                    await self.save_data(self, "bot_data_table", bot_data)
                    if pin:
                        await new_message.pin()
                except discord.Forbidden:
                    logger.error(f"Bot missing permissions to edit/pin message in channel ({channel.id}).")
            else:
                new_message = await channel.send(embed=embed)
                bot_data[message_id_key] = new_message.id
                await self.save_data(self, "bot_data_table", bot_data)
                if pin:
                    await new_message.pin()

        except discord.Forbidden:
            logger.error(f"Bot missing permissions to send message in channel ({channel.id}).")
        except Exception as e:
            logger.error(f"An unexpected error occurred in manage_periodic_message: {e}")

    # P O I N T S     T R A N S A C T I O N      L O G
    async def log_points_transaction(self, user_id, points, purpose):
        try:
            user = self.get_user(int(user_id))
            user_mention = user.mention if user else "Unknown User"
            user_name = user.display_name if user else "Unknown User"

            new_entry = {
                "user_id": str(user_id),
                "points": points,
                "purpose": purpose,
                "timestamp": datetime.now(UTC).isoformat()
            }
            self.points_history.append(new_entry)

            if points > 0:
                title = "🎉 Points Credited"
                color = discord.Color.green()
                sign = "+"
            else:
                title = "💔 Points Debited"
                color = discord.Color.red()
                sign = ""

            embed = discord.Embed(
                title=title,
                description=f"**{user_mention}** received **{sign}{points:.2f} MVpts** for **{purpose}**.",
                color=color,
                timestamp=datetime.now(UTC)
            )
            embed.set_footer(text=f"Transaction logged for {user_name}")

            points_history_channel = self.get_channel(config.POINTS_HISTORY_CHANNEL_ID)
            if points_history_channel:
                await points_history_channel.send(embed=embed)
            else:
                logger.error(f"Points history channel (ID: {config.POINTS_HISTORY_CHANNEL_ID}) not found.")

            if "(burn)" in purpose.lower():
                burns_channel = self.get_channel(config.BURNS_LOG_CHANNEL_ID)
                if burns_channel:
                    burn_embed = discord.Embed(
                        title="🔥 Point Burn Log",
                        description=f"A burn transaction for **{user_mention}** was logged.",
                        color=discord.Color.dark_red()
                    )
                    burn_embed.add_field(name="Amount", value=f"**{sign}{points:.2f} MVpts**", inline=True)
                    burn_embed.add_field(name="Reason", value=f"**{purpose}**", inline=True)
                    burn_embed.set_footer(text=f"Logged by {self.user.name}")
                    await burns_channel.send(embed=burn_embed)
                else:
                    logger.error(f"Bot missing permissions or burns channel ({config.BURNS_LOG_CHANNEL_ID}) not found.")

        except Exception as e:
            logger.error(f"An unexpected error occurred while logging a transaction: {e}", exc_info=True)


# Create and run the bot instance
bot = MyBot()
bot.run(TOKEN)