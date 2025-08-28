# Standard library imports
import os
import time
from datetime import datetime, UTC
import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

# Local application imports
from database import init_db, load_single_json, save_single_json, load_all_json, save_all_json, save_list_values, \
    load_list_values, save_list_of_json, load_list_of_json, log_points_transaction as db_log_points
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

        self.db_executor = ThreadPoolExecutor()

        self.init_db = init_db
        self.load_single_json = load_single_json
        self.save_single_json = save_single_json
        self.load_all_json = load_all_json
        self.save_all_json = save_all_json
        self.load_list_values = load_list_values
        self.save_list_values = save_list_values
        self.load_list_of_json = load_list_of_json
        self.save_list_of_json = save_list_of_json
        self.log_points_transaction_db = db_log_points

        self.users_points = {}
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

    async def load_all_data_from_db(self):
        self.users_points = await self.load_all_json(self, "user_points")
        self.submissions = await self.load_all_json(self, "submissions")
        self.vip_posts = await self.load_all_json(self, "vip_posts")
        self.user_xp = await self.load_all_json(self, "user_xp")
        self.weekly_quests = await self.load_single_json(self, "weekly_quests", "main", {"week": 0, "quests": []})
        self.quest_submissions = await self.load_all_json(self, "quest_submissions")
        self.gm_log = await self.load_all_json(self, "gm_log")
        self.admin_points = await self.load_single_json(self, "admin_points", "main", {
            "total_supply": 10000000000.0,
            "balance": 10000000000.0,
            "in_circulation": 0.0,
            "burned": 0.0,
            "my_points": 0.0,
            "treasury": 0.0
        })
        self.referral_data = await self.load_all_json(self, "referral_data")
        self.pending_referrals = await self.load_all_json(self, "pending_referrals")
        self.active_tickets = await self.load_all_json(self, "active_tickets")
        self.mysterybox_uses = await self.load_all_json(self, "mysterybox_uses")
        self.bot_data = await self.load_single_json(self, "bot_data", "main", {})

        self.approved_proofs = await self.load_list_values(self, "approved_proofs", "normalized_url")
        self.referred_users = set(await self.load_list_values(self, "referred_users", "user_id"))
        self.processed_reactions = set(await self.load_list_values(self, "processed_reactions", "reaction_identifier"))

        self.points_history = await self.load_list_of_json(self, "points_history")
        self.giveaway_winners_log = await self.load_list_of_json(self, "giveaway_logs")
        self.all_time_giveaway_winners_log = await self.load_list_of_json(self, "all_time_giveaway_logs")

        logger.info("✅ All bot data loaded from the database.")

    async def save_all_data_to_db(self):
        try:
            # Save JSON dictionary data
            await self.save_all_json(self, "users_points", self.users_points)
            await self.save_all_json(self, "submissions", self.submissions)
            await self.save_all_json(self, "vip_posts", self.vip_posts)
            await self.save_all_json(self, "user_xp", self.user_xp)
            await self.save_single_json(self, "weekly_quests", "main", self.weekly_quests)
            await self.save_all_json(self, "quest_submissions", self.quest_submissions)
            await self.save_all_json(self, "gm_log", self.gm_log)
            await self.save_single_json(self, "admin_points", "main", self.admin_points)
            await self.save_all_json(self, "referral_data", self.referral_data)
            await self.save_all_json(self, "pending_referrals", self.pending_referrals)
            await self.save_all_json(self, "active_tickets", self.active_tickets)
            await self.save_all_json(self, "mysterybox_uses", self.mysterybox_uses)
            await self.save_single_json(self, "bot_data", "main", self.bot_data)

            # Save list and set data using the new function
            await self.save_list_values(self, "approved_proofs", self.approved_proofs, "normalized_url")
            await self.save_list_values(self, "referred_users", list(self.referred_users), "user_id")
            await self.save_list_values(self, "processed_reactions", list(self.processed_reactions),
                                        "reaction_identifier")

            # Save lists of JSON objects
            await self.save_list_of_json(self, "points_history", self.points_history)
            await self.save_list_of_json(self, "giveaway_logs", self.giveaway_winners_log)
            await self.save_list_of_json(self, "all_time_giveaway_logs", self.all_time_giveaway_winners_log)

            logger.info("✅ All bot data saved to the database.")

        except Exception as e:
            logger.error(f"❌ An error occurred while saving all data: {e}", exc_info=True)

    def ensure_user(self, user_id: str):
        self.users_points.setdefault(user_id, {"all_time_points": 0.0, "available_points": 0.0})

    def get_user_balance(self, user_id: str) -> float:
        return self.users_points.get(user_id, {}).get("available_points", 0.0)

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

    async def setup_hook(self):
        logger.info("Starting the bot...")
        for extension in INITIAL_EXTENSIONS:
            try:
                await self.load_extension(extension)
                print(f"Loaded {extension}")
            except Exception as e:
                print(f"Failed to load {extension}: {e}")
        logger.info("Cogs loaded. Bot is ready.")

    async def on_ready(self):
        logger.info('--------------------------------')
        logger.info(f'Logged in as {self.user.name}')
        logger.info(f'Bot ID: {self.user.id}')
        logger.info('--------------------------------')

        await self.init_db(self)

        self.admin_points = await self.load_single_json(self, "admin_points", "main", {
            "total_supply": 10000000000.0,
            "balance": 10000000000.0,
            "in_circulation": 0.0,
            "burned": 0.0,
            "my_points": 0.0,
            "treasury": 0.0
        })

        if not self.admin_points or "balance" not in self.admin_points:
            logger.info("Initializing bot's main economy table with default values...")
            self.admin_points = {
                "total_supply": 10000000000.0,
                "balance": 10000000000.0,
                "in_circulation": 0.0,
                "burned": 0.0,
                "my_points": 0.0,
                "treasury": 0.0
            }
            await self.save_single_json(self, "admin_points", "main", self.admin_points)
            logger.info("✅ Economy table initialized successfully.")

        await self.load_all_data_from_db()

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
                    await self.save_single_json(self, "bot_data", "main", bot_data)
                    if pin:
                        await new_message.pin()
                except discord.Forbidden:
                    logger.error(f"Bot missing permissions to edit/pin message in channel ({channel.id}).")
            else:
                new_message = await channel.send(embed=embed)
                bot_data[message_id_key] = new_message.id
                await self.save_single_json(self, "bot_data", "main", bot_data)
                if pin:
                    await new_message.pin()

        except discord.Forbidden:
            logger.error(f"Bot missing permissions to send message in channel ({channel.id}).")
        except Exception as e:
            logger.error(f"An unexpected error occurred in manage_periodic_message: {e}")

    async def log_points_transaction(self, user_id, points, purpose):
        try:
            # 1. First, save the transaction to the database using the correct function.
            await self.log_points_transaction_db(self, user_id, points, purpose)

            # 2. Then, send the Discord message.
            user = self.get_user(int(user_id))
            user_mention = user.mention if user else "Unknown User"
            user_name = user.display_name if user else "Unknown User"

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


bot = MyBot()
bot.run(TOKEN)