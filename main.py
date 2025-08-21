# Standard library imports
import os
import time
from datetime import datetime, UTC
import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv

# Local application imports
from database import load_data, save_data
from logger import bot_logger as logger

# Load environment variables from .env file
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

# Ensure DISCORD_TOKEN is loaded
if not TOKEN:
    logger.error("Error: DISCORD_TOKEN not found in environment variables. Please check your .env file.")
    exit()

# Define the cogs you want to load
# This list must include all files in your 'cogs' directory
# that you want to be loaded as extensions.
INITIAL_EXTENSIONS = [
    'cogs.admin',
    'cogs.tasks'
]


class MyBot(commands.Bot):
    def __init__(self):
        # Set intents and command prefix
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

        # --- Attach Configuration and Data to the Bot Object ---

        # Load files that are DICTIONARIES
        self.user_points = load_data("user_points_table", {})
        self.submissions = load_data("submissions_table", {})
        self.vip_posts = load_data("vip_posts_table", {})
        self.user_xp = load_data("user_xp_table", {})
        self.weekly_quests = load_data("weekly_quests_table", {"week": 0, "quests": []})
        self.quest_submissions = load_data("quest_submissions_table", {})
        self.gm_log = load_data("gm_log_table", {})
        self.admin_points = load_data("admin_points_table", {
            "total_supply": 10000000000.0,
            "balance": 10000000000.0,
            "claimed_points": 0.0,
            "burned_points": 0.0,
            "my_points": 0.0,
            "fees_earned": 0.0
        })
        self.referral_data = load_data("referral_data_table", {})
        self.pending_referrals = load_data("pending_referrals_table", {})
        self.active_tickets = load_data("active_tickets_table", {})
        self.mysterybox_uses = load_data("mysterybox_uses_table", {})
        self.bot_data = load_data("bot_data_table", {})

        # Load files that are LISTS
        self.approved_proofs = load_data("approved_proofs_table", [])
        self.points_history = load_data("points_history_table", [])
        self.giveaway_winners_log = load_data("giveaway_logs_table", [])
        self.all_time_giveaway_winners_log = load_data("all_time_giveaway_logs_table", [])

        # Load files that are SETS
        self.referred_users = set(load_data("referred_users_table", []))
        self.processed_reactions = set(load_data("processed_reactions_table", []))

        # --- Static Configurations ---
        self.banned_words = ["shit", "sex", "fuck", "mad", "stupid", "idiot", "pussy", "dick", "boobs", "breast",
                             "asshole", "ass", "dumb"]

        # --- Channel & Role IDs ---
        self.ANNOUNCEMENT_CHANNEL_ID = 1399073900024959048
        self.ARCHIVED_TICKETS_CATEGORY_ID = 1403762112362184714
        self.BURNS_LOG_CHANNEL_ID = 1406022417075273849
        self.COMMAND_LOG_CHANNEL_ID = 1401443654371115018
        self.ENGAGEMENT_CHANNEL_ID = 1399127357595582616
        self.FIRST_ODOGWU_CHANNEL_ID = 1402065169890148454
        self.GIVEAWAY_CHANNEL_ID = 1402371502875218032
        self.GM_MV_CHANNEL_ID = 1402045203262603375
        self.HOW_TO_JOIN_CHANNEL_ID = 1399097281428324362
        self.LEADERBOARD_CHANNEL_ID = 1399125979644821574
        self.MOD_PAYMENT_REVIEW_CHANNEL_ID = 1400522100078280815
        self.MOD_QUEST_REVIEW_CHANNEL_ID = 1399109405995434115
        self.MOD_TASK_REVIEW_CHANNEL_ID = 1401135862661779466
        self.MYSTERYBOX_CHANNEL_ID = 1405125500015349780
        self.PAYMENT_CHANNEL_ID = 1400466642843992074
        self.PAYOUT_REQUEST_CHANNEL_ID = 1399126179574714368
        self.PERIODIC_LEADERBOARD_CHANNEL_ID = 1406757660782624789
        self.POINTS_HISTORY_CHANNEL_ID = 1402322062533726289
        self.QUEST_BOARD_CHANNEL_ID = 1401388448744472686
        self.QUEST_SUBMIT_CHANNEL_ID = 1401923217983143966
        self.SUPPORT_CHANNEL_ID = 1399076745612754944
        self.TASK_SUBMIT_CHANNEL_ID = 1399072864472268961
        self.TICKETS_CATEGORY_ID = 1403762721601753260
        self.XP_REWARD_CHANNEL_ID = 1401145656957206599
        self.VERIFY_CHANNEL_ID = 1399145888710000791
        self.VERIFY_MESSAGE_ID = 1399146011125092392

        # --- Payout Configuration ---
        self.MIN_PAYOUT_AMOUNT = 5000.0
        self.PAYOUT_FEE_PERCENTAGE = 10
        self.CONFIRMATION_TIMEOUT = 30
        self.POINTS_TO_USD = 0.0005
        self.GM_MV_POINTS_REWARD = 150.0
        self.APPROVED_EXCHANGES = ["binance", "bitget", "bybit", "mexc", "bingx"]

        # --- MYSTERY-BOX CONFIGURATION ---
        self.MYSTERYBOX_COST = 1000
        self.MYSTERYBOX_REWARDS = [900, 800, 1000, 1600]
        self.MYSTERYBOX_WEIGHTS = [35, 30, 20, 15]
        self.MYSTERYBOX_MAX_PER_24H = 2

        # --- Role IDs ---
        self.TIVATED_ROLE_ID = 1399078534672158811
        self.GAMER_ROLE_ID = 1399096408568758474
        self.ANIME_ROLE_ID = 1400397464611192914
        self.VIP_ROLE_ID = 1399079208419983540
        self.ROOKIE_ROLE_ID = 1400510593664024778
        self.ELITE_ROLE_ID = 1399095296725614673
        self.SUPREME_ROLE_ID = 1399077199109423125
        self.ADMIN_ROLE_ID = 1403069915623329876
        self.MOD_ROLE_ID = 1401016334338228234
        self.SERVER_ID = 1132898863548731434

        # --- Referral System Constants ---
        self.REFERRAL_CHANNEL_ID = 1402737676364550295
        self.REFERRAL_POINTS_PER_ROLE = {
            1400510593664024778: 1000.0,
            1399095296725614673: 1500.0,
            1399077199109423125: 2000.0,
            1399079208419983540: 10000.0
        }
        self.NEW_MEMBER_POINTS_PER_ROLE = {
            1400510593664024778: 1000.0,
            1399095296725614673: 1500.0,
            1399077199109423125: 2000.0
        }

        # --- Reaction Award Feature ---
        self.REACTION_CATEGORY_IDS = [1399082427338592336, 1400397422450184223]
        self.REACTION_EMOJI = "🌟"
        self.MIN_REACTION_POINTS = 50.0
        self.MAX_REACTION_POINTS = 150.0
        self.MAX_WINNERS_HISTORY = 50

        # --- Static Configurations ---
        self.POINT_VALUES = {"like": 20, "retweet": 30, "comment": 15}
        self.ROLE_MULTIPLIERS = {
            self.ROOKIE_ROLE_ID: 1.0,
            self.ELITE_ROLE_ID: 1.5,
            self.SUPREME_ROLE_ID: 2.0,
            self.VIP_ROLE_ID: 0.0
        }
        self.QUEST_POINTS = 100.0
        self.EMOJI_ROLE_MAP = {
            ("odogwu", 1399069963045572799): self.TIVATED_ROLE_ID,
            ("🎮", None): self.GAMER_ROLE_ID,
            ("🍥", None): self.ANIME_ROLE_ID
        }

        # --- Global Bot State Variables ---
        self.invite_cache = {}
        self.invites_before_join = {}
        self.ticket_messages_to_archive = {}

    # --- Helper Functions (Correctly placed here) ---
    def ensure_user(self, user_id: str):
        """Ensures a user has an entry in the user_points dictionary."""
        self.user_points.setdefault(user_id, {"all_time_points": 0.0, "available_points": 0.0})

    def get_user_balance(self, user_id: str) -> float:
        """Safely retrieves a user's available points."""
        return self.user_points.get(user_id, {}).get("available_points", 0.0)

    def admin_can_issue(self, amount: float) -> bool:
        """Checks if the admin has enough points to issue."""
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
        print("Starting the bot...")
        for extension in INITIAL_EXTENSIONS:
            try:
                await self.load_extension(extension)
                print(f"Loaded {extension}")
            except Exception as e:
                print(f"Failed to load {extension}: {e}")
        print("Cogs loaded. Bot is ready.")

    async def on_ready(self):
        """Event handler for when the bot has connected to Discord."""
        print('--------------------------------')
        print(f'Logged in as {self.user.name}')
        print(f'Bot ID: {self.user.id}')
        print('--------------------------------')

        # Load invites from cache
        for guild in self.guilds:
            try:
                self.invite_cache[guild.id] = await guild.invites()
            except discord.Forbidden:
                pass

    async def on_message(self, message):
        """
        Handles command processing.
        NOTE: This passes control to any cogs with on_message events.
        """
        await self.process_commands(message)


# Create and run the bot instance
bot = MyBot()
bot.run(TOKEN)