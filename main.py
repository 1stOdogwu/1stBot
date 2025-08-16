import json
import logging
import os
import random
import re
from urllib.parse import urlparse, urlunparse
import time
import asyncio

import discord
from discord.ext import commands, tasks
from datetime import UTC
from dotenv import load_dotenv

load_dotenv()
token = os.getenv('DISCORD_TOKEN')

# Ensure DISCORD_TOKEN is loaded
if not token:
    print("Error: DISCORD_TOKEN not found in environment variables. Please check your .env file.")
    exit()

# Logging setup
handler = logging.FileHandler(filename='discord.log', encoding='utf-8', mode='w')
handler.setLevel(logging.INFO)

# Discord Intents
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.reactions = True
intents.presences = True

bot = commands.Bot(command_prefix="!", intents=intents)

# --- File Paths for Data Storage ---
POINTS_FILE = "points.json"
SUBMISSIONS_FILE = "submissions.json"
LOG_FILE = "command_logs.json"
VIP_POSTS_FILE = "vip_posts.json"
XP_FILE = "xp.json"
QUESTS_FILE = "weekly_quests.json"
QUEST_SUBMISSIONS_FILE = "quest_submissions.json"
APPROVED_PROOFS_FILE = "approved_proofs.json"
GM_LOG_FILE = "gm_log.json"
ADMIN_POINTS_FILE = "admin_points.json"
POINTS_HISTORY_FILE = "points_history.json"
GIVEAWAY_LOG_FILE = "giveaway_winners.json"
GIVEAWAY_ALL_TIME_LOG_FILE = "giveaway_all_time.json"
REFERRALS_FILE = "referrals.json"
PENDING_REFERRALS_FILE = "pending_referrals.json"
PROCESSED_REACTIONS_FILE = 'processed_reactions.json'
ACTIVE_TICKETS_FILE = "active_tickets.json"
MYSTERYBOX_USES_FILE = "mysterybox_uses.json"
REFERRED_USERS_FILE = "referred_users.json"
economy_message_id = None
history_message_id = None
giveaway_history_message_id = None
giveaway_winners_message_id = None
referral_leaderboard_message_id = None
points_leaderboard_message_id = None


# --- Helper Functions for Loading Data ---
def load_json_file_with_dict_defaults(filename, default_value):
    """Loads a JSON file, initializing with a default dictionary if not found."""
    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Add missing keys from the default value
            for key, value in default_value.items():
                if key not in data:
                    data[key] = value
            return data
    except (FileNotFoundError, json.JSONDecodeError):
        # The File wasn't found or is empty/malformed, return the default value
        return default_value

def load_json_file_with_list_defaults(filename, default_value):
    """Loads a JSON file, initializing with a default list if not found."""
    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data
    except (FileNotFoundError, json.JSONDecodeError):
        # The File isn't found or is empty/malformed, return the default value
        return default_value


# --- Global Variables ---
invite_cache = {}
invites_before_join = {}
ticket_messages_to_archive = {}

# Initial Total Supply of points
TOTAL_SUPPLY = 10_000_000_000.0

# --- Helper Functions for Saving Data ---
def save_json_file(filename, data):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

def save_points():
    save_json_file(POINTS_FILE, user_points)

def save_submissions():
    save_json_file(SUBMISSIONS_FILE, submissions)

def save_command_logs():
    save_json_file(LOG_FILE, logs)

def save_vip_posts():
    save_json_file(VIP_POSTS_FILE, vip_posts)

def save_xp():
    save_json_file(XP_FILE, user_xp)

def save_weekly_quests():
    save_json_file(QUESTS_FILE, weekly_quests)

def save_quest_submissions():
    save_json_file(QUEST_SUBMISSIONS_FILE, quest_submissions)

def save_approved_proofs():
    save_json_file(APPROVED_PROOFS_FILE, approved_proofs)

def save_gm_log():
    save_json_file(GM_LOG_FILE, gm_log)

def save_processed_reactions():
    save_json_file(list(PROCESSED_REACTIONS_FILE), processed_reactions)

# The corrected helper function
def save_giveaway_log():
    save_json_file(GIVEAWAY_LOG_FILE, giveaway_winners_log)

# This was also bugged, calling itself recursively.
def save_giveaway_all_time():
    save_json_file(GIVEAWAY_ALL_TIME_LOG_FILE, all_time_giveaway_winners_log)

def save_referrals():
    save_json_file(REFERRALS_FILE, referral_data)

def save_pending_referrals():
    save_json_file(PENDING_REFERRALS_FILE, pending_referrals)

def save_points_history():
    save_json_file(POINTS_HISTORY_FILE, points_history)

def save_active_tickets():
    save_json_file(ACTIVE_TICKETS_FILE, active_tickets)

def save_mysterybox_uses():
    save_json_file(MYSTERYBOX_USES_FILE, mysterybox_uses)

def save_referred_users():
    save_json_file(REFERRED_USERS_FILE, referred_users)

def ensure_user(user_id: str):
    """Ensures a user has an entry in the user_points dictionary."""
    if user_id not in user_points:
        user_points[user_id] = {"all_time_points": 0.0, "available_points": 0.0}

def get_user_balance(user_id: str) -> float:
    """Safely retrieves a user's available points."""
    return user_points.get(user_id, {}).get("available_points", 0.0)

def admin_can_issue(amount: float) -> bool:
    """Checks if the admin has enough points to issue."""
    return admin_points["balance"] >= amount



# --- Load files that are DICTIONARIES ---
user_points = load_json_file_with_dict_defaults(POINTS_FILE, {})
submissions = load_json_file_with_dict_defaults(QUEST_SUBMISSIONS_FILE, {})
logs = load_json_file_with_dict_defaults(LOG_FILE, {})
vip_posts = load_json_file_with_dict_defaults(VIP_POSTS_FILE, {})
user_xp = load_json_file_with_dict_defaults(XP_FILE, {})
weekly_quests = load_json_file_with_dict_defaults(QUESTS_FILE, {"week": 0, "quests": []})
quest_submissions = load_json_file_with_dict_defaults(SUBMISSIONS_FILE, {})
gm_log = load_json_file_with_dict_defaults(GM_LOG_FILE, {})
admin_points = load_json_file_with_dict_defaults(ADMIN_POINTS_FILE, {
    "total_supply": TOTAL_SUPPLY,
    "balance": TOTAL_SUPPLY,
    "claimed_points": 0.0,
    "burned_points": 0.0,
    "my_points": 0.0,
    "fees_earned": 0.0
})
referral_data = load_json_file_with_dict_defaults(REFERRALS_FILE, {})
pending_referrals = load_json_file_with_dict_defaults(PENDING_REFERRALS_FILE, {})
active_tickets = load_json_file_with_dict_defaults(ACTIVE_TICKETS_FILE, {})
mysterybox_uses = load_json_file_with_dict_defaults(MYSTERYBOX_USES_FILE, {})
referred_users = load_json_file_with_dict_defaults(REFERRED_USERS_FILE, {})

# --- Load files that are LISTS ---
approved_proofs = load_json_file_with_list_defaults(APPROVED_PROOFS_FILE, [])
points_history = load_json_file_with_list_defaults(POINTS_HISTORY_FILE, [])
giveaway_winners_log = load_json_file_with_list_defaults(GIVEAWAY_LOG_FILE, [])
all_time_giveaway_winners_log = load_json_file_with_list_defaults(GIVEAWAY_ALL_TIME_LOG_FILE, [])
processed_reactions = set(load_json_file_with_list_defaults('processed_reactions.json', []))

print(f"POINTS_FILE type: {type(POINTS_FILE)}")
print(f"user_points type: {type(user_points)}")
print(f"GIVEAWAY_LOG_FILE type: {type(GIVEAWAY_LOG_FILE)}")
print(f"giveaway_winners_log type: {type(giveaway_winners_log)}")


# --- Channel & Role IDs (Ensure these are correct for your server) ---
ARCHIVED_TICKETS_CATEGORY_ID = 1403762112362184714
BURNS_LOG_CHANNEL_ID = 1406022417075273849
COMMAND_LOG_CHANNEL = 1401443654371115018
ENGAGEMENT_CHANNEL_ID = 1399127357595582616
FIRST_ODOGWU_CHANNEL_ID = 1402065169890148454
GIVEAWAY_CHANNEL_ID = 1402371502875218032
GM_G1ST_CHANNEL_ID = 1402045203262603375
HOW_TO_JOIN_CHANNEL_ID = 1399097281428324362
LEADERBOARD_CHANNEL_ID = 1399125979644821574
MOD_PAYMENT_REVIEW_CHANNEL_ID = 1400522100078280815
MOD_QUEST_REVIEW_CHANNEL_ID = 1399109405995434115
MOD_TASK_REVIEW_CHANNEL_ID = 1401135862661779466
MYSTERYBOX_CHANNEL_ID = 1405125500015349780
PAYMENT_CHANNEL_ID = 1400466642843992074
PAYOUT_REQUEST_CHANNEL_ID = 1399126179574714368
POINTS_HISTORY_CHANNEL_ID = 1402322062533726289
QUEST_BOARD_CHANNEL_ID = 1401388448744472686
QUEST_SUBMIT_CHANNEL_ID = 1401923217983143966
SUPPORT_CHANNEL_ID = 1399076745612754944
TASK_SUBMIT_CHANNEL_ID = 1399072864472268961
TICKETS_CATEGORY_ID = 1403762721601753260
XP_REWARD_CHANNEL_ID = 1401145656957206599
VERIFY_CHANNEL_ID = 1399145888710000791
VERIFY_MESSAGE_ID = 1399146011125092392

# Payout configuration constants
MIN_PAYOUT_AMOUNT = 5000.0  # New minimum payout amount
PAYOUT_FEE_PERCENTAGE = 10
CONFIRMATION_TIMEOUT = 30
POINTS_TO_USD = 0.0005
GM_G1ST_POINTS_REWARD = 150.0
APPROVED_EXCHANGES = ["binance", "bitget", "bybit", "mexc", "bingx"]

#MYSTERY-BOX CONFIGURATION CONSTANTS
MYSTERYBOX_COST = 1000
MYSTERYBOX_REWARDS = [900, 800, 1000, 1600]
MYSTERYBOX_WEIGHTS = [35, 30, 20, 15]
MYSTERYBOX_MAX_PER_24H = 2

# === Role IDs ===
TIVATED_ROLE_ID = 1399078534672158811
GAMER_ROLE_ID = 1399096408568758474
ANIME_ROLE_ID = 1400397464611192914
VIP_ROLE_ID = 1399079208419983540
ROOKIE_ROLE_ID = 1400510593664024778
ELITE_ROLE_ID = 1399095296725614673
SUPREME_ROLE_ID = 1399077199109423125
ADMIN_ROLE_ID = 1403069915623329876
MOD_ROLE_ID = 1401016334338228234
SERVER_ID = 1132898863548731434

# === Referral System Constants ===
REFERRAL_CHANNEL_ID = 1402737676364550295  # Channel where welcome messages are sent
REFERRAL_POINTS_PER_ROLE = {
    1400510593664024778: 1000.0,  # Role ID for 'Premium Member'
    1399095296725614673: 1500.0, # Role ID for 'VIP Member'
    1399077199109423125: 2000.0, # Role ID for 'Elite Member'
    1399079208419983540: 10000.0
}

NEW_MEMBER_POINTS_PER_ROLE = {
    1400510593664024778: 1000.0,  # Role ID for 'Premium Member'
    1399095296725614673: 1500.0,  # Role ID for 'VIP Member'
    1399077199109423125: 2000.0   # Role ID for 'Elite Member'
}

# === Reaction Award Feature ===
REACTION_CATEGORY_IDS = [1399082427338592336, 1400397422450184223]
REACTION_EMOJI = "🌟"
MIN_REACTION_POINTS = 50.0
MAX_REACTION_POINTS = 150.0



# --- Static Configurations ---
POINT_VALUES = {"like": 20, "retweet": 30, "comment": 15}
ROLE_MULTIPLIERS = {
    ROOKIE_ROLE_ID: 1.0,
    ELITE_ROLE_ID: 1.5,
    SUPREME_ROLE_ID: 2.0,
    VIP_ROLE_ID: 0.0
}
banned_words = ["shit", "sex", "fuck", "mad", "stupid", "idiot", "pussy", "dick", "boobs", "breast", "asshole", "ass", "dumb"]

EMOJI_ROLE_MAP = {
    ("odogwu", 1399069963045572799): TIVATED_ROLE_ID,
    ("🎮", None): GAMER_ROLE_ID,
    ("🍥", None): ANIME_ROLE_ID
}


# --- Utility Function for URL Normalization ---
def normalize_url(url: str) -> str:
    """Normalizes URLs for consistent comparison."""
    if not url:
        return ""

    url = url.replace("https://x.com/", "https://twitter.com/").replace("http://", "https://")

    parsed = urlparse(url)
    path = parsed.path.rstrip('/')
    netloc = parsed.netloc.lower()

    if 'twitter.com' in netloc:
        match = re.match(r'(https?://(?:www\.)?twitter\.com/[^/]+/status/\d+)', url, re.IGNORECASE)
        if match:
            return match.group(1).lower()

    normalized = urlunparse((parsed.scheme, netloc, path, '', '', ''))
    return normalized.lower()


# === Bot Events ===
@bot.event
async def on_ready():
    """Event handler for when the bot has connected to Discord."""
    global user_points, approved_proofs, quest_submissions, weekly_quests, admin_points, gm_log
    global submissions, user_xp, points_history, history_message_id, giveaway_winners_log, giveaway_history_message_id, all_time_giveaway_winners_log
    global referral_data, pending_referrals, invite_cache, processed_reactions

    # Load user points data
    try:
        with open(POINTS_FILE, "r") as f:
            user_points = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        user_points = {}

    # Load command logs data
    try:
        with open(LOG_FILE, "r") as f:
            command_logs = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        command_logs = {}

    # Load the GM/G1st log file
    try:
        with open(GM_LOG_FILE, "r") as f:
            gm_log = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        gm_log = {}

    # NEW: Load admin's point data and initialize if the file doesn't exist
    admin_points = load_json_file_with_dict_defaults(ADMIN_POINTS_FILE, {
        "total_supply": TOTAL_SUPPLY,
        "balance": TOTAL_SUPPLY,
        "claimed_points": 0.0,
        "burned_points": 0.0,
        "my_points": 0.0,
        "fees_earned": 0.0
    })

        # NEW: Load invites from cache
    for guild in bot.guilds:
        try:
            invite_cache[guild.id] = await guild.invites()
            print(f"✅ Invite cache initialized for {guild.name} ({len(invite_cache[guild.id])} invites)")
        except discord.Forbidden:
            print(f"⚠️ Missing permission to view invites for {guild.name}")

    processed_reactions = set(load_json_file_with_list_defaults('processed_reactions.json', []))

    print(f"✅ Bot is live as {bot.user} (ID: {bot.user.id})")
    print("✅ Invite cache loaded and points loaded.")

    # Start all background tasks
    save_logs_periodically.start()
    reset_vip_posts.start()
    weekly_xp_bonus.start()
    update_economy_message.start()
    await update_points_history_message()
    update_giveaway_winners_history.start()
    update_referral_leaderboard.start()
    update_points_leaderboard.start()


# === LOG POINTS TRANSACTIONS ===
async def log_points_transaction(user_id, points, purpose):
    """Adds a new entry to the points' history log and updates the channel message."""
    new_entry = {
        "user_id": str(user_id),
        "points": points,
        "purpose": purpose,
        "timestamp": datetime.now().isoformat()
    }
    points_history.append(new_entry)
    save_json_file(POINTS_HISTORY_FILE, points_history)

    # --- ADD THIS BLOCK FOR BURN LOGS ---
    # Send a separate log to the burn channel for burn transactions
    if "(burn)" in purpose:
        user = bot.get_user(int(user_id))
        user_name = user.name if user else "Unknown User"
        sign = "+" if points >= 0 else ""
        log_message = f"💵 {user_name} | {purpose} | **{sign}{points:.2f} pts**"

        burns_channel = bot.get_channel(BURNS_LOG_CHANNEL_ID)
        if burns_channel:
            try:
                await burns_channel.send(f"🔥 BURN LOG: {log_message}")
            except discord.Forbidden:
                print(f"Bot missing permissions to log burn transaction to channel ({BURNS_LOG_CHANNEL_ID}).")

    # --- NEW LINE ---
    await update_points_history_message()


# === POINTS HISTORY MESSAGE ===
async def update_points_history_message():
    global history_message_id

    channel = bot.get_channel(POINTS_HISTORY_CHANNEL_ID)
    if not channel:
        print(f"Error: Points history channel with ID {POINTS_HISTORY_CHANNEL_ID} not found.")
        return

    if not points_history:
        history_message = "📈 **Points History**\nNo transactions to display yet."
    else:
        recent_history = points_history[-15:]  # Show the last 15 transactions
        history_message = "📈 **Points History**\n"

        for entry in recent_history:
            user = bot.get_user(int(entry["user_id"]))
            user_name = user.name if user else f"User ID: {entry['user_id']}"
            points = entry["points"]
            purpose = entry["purpose"]
            timestamp = datetime.fromisoformat(entry["timestamp"]).strftime('%Y-%m-%d %H:%M')

            history_message += f"💵• `{timestamp}`: **{user_name}** earned **{points:.2f} points** for **{purpose}**.\n"

    try:
        if history_message_id:
            message = await channel.fetch_message(history_message_id)
            await message.edit(content=history_message)
        else:
            message = await channel.send(history_message)
            history_message_id = message.id
    except discord.NotFound:
        # The Message was deleted, so send a new one
        message = await channel.send(history_message)
        history_message_id = message.id
    except discord.Forbidden:
        print(f"Error: Bot does not have permissions to send/edit messages in channel {channel.name}.")


#------------------C O M M A N D    L O G G I N G    &    R A T E     L I M I T I N G------------------
@tasks.loop(minutes=5)
async def save_logs_periodically():
    """Saves the command logs to a file every 5 minutes."""
    save_command_logs()
    print("✅ Command logs saved.")


# Event handler for logging commands (without rate-limit logic)
@bot.event
async def on_command(ctx):
    uid = str(ctx.author.id)
    cmd = ctx.command.name
    now = time.time()

    user_log = logs.setdefault(uid, {}).setdefault(cmd, [])
    # Clean up old entries
    user_log = [t for t in user_log if now - t < 1800]
    user_log.append(now)

    logs[uid][cmd] = user_log

    # Log to a channel
    log_channel = bot.get_channel(COMMAND_LOG_CHANNEL)
    if log_channel:
        try:
            await log_channel.send(f"🧾 `{cmd}` used by <@{uid}>")
        except discord.Forbidden:
            print(f"Bot missing permissions to log command to channel ({COMMAND_LOG_CHANNEL}).")


#-----------------------------------------------------------------------------------------------------------
# === THE ECONOMY MECHANISM ===
def get_economy_message_content(admin_data):
    """Builds the formatted string for the economy status message."""
    try:
        balance = admin_data.get("balance", 0.0)
        in_circulation = admin_data.get("claimed_points", 0.0)
        burned_points = admin_data.get("burned_points", 0.0)
        treasury = admin_data.get("fees_earned", 0.0)
        my_points = admin_data.get("my_points", 0.0)
        total_supply = admin_data.get("total_supply", 0.0)

        usd_total_supply = total_supply * POINTS_TO_USD
        usd_balance = balance * POINTS_TO_USD
        usd_in_circulation = in_circulation * POINTS_TO_USD
        usd_burned_points = burned_points * POINTS_TO_USD
        usd_treasury = treasury * POINTS_TO_USD
        usd_my_points = my_points * POINTS_TO_USD

        message_content = (
            f"**🪙 Odogwu Points Economy Status**\n\n"
            f"**Total Supply:** {total_supply:.2f} points (${usd_total_supply:.2f})\n"
            f"**Remaining Supply:** {balance:.2f} points (${usd_balance:.2f})\n"
            f"**In Circulation:** {in_circulation:.2f} points (${usd_in_circulation:.2f})\n"
            f"**Burned:** {burned_points:.2f} points (${usd_burned_points:.2f})\n"
            f"**Treasury:** {treasury:.2f} points (${usd_treasury:.2f})\n"
            f"**Admin's Earned Points:** {my_points:.2f} points (${usd_my_points:.2f})\n"
            f"*(Last updated: <t:{int(time.time())}:R>)*"
        )
        return message_content
    except NameError:
        return "❌ Error: POINTS_TO_USD constant is not defined."
    except Exception as e:
        return f"❌ An error occurred while generating the message content: {e}"


@tasks.loop(minutes=5)
async def update_economy_message():
    """Periodically updates the economy status message in a dedicated channel."""
    global economy_message_id
    message_content = None
    channel = None

    try:
        channel = bot.get_channel(FIRST_ODOGWU_CHANNEL_ID)
        if not channel:
            print(f"❌ Error: Economy updates channel (ID: {FIRST_ODOGWU_CHANNEL_ID}) not found.")
            return

        message_content = get_economy_message_content(admin_points)

        if economy_message_id:
            message = await channel.fetch_message(economy_message_id)
            await message.edit(content=message_content)
        else:
            message = await channel.send(message_content)
            economy_message_id = message.id

    except discord.NotFound:
        message = await channel.send(message_content)
        economy_message_id = message.id
    except discord.Forbidden:
        print(f"❌ Bot missing permissions to send/edit messages in channel ({FIRST_ODOGWU_CHANNEL_ID}).")
    except Exception as e:
        print(f"❌ An error occurred in the economy update task: {e}")


#-------------------------A D M I N       U N I T-----------------------------------
@bot.command()
@commands.has_role(ADMIN_ROLE_ID)
async def admin(ctx):
    """(Admin Only) Displays the bot's point economy status."""
    try:
        # Delete the command message
        await ctx.message.delete()

        # Get the message content from the helper function
        message_content = get_economy_message_content(admin_points)
        await ctx.send(message_content)
    except discord.Forbidden:
        # If the bot doesn't have permissions, just send the message without deleting the command
        message_content = get_economy_message_content(admin_points)
        await ctx.send(message_content)
    except Exception as e:
        await ctx.send(f"❌ An error occurred while checking balances: {e}")


@bot.command(name="data")
@commands.has_role(ADMIN_ROLE_ID)
@commands.cooldown(1, 30, commands.BucketType.user)
async def get_server_data(ctx):
    """
    (Admin Only) Displays all user data sorted by points, including referral count.
    """
    all_users_data = []

    # Process all user data and get referral counts
    for user_id, user_data in user_points.items():
        all_time_points = user_data.get("all_time_points", 0.0)

        # Count referrals for the current user
        referral_count = 0
        for _, referrer_id in referral_data.items():
            if referrer_id == user_id:
                referral_count += 1

        all_users_data.append({
            "id": user_id,
            "points": all_time_points,
            "referrals": referral_count
        })

    # Sort the list by points, from highest to lowest
    all_users_data.sort(key=lambda x: x["points"], reverse=True)

    # Prepare the embed
    embed = discord.Embed(
        title="📊 Server Economy Data",
        description="A list of all users, sorted by points.",
        color=discord.Color.dark_purple()
    )

    # Format the data into a single string for a field
    data_text = ""
    for idx, user_info in enumerate(all_users_data[:50], 1):  # Show top 50 users
        try:
            user = await bot.fetch_user(int(user_info["id"]))
            username = user.name
        except discord.NotFound:
            username = f"Unknown User (ID: {user_info['id']})"

        points = user_info["points"]
        referrals = user_info["referrals"]

        data_text += f"**#{idx}**: {username} - **{points:.2f} pts** | Referrals: {referrals}\n"

    if data_text:
        embed.add_field(name="User Rankings", value=data_text, inline=False)
    else:
        embed.add_field(name="User Rankings", value="No user data available.", inline=False)

    embed.set_footer(text="Data refreshes upon command.")
    embed.timestamp = datetime.now(UTC)

    await ctx.send(embed=embed)


@get_server_data.error
async def data_command_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("❌ You do not have the required role to use this command.", delete_after=15)

# ---------------------------------------------------------------------------

# Event handler for cooldown errors
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandOnCooldown):
        # Notify the user they are on cooldown
        cooldown_message = await ctx.send(f"⚠️ {ctx.author.mention}, you are on cooldown for this command. Try again in **{error.retry_after:.2f} seconds**.", delete_after=10)
        # We can optionally delete the user's original message
        try:
            await ctx.message.delete(delay=10)
        except discord.Forbidden:
            pass
    elif isinstance(error, commands.MissingPermissions):
        await ctx.send("❌ You do not have the required permissions to use this command.", delete_after=10)
    else:
        # Pass other errors to the default error handler
        await bot.on_command_error(ctx, error)


# --- Example Command using the new system ---
@bot.command()
@commands.cooldown(5, 1800, commands.BucketType.user)
async def newcommand(ctx):
    """An example of a rate-limited command."""
    await ctx.send("This command is limited to 5 uses every 30 minutes.")


@tasks.loop(hours=24)
async def reset_vip_posts():
    """Resets the daily VIP post-limit."""
    global vip_posts
    vip_posts = {}
    save_vip_posts()
    print("🔄 VIP post limit reset.")


@tasks.loop(hours=168)
async def update_giveaway_winners_history():
    global giveaway_winners_log, giveaway_history_message_id, all_time_giveaway_winners_log

    # Only update if there are new winners in the temporary log
    if not giveaway_winners_log:
        return

    # Find the giveaway channel
    channel = bot.get_channel(GIVEAWAY_CHANNEL_ID)
    if not channel:
        print(f"Error: Giveaway channel with ID {GIVEAWAY_CHANNEL_ID} not found.")
        return

    # 🎉 Create the embed from the ALL-TIME log
    embed = discord.Embed(
        title="🎉 All-Time Giveaway Winners 🎉",
        description="Here’s the full hall of fame for all giveaways so far 🏆",
        color=discord.Color.gold()
    )

    for winner in all_time_giveaway_winners_log:
        user = bot.get_user(int(winner['user_id']))
        user_name = user.mention if user else f"User ID: {winner['user_id']}"
        embed.add_field(
            name=f"✨ {user_name}",
            value=f"**{winner['points']:.2f} points** 🎁\n*Reason:* {winner['purpose']}",
            inline=False
        )

    embed.set_footer(text="Updated automatically as giveaways happen 🚀")
    embed.timestamp = datetime.now(UTC)

    try:
        if giveaway_history_message_id:
            message = await channel.fetch_message(giveaway_history_message_id)
            await message.edit(embed=embed)  # ✅ Edit existing embed instead of plain text
        else:
            message = await channel.send(embed=embed)
            giveaway_history_message_id = message.id

    except discord.NotFound:
        # If the old message was deleted, send a fresh one
        message = await channel.send(embed=embed)
        giveaway_history_message_id = message.id
    except discord.Forbidden:
        print(f"Error: Bot does not have permissions to send/edit embeds in channel {channel.name}.")
        return

    # Once the message is updated, clear the temporary log file only
    giveaway_winners_log.clear()
    save_json_file(GIVEAWAY_LOG_FILE, giveaway_winners_log)
    print("✅ Giveaway history updated and temporary log cleared.")


async def append_new_winner_to_history(new_winners_data: list):
    global giveaway_history_message_id

    channel = bot.get_channel(GIVEAWAY_CHANNEL_ID)
    if not channel:
        print(f"Error: Giveaway channel with ID {GIVEAWAY_CHANNEL_ID} not found.")
        return

    if not giveaway_history_message_id:
        # 🎉 If no history message exists, create one with the new winner(s)
        embed = discord.Embed(
            title="🎉 All-Time Giveaway Winners 🎉",
            description="Here’s a record of our amazing community members who’ve snagged rewards from giveaways! 🏆",
            color=discord.Color.purple()
        )

        for winner in new_winners_data:
            user = bot.get_user(int(winner['user_id']))
            user_name = user.mention if user else f"User ID: {winner['user_id']}"
            embed.add_field(
                name=f"✨ {user_name}",
                value=f"**{winner['points']:.2f} points** 🎁\n*Reason:* {winner['purpose']}",
                inline=False
            )

        embed.set_footer(text="Keep participating in giveaways for a chance to win! 🚀")
        embed.timestamp = datetime.now(UTC)

        try:
            message = await channel.send(embed=embed)
            giveaway_history_message_id = message.id
            # Save this ID to persist after restart
            # e.g., save_json_file({"giveaway_history_message_id": giveaway_history_message_id}, "message_ids.json")
        except discord.Forbidden:
            print(f"Error: Bot does not have permissions to send embeds in channel {channel.name}.")
        return

    # If the history message already exists, fetch it and append
    try:
        message = await channel.fetch_message(giveaway_history_message_id)

        # Build the content for the new winners
        new_content = ""
        for winner in new_winners_data:
            user = bot.get_user(int(winner['user_id']))
            user_name = user.mention if user else f"User ID: {winner['user_id']}"
            new_content += f"**{user_name}** won **{winner['points']:.2f} points**! (Reason: {winner['purpose']})\n"

        # Edit the message to add the new content at the end
        updated_content = message.content + "\n" + new_content
        await message.edit(content=updated_content)

    except discord.NotFound:
        print("Error: The giveaway history message was deleted. A new one will be created next time.")
        giveaway_history_message_id = None
    except discord.Forbidden:
        print(f"Error: Bot does not have permissions to edit messages in channel {channel.name}.")


# ===  WEEKLY XP BONUS ===
@tasks.loop(hours=168)
async def weekly_xp_bonus():
    """Awards bonus points to top 3 XP earners weekly. XP is no longer reset."""
    guild = bot.get_guild(SERVER_ID)
    if not guild:
        print("Error: Server not found. Cannot award weekly XP bonus.")
        return

    # --- NEW: Filter eligible users, excluding admins and mods ---
    eligible = {}
    allowed_roles = [ADMIN_ROLE_ID, MOD_ROLE_ID]
    for uid, data in user_xp.items():
        xp_val = data.get("xp", 0) if isinstance(data, dict) else data
        if xp_val >= 500:
            member = guild.get_member(int(uid))
            if member and not any(role.id in allowed_roles for role in member.roles):
                eligible[uid] = xp_val

    top_users = sorted(eligible.items(), key=lambda x: x[1], reverse=True)[:3]

    if not top_users:
        print("No eligible users for weekly XP bonus this week.")
        return

    # NEW: Calculate total points to be awarded and check admin balance
    points_to_award = len(top_users) * 200
    if admin_points["balance"] < points_to_award:
        print("⚠️ Admin balance is too low to award weekly XP bonus. Skipping.")
        return

    for uid, _ in top_users:
        user_id = str(uid)
        if user_id not in user_points:
            user_points[user_id] = {"all_time_points": 0.0, "available_points": 0.0}

        user_points[user_id]["all_time_points"] += 200
        user_points[user_id]["available_points"] += 200

        # --- NEW LINE ---
        await log_points_transaction(user_id, 200.0, "Weekly XP bonus")

        save_json_file(user_points, "user_points.json")

    # NEW: Deduct points from the admin's balance and update claimed points
    admin_points["balance"] -= points_to_award
    admin_points["claimed_points"] += points_to_award

    save_points()
    # NEW: Save the updated admin points
    save_json_file(ADMIN_POINTS_FILE, admin_points)

    reward_channel = bot.get_channel(XP_REWARD_CHANNEL_ID)
    if reward_channel:
        # 🎉 First, ping all winners in a celebratory message
        mentions = []
        for uid, _ in top_users:
            try:
                user = await bot.fetch_user(int(uid))
                mentions.append(user.mention)
            except (discord.NotFound, discord.HTTPException):
                mentions.append(f"Unknown User (ID: {uid})")

        if mentions:
            await reward_channel.send(f"🔥 Congrats to Mana XP legends: {', '.join(mentions)} 🎉")

        # 🏆 Then send the premium embed
        embed = discord.Embed(
            title="🏆 Weekly XP Rewards",
            description="The **Top 3 XP Earners** of the week have been awarded their bonus! 🎉",
            color=discord.Color.gold()
        )

        for idx, (uid, _) in enumerate(top_users, 1):
            try:
                user = await bot.fetch_user(int(uid))
                embed.add_field(
                    name=f"⭐ Rank #{idx}",
                    value=f"{user.mention} — **+200 points**",
                    inline=False
                )
            except (discord.NotFound, discord.HTTPException):
                embed.add_field(
                    name=f"⭐ Rank #{idx}",
                    value=f"Unknown User (ID: {uid}) — **+200 points**",
                    inline=False
                )

        embed.set_footer(text="Keep chatting, questing, and engaging to climb the ranks! 🚀")
        embed.timestamp = datetime.now(UTC)

        await reward_channel.send(embed=embed)

        try:
            await reward_channel.send(embed=embed)
        except discord.Forbidden:
            print(f"Bot missing permissions to send message to XP reward channel ({XP_REWARD_CHANNEL_ID}).")
    else:
        print(f"XP Reward Channel (ID: {XP_REWARD_CHANNEL_ID}) not found.")

    print("Weekly XP bonus awarded. XP was not reset.")


@bot.event
async def on_raw_reaction_add(payload):
    """Handles reaction role assignments."""
    if payload.channel_id != VERIFY_CHANNEL_ID or payload.message_id != VERIFY_MESSAGE_ID:
        return

    guild = bot.get_guild(payload.guild_id)
    if not guild: return

    member = guild.get_member(payload.user_id)
    if not member or member.bot: return

    emoji_lookup_key = (payload.emoji.name, payload.emoji.id)
    if payload.emoji.id is None:
        emoji_lookup_key = (str(payload.emoji), None)

    role_id_to_add = EMOJI_ROLE_MAP.get(emoji_lookup_key)

    if role_id_to_add:
        role = guild.get_role(role_id_to_add)
        if role and role not in member.roles:
            try:
                await member.add_roles(role)
                print(f"✅ Added role '{role.name}' to {member.display_name}")
            except discord.Forbidden:
                print(f"Bot missing permissions to add role '{role.name}' to {member.display_name}.")
            except discord.HTTPException as e:
                print(f"HTTP Error adding role '{role.name}' to {member.display_name}: {e}")


#----------------------R E F E R R A L-----------------U N I T-----------------------------------------
# === MEMBER JOIN (REFERRAL) ===
@bot.event
async def on_member_join(member):
    global invite_cache, pending_referrals
    if member.bot:
        return

    # === NEW REFERRAL CHECK ===
    user_id = str(member.id)
    if user_id in referred_users:
        print(f"User {member.name} has rejoined but has already been referred. Skipping referral check.")
        return # Exit the function, user is not eligible for another bonus
    # === END OF NEW CHECK ===

    guild = member.guild
    invites_after_join = await guild.invites()
    referrer = None

    invites_before_join = invite_cache.get(guild.id, [])
    for invite_after in invites_after_join:
        invite_before = next((i for i in invites_before_join if i.code == invite_after.code), None)
        if invite_before and invite_after.uses > invite_before.uses:
            referrer = invite_after.inviter
            break

    invite_cache[guild.id] = invites_after_join

    if referrer and referrer.id != bot.user.id:
        pending_referrals[str(member.id)] = str(referrer.id)
        save_json_file(PENDING_REFERRALS_FILE, pending_referrals)


@bot.event
async def on_member_update(before, after):
    global user_points, referral_data, pending_referrals, admin_points, referred_users

    new_roles = [r for r in after.roles if r not in before.roles]
    if not new_roles:
        return

    user_id = str(after.id)
    channel = bot.get_channel(REFERRAL_CHANNEL_ID)

    if TIVATED_ROLE_ID in [role.id for role in new_roles]:
        referrer_id = pending_referrals.get(user_id)
        if channel and referrer_id:
            try:
                referrer = await bot.fetch_user(int(referrer_id))
                embed = discord.Embed(
                    title="👋 Welcome to ManaVerse!",
                    description=(
                        f"🎉 {after.mention} just joined the community!\n\n"
                        f"🙌 You were referred by {referrer.mention}.\n\n"
                        f"💡 **Reminder:** {referrer.mention} will receive their referral reward "
                        f"once {after.mention} gets a **paid role** in the server.\n\n"
                        f"👉 To get started, check out <#{HOW_TO_JOIN_CHANNEL_ID}>."
                    ),
                    color=discord.Color.blue()
                )
                embed.set_thumbnail(url=after.avatar.url if after.avatar else None)
                embed.set_footer(
                    text="ManaVerse Referral System – Building stronger connections 💎"
                )
                embed.timestamp = datetime.now(UTC)

                await channel.send(embed=embed)

            except discord.NotFound:
                await channel.send(f"🎉 Welcome {after.mention}!")
        elif channel:
            await channel.send(f"🎉 Welcome {after.mention}!")

    if user_id not in pending_referrals:
        return

    # Check if the user has already received a referral bonus
    if user_id in referred_users:
        print(f"User {after.name} has already received a referral bonus. Skipping point award.")
        return

    referrer_id = pending_referrals[user_id]

    awarded = False
    for role in new_roles:
        if role.id in REFERRAL_POINTS_PER_ROLE:
            referrer_points = REFERRAL_POINTS_PER_ROLE[role.id]
            new_member_points = NEW_MEMBER_POINTS_PER_ROLE.get(role.id, 0.0)

            referrer_member = after.guild.get_member(int(referrer_id))
            if referrer_member and any(role.id == ADMIN_ROLE_ID for role in referrer_member.roles):
                referrer_points = 0.0

            total_points_to_award = referrer_points + new_member_points

            # --- CRITICAL SAFETY CHECK: Always check balance first ---
            if admin_points["balance"] < total_points_to_award:
                print(f"❌ Not enough points in admin balance to award referral.")
                if channel:
                    await channel.send(
                        "❌ Referral reward could not be given due to insufficient points. Please notify admin.")
                return  # Stop the process if the balance is too low

            # --- BEGIN TRANSACTION ---
            try:
                # Add points to users first
                user_points.setdefault(user_id, {"all_time_points": 0.0, "available_points": 0.0})
                user_points[user_id]["all_time_points"] += new_member_points
                user_points[user_id]["available_points"] += new_member_points

                user_points.setdefault(referrer_id, {"all_time_points": 0.0, "available_points": 0.0})
                user_points[referrer_id]["all_time_points"] += referrer_points
                user_points[referrer_id]["available_points"] += referrer_points

                save_json_file(POINTS_FILE, user_points)

                referral_data[user_id] = referrer_id

                # Deduct points from the admin balance
                admin_points["balance"] -= total_points_to_award
                admin_points["claimed_points"] += total_points_to_award

                save_json_file(ADMIN_POINTS_FILE, admin_points)

                # Log the transactions
                referrer_member_obj = await bot.fetch_user(int(referrer_id))
                await log_points_transaction(user_id, new_member_points,
                                             f"Joined via referral by {referrer_member_obj.name}")
                await log_points_transaction(referrer_id, referrer_points, f"Successful referral of {after.name}")

                awarded = True

            except Exception as e:
                # If an error occurs, print it and revert the transaction to be safe
                print(f"❌ An error occurred during point transaction: {e}")

                # Revert points for safety
                if user_id in user_points:
                    user_points[user_id]["all_time_points"] -= new_member_points
                    user_points[user_id]["available_points"] -= new_member_points
                if referrer_id in user_points:
                    user_points[referrer_id]["all_time_points"] -= referrer_points
                    user_points[referrer_id]["available_points"] -= referrer_points

                if channel:
                    await channel.send(f"❌ An error occurred during point transaction. Please contact an admin.")

            # --- END TRANSACTION ---

    referrer_points = 0.0
    new_member_points = 0.0

    if awarded:
        # If the transaction was successful, save all files
        try:
            # === NEW CODE TO SAVE REFERRED USER ===
            referred_users[user_id] = True
            save_json_file("referred_users.json", referred_users)
            # =======================================

            del pending_referrals[user_id]
            save_json_file(REFERRALS_FILE, referral_data)
            save_json_file(PENDING_REFERRALS_FILE, pending_referrals)

            if channel:
                referrer = await bot.fetch_user(int(referrer_id))
                embed = discord.Embed(
                    title="🎉 Successful Referral!",
                    description=(
                        f"🔥 {referrer.mention} just referred {after.mention}!\n\n"
                        f"💰 **Rewards Distributed:**\n"
                        f"• {referrer.mention} earned **{referrer_points:.2f} points** 🪙\n"
                        f"• {after.mention} earned **{new_member_points:.2f} points** 🎁"
                    ),
                    color=discord.Color.green()
                )
                embed.set_thumbnail(url=after.avatar.url if after.avatar else None)
                embed.set_footer(
                    text="ManaVerse Referral System – Keep growing the community 🚀"
                )
                embed.timestamp = datetime.now(UTC)

                await channel.send(embed=embed)

        except Exception as e:
            print(f"❌ An error occurred while saving files: {e}")

        # Check if the change was about a role update
        if before.roles == after.roles:
            return

        # Check if the member is the bot itself to prevent errors
        if after.bot:
            return

        verified_role = after.guild.get_role(TIVATED_ROLE_ID)

        # Check if the verified role was removed from the user
        if verified_role in before.roles and verified_role not in after.roles:
            try:
                # Create a list of roles to remove
                roles_to_remove = []
                for role in after.roles:
                    # Exclude the @everyone role to avoid errors
                    if role.id != after.guild.default_role.id:
                        roles_to_remove.append(role)

                # Remove all roles from the member
                await after.remove_roles(*roles_to_remove, reason="Verified role was removed.")

                print(f"Removed all roles from {after.name} because their verified role was removed.")

            except discord.Forbidden:
                print(f"Permission error: Bot could not remove roles from {after.name}.")


def get_referral_leaderboard_content(referral_data):
    """Generates a formatted referral leaderboard message from the referral data."""
    if not referral_data:
        return "The referral leaderboard is empty."

    # Count referrals for each user by iterating through the values of the dictionary
    referral_counts = {}
    for user_id, referrer_id in referral_data.items():
        # Ensure referrer_id is not the bot's ID
        if int(referrer_id) != bot.user.id:
            referral_counts[referrer_id] = referral_counts.get(referrer_id, 0) + 1

    # Sort the users by their referral count
    sorted_referrals = sorted(
        referral_counts.items(),
        key=lambda item: item[1],
        reverse=True
    )

    # Build the message string
    message_content = "**🏆 Top 10 Referral Leaderboard**\n\n"
    # This loop now only iterates over the top 10 results
    for rank, (user_id, count) in enumerate(sorted_referrals[:10], 1):
        if count == 0:
            continue

        user = bot.get_user(int(user_id))
        user_name = user.display_name if user else f"User ID: {user_id}"
        message_content += f"**{rank}.** {user_name}: {count} referrals\n"

    message_content += f"\n*Next update in 30 minutes.*"
    return message_content


@tasks.loop(minutes=30)
async def update_referral_leaderboard():
    global referral_leaderboard_message_id

    try:
        channel = bot.get_channel(REFERRAL_CHANNEL_ID)
        if not channel:
            print(f"❌ Error: Referral leaderboard channel not found (ID: {REFERRAL_CHANNEL_ID}).")
            return

        # Delete the previous leaderboard message if it exists
        if referral_leaderboard_message_id:
            try:
                old_message = await channel.fetch_message(referral_leaderboard_message_id)
                await old_message.delete()
                print("Old leaderboard message deleted successfully.")
            except discord.NotFound:
                print("Old leaderboard message not found, creating a new one.")
            except discord.Forbidden:
                print("Bot missing permissions to delete old messages.")

        # Generate and post the new leaderboard message
        message_content = get_referral_leaderboard_content(referral_data)
        new_message = await channel.send(message_content)
        referral_leaderboard_message_id = new_message.id

        print("Referral leaderboard updated successfully.")

        # Schedule the deletion of the new message in 15 minutes
        await asyncio.sleep(15 * 60)  # Wait for 15 minutes

        try:
            # Re-fetch the message to ensure it wasn't deleted by something else
            message_to_delete = await channel.fetch_message(referral_leaderboard_message_id)
            await message_to_delete.delete()
            print("Leaderboard message deleted successfully after 15 minutes.")
            referral_leaderboard_message_id = None  # Clear the ID
        except discord.NotFound:
            print("Leaderboard message was already deleted or not found.")
        except discord.Forbidden:
            print("Bot missing permissions to delete the message.")

    except Exception as e:
        print(f"❌ An error occurred in the referral leaderboard task: {e}")


# === INVITE MECHANISM ===
@bot.command(name="invite")
async def invite_link(ctx):
    """
    Generates a unique referral link for the user.
    If a link already exists for the user, it is sent instead.
    """
    if ctx.channel.id != REFERRAL_CHANNEL_ID:
        try:
            await ctx.message.delete(delay=0)
            await ctx.send(f"❌ The `!invite` command can only be used in the <#{REFERRAL_CHANNEL_ID}> channel.",
                           delete_after=10)
        except discord.Forbidden:
            print(f"Bot missing permissions to send a redirect message in {ctx.channel.name}")
        return

    # Check for an existing invite link for the user
    existing_invites = await ctx.guild.invites()
    user_invite = None
    for invite in existing_invites:
        if invite.inviter == ctx.author and invite.max_uses == 0 and not invite.max_age:
            user_invite = invite
            break

    # If an existing link is found, send it
    if user_invite:
        await ctx.send(
            f"🔗 Here is your personal referral link, {ctx.author.mention}: `{user_invite.url}`\n"
            "Share this link with friends to earn bonus points when they join!",
            delete_after=45
        )
    else:
        # Otherwise, create a new link associated with the user
        try:
            invite = await ctx.channel.create_invite(
                max_uses=0,
                max_age=0,
                reason="Referral link for a user",
                target_user=ctx.author
            )
            await ctx.send(
                f"🔗 Here is your personal referral link, {ctx.author.mention}: `{invite.url}`\n"
                "Share this link with friends to earn bonus points when they join!",
                delete_after=30
            )
        except discord.Forbidden:
            print(f"❌ Bot missing permissions to create invite links in channel '{ctx.channel.name}'.")
            await ctx.send("❌ I do not have permission to create invite links.", delete_after=10)
        except Exception as e:
            print(f"❌ An unexpected error occurred while running the !invite command: {e}")
            await ctx.send("❌ An unexpected error occurred. Kindly create your referral link first on this channel and try again.", delete_after=15)

    try:
        await ctx.message.delete(delay=5)
    except discord.Forbidden:
        print(f"❌ Bot missing permissions to delete command message in channel '{ctx.channel.name}'.")


@bot.command(name="ref")
@commands.cooldown(1, 60, commands.BucketType.user)
async def ref_command(ctx):
    """
    Shows the user a list of people they have successfully referred.
    This command is restricted to the referral channel.
    """
    # Delete the user's command message
    try:
        await ctx.message.delete()
    except discord.Forbidden:
        pass  # Bot doesn't have permissions to delete the message

    # Channel Restriction Check
    if ctx.channel.id != REFERRAL_CHANNEL_ID:
        try:
            await ctx.send(
                f"❌ This command can only be used in the <#{REFERRAL_CHANNEL_ID}> channel.",
                delete_after=10
            )
        except discord.Forbidden:
            pass
        return

    referrer_id = str(ctx.author.id)
    referred_members = []

    # Iterate through the referral data to find the user's referrals
    for user_id, ref_id in referral_data.items():
        if ref_id == referrer_id:
            referred_members.append(user_id)

    if not referred_members:
        await ctx.send(
            "You have not referred anyone yet. Share your invite link to get started!",
            delete_after=40  # Deletes this message after 40 seconds
        )
        return

    # Create an embed to display the list of referrals
    embed = discord.Embed(
        title="👥 Your Referrals",
        description="Here is a list of members you have successfully referred:",
        color=discord.Color.blue()
    )

    referral_list = ""
    for referred_id in referred_members:
        try:
            user = await bot.fetch_user(int(referred_id))
            referral_list += f"• {user.name}\n"
        except discord.NotFound:
            referral_list += f"• Unknown User (ID: {referred_id})\n"

    embed.add_field(name="Referred Users", value=referral_list, inline=False)
    embed.set_footer(text=f"Total Referrals: {len(referred_members)}")

    await ctx.send(embed=embed, delete_after=40)  # Deletes this message after 40 seconds


#---------------------------------------------------------------------------------------------------

# === REACT TO AWARD POINTS TO USERS ===
@bot.event
async def on_reaction_add(reaction, user):
    global user_points, admin_points, processed_reactions

    # Check 1: Event trigger and bot user
    if user.bot:
        return

    # Check 2: Category and emoji
    if reaction.message.channel.category is None or \
            reaction.message.channel.category.id not in REACTION_CATEGORY_IDS or \
            str(reaction.emoji) != REACTION_EMOJI:
        return

    # Check 3: Role and self-award
    reactor_member = reaction.message.guild.get_member(user.id)
    allowed_roles = [ADMIN_ROLE_ID, MOD_ROLE_ID]
    if not reactor_member or \
            (not reactor_member.guild_permissions.administrator and not any(
                role.id in allowed_roles for role in reactor_member.roles)) or \
            reaction.message.author == user:
        if reaction.message.author == user:
            try:
                await reaction.message.channel.send("❌ Error: You cannot award points to yourself.", delete_after=10)
            except discord.Forbidden:
                pass
        return

    # Check 4: Processed reaction check
    reaction_identifier = f"{reaction.message.id}-{user.id}"
    if reaction_identifier in processed_reactions:
        return

    # Check 5: Constant values and random points
    try:
        points_to_add = random.uniform(MIN_REACTION_POINTS, MAX_REACTION_POINTS)
    except NameError:
        print("❌ CRITICAL ERROR: MIN_REACTION_POINTS or MAX_REACTION_POINTS are not defined!")
        return

    # Check 6: Admin balance check
    if 'balance' not in admin_points or admin_points['balance'] < points_to_add:
        try:
            await reaction.message.channel.send(
                f"❌ Error: Admin balance is too low to award {points_to_add:.2f} points.", delete_after=10)
        except discord.Forbidden:
            pass
        return

    # --- All checks passed. Begin awarding processes ---

    user_id = str(reaction.message.author.id)

    try:
        # Award points to the message author
        user_points.setdefault(user_id, {"all_time_points": 0.0, "available_points": 0.0})
        user_points[user_id]["all_time_points"] += points_to_add
        user_points[user_id]["available_points"] += points_to_add

        # Deduct points from the admin balance
        admin_points["balance"] -= points_to_add
        admin_points["claimed_points"] = admin_points.get("claimed_points", 0.0) + points_to_add

        # Log the transaction
        await log_points_transaction(user_id, points_to_add, f"Reaction award from {user.name}")

        # Add reaction to the processed set
        processed_reactions.add(reaction_identifier)

        # Save all data files
        save_json_file(POINTS_FILE, user_points)
        save_json_file(ADMIN_POINTS_FILE, admin_points)
        save_json_file(PROCESSED_REACTIONS_FILE, list(processed_reactions))

        # Send a confirmation message
        await reaction.message.channel.send(
            f"🎉 **{reaction.message.author.mention}** received **{points_to_add:.2f} points** "
            f"for a great message! (Awarded by {user.mention})",
            delete_after=15
        )
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
        try:
            await reaction.message.channel.send(f"❌ An internal error occurred. Please notify an admin. Error: {e}")
        except discord.Forbidden:
            pass


@bot.event
async def on_raw_reaction_remove(payload):
    """Handles reaction role removals."""
    if payload.channel_id != VERIFY_CHANNEL_ID or payload.message_id != VERIFY_MESSAGE_ID:
        return

    guild = bot.get_guild(payload.guild_id)
    if not guild: return

    member = guild.get_member(payload.user_id)
    if not member or member.bot: return

    emoji_lookup_key = (payload.emoji.name, payload.emoji.id)
    if payload.emoji.id is None:
        emoji_lookup_key = (str(payload.emoji), None)

    role_id_to_remove = EMOJI_ROLE_MAP.get(emoji_lookup_key)

    if role_id_to_remove:
        role = guild.get_role(role_id_to_remove)
        if member and role and role in member.roles:
            try:
                await member.remove_roles(role)
                print(f"❌ Removed role '{role.name}' from {member.display_name}")
            except discord.Forbidden:
                print(f"Bot missing permissions to remove role '{role.name}' from {member.display_name}.")
            except discord.HTTPException as e:
                print(f"HTTP Error removing role '{role.name}' from {member.display_name}: {e}")


# --- !submit_proof Command (Optimized with duplicate check) ---
@bot.command()
async def proof(ctx, tweet_url: str, *engagements):
    """
    Allows users to submit proof of engagements for points, applying role multipliers.
    Checks for duplicate tweet URLs or attached images.
    Usage: !submit_proof <tweet_url> [like] [comment] [retweet]
    """
    user_id = str(ctx.author.id)

    # --- 1. Extract and Normalize all potential proof URLs ---
    all_proof_urls = []

    # Add tweet URL
    if tweet_url:
        all_proof_urls.append(normalize_url(tweet_url))

    # Add image attachment URLs
    for attachment in ctx.message.attachments:
        if attachment.content_type and attachment.content_type.startswith('image/'):
            all_proof_urls.append(normalize_url(attachment.url))

    if not all_proof_urls:
        await ctx.send(f"{ctx.author.mention}, please provide a tweet URL and/or attach an image to your command.",
                       delete_after=15)
        try:
            await ctx.message.delete(delay=5)
        except discord.Forbidden:
            pass
        return

    # --- 2. Check for Duplicates ---
    for url in all_proof_urls:
        if url in approved_proofs:
            try:
                await ctx.message.delete(delay=0)
            except discord.Forbidden:
                print(f"Bot missing permissions to delete message in {ctx.channel.name} ({ctx.channel.id}).")
            await ctx.send(
                f"🚫 {ctx.author.mention}, your submission was removed! One or more of the proofs (tweet or image) has already been submitted and approved. Please ensure all proofs are unique.",
                delete_after=25
            )
            return

    # --- 3. Process Valid Engagements ---
    valid_engagements = [e.lower() for e in engagements if e.lower() in POINT_VALUES]

    if not valid_engagements:
        await ctx.send(f"{ctx.author.mention}, please specify valid engagement types: `like`, `comment`, `retweet`.",
                       delete_after=15)
        try:
            await ctx.message.delete(delay=5)
        except discord.Forbidden:
            pass
        return

    # Calculate base points from engagements
    base_points = sum(POINT_VALUES[e] for e in valid_engagements)

    # Check for pending submission
    if user_id in submissions:
        await ctx.send(
            f"⏳ {ctx.author.mention}, you already have a pending submission. Please wait for it to be reviewed or contact a moderator.",
            delete_after=15)
        try:
            await ctx.message.delete(delay=5)
        except discord.Forbidden:
            pass
        return

    # --- 4. Apply Role Multiplier at Submission ---
    multiplier = 1.0
    member_role_ids = [role.id for role in ctx.author.roles]

    current_max_multiplier = 1.0
    for role_id, mult_value in ROLE_MULTIPLIERS.items():
        if role_id in member_role_ids:
            current_max_multiplier = max(current_max_multiplier, mult_value)

    multiplier = current_max_multiplier

    # Calculate final points to be requested
    final_points = round(base_points * multiplier, 2)

    # Store submission details, including all proof URLs found
    submissions[user_id] = {
        "tweet_url": tweet_url,
        "attachment_urls": [att.url for att in ctx.message.attachments if
                            att.content_type and att.content_type.startswith('image/')],
        "normalized_proof_urls": all_proof_urls,
        "engagements": valid_engagements,
        "base_points": base_points,
        "multiplier": multiplier,
        "points_requested": final_points,
        "status": "pending",
        "channel_id": ctx.channel.id,
        "timestamp": int(discord.utils.utcnow().timestamp())
    }
    save_submissions()

    # --- 5. Notify Moderators ---
    mod_channel = bot.get_channel(MOD_TASK_REVIEW_CHANNEL_ID)
    if mod_channel:
        try:
            mod_message_files = []
            for att in ctx.message.attachments:
                if att.content_type and att.content_type.startswith('image/'):
                    try:
                        mod_message_files.append(await att.to_file())
                    except discord.HTTPException as e:
                        print(f"Error fetching attachment {att.url} for mod notification: {e}")

            mod_notification_msg = (
                f"🔍 **New Submission:**\n"
                f"User: <@{user_id}> ({ctx.author.name})\n"
                f"Tweet: {tweet_url}\n"
                f"Attached Images: {', '.join(att.url for att in ctx.message.attachments if att.content_type and att.content_type.startswith('image/')) or 'None'}\n"
                f"Engagements: {', '.join(valid_engagements)}\n"
                f"Base Points: {base_points}\n"
                f"Multiplier: x{multiplier}\n"
                f"**Total Points Requested: {final_points}**\n"
                f"To approve/reject: `!verify {user_id} <approve|reject>`"
            )
            await mod_channel.send(mod_notification_msg, files=mod_message_files)
        except discord.Forbidden:
            print(f"Bot missing permissions to send message to mod review channel ({MOD_TASK_REVIEW_CHANNEL_ID}).")
        except discord.HTTPException as e:
            print(f"HTTP Error sending mod notification: {e}")
    else:
        print(f"Mod review channel (ID: {MOD_TASK_REVIEW_CHANNEL_ID}) not found.")

    await ctx.send(f"✅ {ctx.author.mention}, your submission for **{final_points} points** has been logged for review!",
                   delete_after=15)


# --- !verify Command (Optimized and Secured) ---
@bot.command()
@commands.has_permissions(manage_messages=True)
async def verify(ctx, member: discord.Member, action: str):
    """
    (Moderator Only) Approves or rejects a user's task submission.
    This command is restricted to a specific moderator channel to prevent misuse.
    Usage: !verify <@member> <approve|reject>
    """
    if ctx.channel.id != MOD_TASK_REVIEW_CHANNEL_ID:
        try:
            await ctx.message.delete(delay=5)
            await ctx.send(f"❌ This command can only be used in the <#{MOD_TASK_REVIEW_CHANNEL_ID}> channel.",
                           delete_after=10)
        except discord.Forbidden:
            pass
        return

    user_id = str(member.id)
    action = action.lower()

    if user_id not in submissions:
        await ctx.send("❌ No pending submission found for this user.", delete_after=10)
        return

    submission = submissions[user_id]
    points_to_award = submission["points_requested"]
    reply_channel = bot.get_channel(submission.get("channel_id", ENGAGEMENT_CHANNEL_ID))

    if not reply_channel:
        print(f"Warning: Could not find reply channel for user {user_id}'s submission. Falling back to ctx.channel.")
        reply_channel = ctx.channel

    if action == "approve":
        # NEW: Check if the admin has enough points to award
        if admin_points["balance"] < points_to_award:
            await ctx.send(f"❌ Error: Admin balance is too low to award {points_to_award:.2f} points.",
                           delete_after=10)
            print("⚠️ Admin balance is too low to award points. Skipping.")
            return

        if user_id not in user_points:
            user_points[user_id] = {"all_time_points": 0.0, "available_points": 0.0}

        user_points[user_id]["all_time_points"] += points_to_award
        user_points[user_id]["available_points"] += points_to_award

        # --- NEW LINE ---
        await log_points_transaction(user_id, points_to_award, f"Task submission approved")

        # --- CORRECTED ORDER: Save user points before admin points ---
        save_json_file(POINTS_FILE, user_points)

        # NEW: Deduct points from the admin's balance and update claimed points
        admin_points["balance"] -= points_to_award
        admin_points["claimed_points"] += points_to_award
        save_json_file(ADMIN_POINTS_FILE, admin_points)

        for url in submission.get("normalized_proof_urls", []):
            if url not in approved_proofs:
                approved_proofs.append(url)
        save_approved_proofs()

        try:
            await reply_channel.send(
                f"🎉 {member.mention}, your engagement proof has been **approved**! You earned **{points_to_award:.2f} points**. Your new total is **{user_points[user_id]['available_points']:.2f} points**."
            )
        except discord.Forbidden:
            print(f"Bot missing permissions to send message to {reply_channel.name} ({reply_channel.id}) for approval.")

        del submissions[user_id]
        save_submissions()

    elif action == "reject":
        try:
            await reply_channel.send(
                f"🚫 {member.mention}, your engagement proof has been **rejected**. Please review your proof and submit again if needed."
            )
        except discord.Forbidden:
            print(
                f"Bot missing permissions to send message to {reply_channel.name} ({reply_channel.id}) for rejection.")
        del submissions[user_id]
        save_submissions()

    else:
        await ctx.send("❌ Invalid action. Please use `approve` or `reject`.", delete_after=10)
        return


# === !approve_payment ===
@bot.command()
@commands.has_permissions(manage_roles=True)
async def approve_payment(ctx, member: discord.Member, amount: int):
    """
    Moderator command to approve a payment and assign a role based on amount.
    Usage: !approve_payment <@member> <amount>
    """
    if ctx.channel.id != MOD_PAYMENT_REVIEW_CHANNEL_ID:
        try:
            await ctx.message.delete(delay=5)
            await ctx.send(f"❌ This command can only be used in the <#{MOD_TASK_REVIEW_CHANNEL_ID}> channel.",
                           delete_after=15)
        except discord.Forbidden:
            pass
        return

    role_map = {
        10: (ROOKIE_ROLE_ID, "Odogwu Rookie"),
        15: (ELITE_ROLE_ID, "Odogwu Elite"),
        20: (SUPREME_ROLE_ID, "Odogwu Supreme"),
        50: (VIP_ROLE_ID, "1st Circle (VIP)")
    }

    if amount not in role_map:
        await ctx.send("❌ Invalid amount. Use 10, 15, 20, or 50.", delete_after=10)
        return

    role_id, role_name = role_map[amount]
    role = ctx.guild.get_role(role_id)

    if not role:
        await ctx.send(f"Role with ID {role_id} not found. Please check configuration.", delete_after=10)
        print(f"Error: Role ID {role_id} for amount {amount} not found in guild.")
        return

    if role in member.roles:
        await ctx.send(f"⚠️ {member.mention} already has the **{role_name}** role.", delete_after=10)
        return

    try:
        await member.add_roles(role)
        confirm_channel = bot.get_channel(PAYMENT_CHANNEL_ID)
        if confirm_channel:
            try:
                await confirm_channel.send(
                    f"🎉 {member.mention}, your payment has been confirmed and you’ve been assigned the **{role_name}** role!"
                )
            except discord.Forbidden:
                print(
                    f"Bot missing permissions to send message to payment confirmation channel ({PAYMENT_CHANNEL_ID}).")
        else:
            print(f"Payment confirmation channel (ID: {PAYMENT_CHANNEL_ID}) not found.")

        await ctx.send(f"✅ Successfully assigned **{role_name}** to {member.mention}.", delete_after=10)

    except discord.Forbidden:
        await ctx.send(f"❌ Bot does not have permissions to add the **{role_name}** role.", delete_after=10)
        print(f"Bot missing permissions to add role '{role.name}' to {member.display_name}.")
    except discord.HTTPException as e:
        await ctx.send(f"❌ An error occurred while adding the role: {e}", delete_after=10)
        print(f"HTTP Error adding role '{role.name}' to {member.display_name}: {e}")


# === MODIFIED: !points command to show all-time points, available points, and rank ===
# === CONSOLIDATED: !points command to show all-time points, available points, and rank ===
@bot.command()
async def points(ctx, member: discord.Member = None):
    """
    Displays the points of a specific member or the user who ran the command.
    Usage: !points <@member> or !points
    """
    if ctx.channel.id != LEADERBOARD_CHANNEL_ID:
        try:
            await ctx.message.delete(delay=5)
            await ctx.send(f"❌ This command can only be used in the <#{LEADERBOARD_CHANNEL_ID}> channel.", delete_after=10)
        except discord.Forbidden:
            pass
        return

    if member is None:
        member = ctx.author

    user_id = str(member.id)
    user_data = user_points.get(user_id, {"all_time_points": 0.0, "available_points": 0.0})
    all_time_points = user_data["all_time_points"]
    available_points = user_data["available_points"]

    # Calculate rank based on all-time points
    if not user_points:
        rank = "N/A"
    else:
        sorted_users = sorted(user_points.items(), key=lambda item: item[1]['all_time_points'], reverse=True)
        rank = "N/A"
        for i, (uid, _) in enumerate(sorted_users, 1):
            if uid == user_id:
                rank = i
                break

    usd_value = available_points * POINTS_TO_USD

    await ctx.send(
        f"💰 {member.mention}, here are your point details:\n"
        f"**All-Time Points:** {all_time_points:.2f}\n"
        f"**Available Points:** {available_points:.2f} (${usd_value:.2f})\n"
        f"**Current Rank:** #{rank}",
        delete_after=30
    )
    await ctx.message.delete(delay=5)


# === MANUALLY ADD POINTS ===
@bot.command()
@commands.has_role(ADMIN_ROLE_ID)
async def addpoints(ctx, members: commands.Greedy[discord.Member], points_to_add: float, *,
                    purpose: str = "Giveaway winner"):
    """
    (Admin Only) Manually adds a specified number of points to one or more users.
    Usage: !addpoints <@member1> [member2] [member3]... <points> [purpose]
    Example: !addpoints @user1 @user2 @user3 50 Raffle winners
    """
    if ctx.channel.id != GIVEAWAY_CHANNEL_ID:
        try:
            await ctx.message.delete(delay=20)
            await ctx.send(f"❌ This command can only be used in the <#{GIVEAWAY_CHANNEL_ID}> channel.",
                           delete_after=10)
        except discord.Forbidden:
            pass
        return

    try:
        await ctx.message.delete()
    except discord.Forbidden:
        pass

    if points_to_add <= 0:
        await ctx.send("❌ Error: Points to add must be greater than zero.", delete_after=10)
        return

    total_points = points_to_add * len(members)
    if admin_points["balance"] < total_points:
        await ctx.send(f"❌ Error: Admin balance is too low to award a total of {total_points:.2f} points.",
                       delete_after=10)
        return


    # Process each winner
    winners_list = []
    new_winners_data = []
    for member in members:
        user_id = str(member.id)

        # Update user points
        user_points.setdefault(user_id, {"all_time_points": 0.0, "available_points": 0.0})
        user_points[user_id]["all_time_points"] += points_to_add
        user_points[user_id]["available_points"] += points_to_add

        # Log the transaction
        await log_points_transaction(user_id, points_to_add, purpose)

        # Add winner details to the logs
        winner_entry = {
            "user_id": user_id,
            "points": points_to_add,
            "purpose": purpose,
            "timestamp": datetime.now().isoformat()
        }
        giveaway_winners_log.append(winner_entry)
        all_time_giveaway_winners_log.append(winner_entry)

        winners_list.append(member.mention)

    # --- DEDUCT POINTS AFTER THE LOOP ---
    admin_points["balance"] -= total_points
    admin_points["claimed_points"] += total_points

    # Save all updated files once
    save_json_file(POINTS_FILE, user_points)
    save_json_file(ADMIN_POINTS_FILE, admin_points)
    save_json_file(GIVEAWAY_LOG_FILE, giveaway_winners_log)
    save_json_file(GIVEAWAY_ALL_TIME_LOG_FILE, all_time_giveaway_winners_log)

    await append_new_winner_to_history(new_winners_data)

    await ctx.send(
        f"🎉 **{', '.join(winners_list)}** just won **{points_to_add:.2f} points**! (Reason: {purpose})",
        delete_after=86400
    )


import discord
from discord.ext import commands
from datetime import datetime


@bot.command()
@commands.has_role(ADMIN_ROLE_ID)
async def addpoints_flex(ctx, *args):
    """
    (Admin Only) Manually adds different point amounts to multiple users.
    Usage: !addpoints_flex <@user1> <points1> <@user2> <points2> ... [purpose]
    Example: !addpoints_flex @user1 50 @user2 30 @user3 20 Raffle winners
    """
    if ctx.channel.id != GIVEAWAY_CHANNEL_ID:
        try:
            await ctx.message.delete(delay=5)
            await ctx.send(f"❌ This command can only be used in the <#{GIVEAWAY_CHANNEL_ID}> channel.",
                           delete_after=10)
        except discord.Forbidden:
            pass
        return

    try:
        await ctx.message.delete()

        # --- Start of the main command logic ---
        if not args:
            await ctx.send("❌ Error: Please provide at least one user and point pair.", delete_after=20)
            return

        points_to_award = {}
        purpose = "Manual addition"

        i = 0
        while i < len(args):
            if i + 1 >= len(args):
                purpose = " ".join(args[i:])
                break

            user_mention = args[i]
            points_str = args[i + 1]

            try:
                member = await commands.MemberConverter().convert(ctx, user_mention)
                points = float(points_str)

                if points <= 0:
                    await ctx.send("❌ Error: Points must be greater than zero.", delete_after=20)
                    return

                points_to_award[member] = points
                i += 2
            except (commands.BadArgument, ValueError):
                purpose = " ".join(args[i:])
                break

        if not points_to_award:
            await ctx.send("❌ Error: Could not find any valid user and point pairs.", delete_after=20)
            return

        total_points = sum(points_to_award.values())

        if "balance" not in admin_points or not isinstance(admin_points["balance"], (int, float)):
            await ctx.send("❌ Error: Admin balance is not set up correctly.", delete_after=20)
            return

        if admin_points["balance"] < total_points:
            await ctx.send(f"❌ Error: Admin balance is too low to award a total of {total_points:.2f} points.",
                           delete_after=20)
            return

        winners_list = []
        for member, points in points_to_award.items():
            user_id = str(member.id)
            user_points.setdefault(user_id, {"all_time_points": 0.0, "available_points": 0.0})
            user_points[user_id]["all_time_points"] += points
            user_points[user_id]["available_points"] += points
            await log_points_transaction(user_id, points, purpose)

            winner_entry = {
                "user_id": user_id,
                "points": points,
                "purpose": purpose,
                "timestamp": datetime.now().isoformat()
            }
            giveaway_winners_log.append(winner_entry)
            all_time_giveaway_winners_log.append(winner_entry)

            winners_list.append(f"{member.mention} ({points:.2f})")

        save_json_file(POINTS_FILE, user_points)
        save_json_file(GIVEAWAY_LOG_FILE, giveaway_winners_log)
        save_json_file(GIVEAWAY_ALL_TIME_LOG_FILE, all_time_giveaway_winners_log)

        admin_points["balance"] -= total_points
        admin_points["claimed_points"] += total_points
        save_json_file(ADMIN_POINTS_FILE, admin_points)

        await update_giveaway_winners_history()

        await ctx.send(
            f"🎉 Winners for **{purpose}**:\n"
            f"**{', '.join(winners_list)}**",
            delete_after=86400
        )
        # --- End of the main command logic ---

    except Exception as e:
        print(f"❌ An unhandled error occurred in the !addpoints_flex command: {e}")


#-------------------------------- R A N K I N G --- S Y S T E M ---------------------------------------

def get_points_leaderboard_content(user_points):
    """Generates a formatted points leaderboard message."""
    # Filter out users with 0 points, the bot itself, and anyone with a specific role
    eligible_users = {}
    for user_id, data in user_points.items():
        try:
            # Check if the user is a member of the guild
            member = bot.get_guild(SERVER_ID).get_member(int(user_id))
            if member:
                # Check for admin or mod roles
                if not any(role.id in [ADMIN_ROLE_ID, MOD_ROLE_ID] for role in member.roles):
                    if data['all_time_points'] > 0:
                        eligible_users[user_id] = data
        except (ValueError, AttributeError):
            continue

    if not eligible_users:
        return "The points leaderboard is currently empty."

    # Sort the users by their all-time points in descending order
    sorted_points = sorted(
        eligible_users.items(),
        key=lambda item: item[1]['all_time_points'],
        reverse=True
    )

    # Build the message content for the top 10 users
    message_content = "**🏆 Top 10 Points Leaderboard**\n\n"
    for rank, (user_id, points_data) in enumerate(sorted_points[:10], 1):
        points = points_data['all_time_points']

        # Get the user's name
        user = bot.get_user(int(user_id))
        if user:
            user_name = user.display_name
        else:
            user_name = f"User ID: {user_id}"

        message_content += f"**{rank}.** {user_name}: {points:.2f} points\n"

    return message_content


@tasks.loop(minutes=30)
async def update_points_leaderboard():
    global points_leaderboard_message_id

    try:
        channel = bot.get_channel(LEADERBOARD_CHANNEL_ID)
        if not channel:
            print(f"❌ Error: Points leaderboard channel not found (ID: {LEADERBOARD_CHANNEL_ID}).")
            return

        # Delete the previous leaderboard message if it exists
        if points_leaderboard_message_id:
            try:
                old_message = await channel.fetch_message(points_leaderboard_message_id)
                await old_message.delete()
                print("Old points leaderboard message deleted successfully.")
            except discord.NotFound:
                print("Old points leaderboard message not found, creating a new one.")
            except discord.Forbidden:
                print("Bot missing permissions to delete old messages.")

        # Generate and post the new leaderboard message
        message_content = get_points_leaderboard_content(user_points)
        new_message = await channel.send(message_content)
        points_leaderboard_message_id = new_message.id

        print("Points leaderboard updated successfully.")

        # Schedule the deletion of the new message in 15 minutes
        await asyncio.sleep(15 * 60)  # Wait for 15 minutes

        try:
            # Re-fetch the message to ensure it wasn't deleted by something else
            message_to_delete = await channel.fetch_message(points_leaderboard_message_id)
            await message_to_delete.delete()
            print("Points leaderboard message deleted successfully after 15 minutes.")
            points_leaderboard_message_id = None  # Clear the ID
        except discord.NotFound:
            print("Points leaderboard message was already deleted or not found.")
        except discord.Forbidden:
            print("Bot missing permissions to delete the message.")

    except Exception as e:
        print(f"❌ An error occurred in the points leaderboard task: {e}")


#-----------------------M  Y---------------R   A   N   K----------------------------------------------
@bot.command(name="rank")
@commands.cooldown(1, 60, commands.BucketType.user)
async def rank(ctx):
    """Shows the user's rank and the top 10 leaderboards."""

    if ctx.channel.id != LEADERBOARD_CHANNEL_ID:
        try:
            await ctx.message.delete(delay=5)
            await ctx.send(
                f"❌ This command can only be used in the <#{LEADERBOARD_CHANNEL_ID}> channel.",
                delete_after=10
            )
        except discord.Forbidden:
            pass
        return

    user_id = str(ctx.author.id)
    user_data = user_points.get(user_id, {"all_time_points": 0.0})
    user_score = user_data.get("all_time_points", 0.0)

    # Filter eligible users (exclude admin, mods, non-members)
    eligible_users = {}
    for uid, data in user_points.items():
        try:
            member = ctx.guild.get_member(int(uid))
            if member and not any(role.id in [ADMIN_ROLE_ID, MOD_ROLE_ID] for role in member.roles):
                if data.get('all_time_points', 0) > 0:
                    eligible_users[uid] = data
        except (ValueError, discord.NotFound):
            continue

    if not eligible_users:
        await ctx.send("The leaderboard is currently empty. Start earning points!", delete_after=20)
        return

    # Sort for leaderboard
    sorted_users = sorted(
        eligible_users.items(),
        key=lambda item: item[1].get('all_time_points', 0.0),
        reverse=True
    )

    # Find user's rank
    rank_position = None
    for i, (uid, _) in enumerate(sorted_users, start=1):
        if uid == user_id:
            rank_position = i
            break

    # Embed setup
    embed = discord.Embed(
        title="🏆 Odogwu Global Rankings",
        description=f"Your progress and the **Top 10 Legends** of {ctx.guild.name}.",
        color=discord.Color.gold()
    )

    # Add personal rank
    if rank_position:
        embed.add_field(
            name=f"👑 Your Rank",
            value=f"**#{rank_position}** with **{user_score:.2f} points**",
            inline=False
        )
    else:
        embed.add_field(
            name=f"👑 Your Rank",
            value="You are not ranked yet. Start earning points!",
            inline=False
        )

    # Add leaderboard
    medals = ["🥇", "🥈", "🥉"]
    leaderboard_text = ""
    for i, (uid, data) in enumerate(sorted_users[:10]):
        member = ctx.guild.get_member(int(uid))
        username = member.name if member else f"Unknown User ({uid})"

        # Highlight if this is the command user
        if uid == user_id:
            username = f"⭐ **{username}** ⭐"

        medal = medals[i] if i < len(medals) else "🏅"
        leaderboard_text += f"{medal} **#{i + 1} – {username}**: {data['all_time_points']:.2f} pts\n"

    embed.add_field(name="🌟 Top 10 Legends", value=leaderboard_text, inline=False)

    embed.set_footer(
        text="Grind, engage, and claim your spot at the top!",
        icon_url=ctx.guild.icon.url if ctx.guild.icon else None
    )
    embed.timestamp = datetime.now(UTC)

    await ctx.send(embed=embed)


# === MODIFIED: !leaderboard command to show all-time points ===
@bot.command()
@commands.cooldown(1, 60, commands.BucketType.user)
async def leaderboard(ctx):
    """Displays the top 10 users by all-time points."""
    if ctx.channel.id != LEADERBOARD_CHANNEL_ID:
        try:
            await ctx.message.delete(delay=5)
            await ctx.send(f"❌ This command can only be used in the <#{LEADERBOARD_CHANNEL_ID}> channel.",
                           delete_after=10)
        except discord.Forbidden:
            pass
        return

    # NEW: Correctly filter out the admin, mod, and users who have left the guild
    admin_id = bot.get_user(ADMIN_ROLE_ID)
    mod_id = bot.get_user(MOD_ROLE_ID)

    # We're now filtering out the bot, admin, and mod roles
    eligible_users = {}
    for user_id, data in user_points.items():
        try:
            member = ctx.guild.get_member(int(user_id))
            if member and not any(role.id in [ADMIN_ROLE_ID, MOD_ROLE_ID] for role in member.roles):
                if data['all_time_points'] > 0:
                    eligible_users[user_id] = data
        except (ValueError, discord.NotFound):
            # This handles cases where the user_id is not a valid integer or user is not found
            continue

    if not eligible_users:
        await ctx.send("The leaderboard is currently empty. Start earning points!", delete_after=20)
        return

    sorted_users = sorted(eligible_users.items(), key=lambda item: item[1]['all_time_points'], reverse=True)
    msg = "**🏆 Top 10 ManaVerse Leaders 🏆**\n"
    leaderboard_entries = []

    for i, (user_id, data) in enumerate(sorted_users[:10]):
        try:
            # We already have the member object from the filtering step, so we can use that
            member = ctx.guild.get_member(int(user_id))
            username = member.name
        except (discord.NotFound, discord.HTTPException, AttributeError):
            username = f"Unknown User (ID: {user_id})"

        all_time_points = data['all_time_points']
        leaderboard_entries.append(f"{i + 1}. **{username}** – {all_time_points:.2f} pts")

    msg += "\n".join(leaderboard_entries)
    await ctx.send(msg)


# === MODIFIED: !requestpayout command with minimum amount and fee ===
@bot.command()
async def requestpayout(ctx, amount: float = None, uid: str = None, exchange: str = None):
    """
    Initiates a two-step payout request, requiring confirmation.
    Usage: !requestpayout <amount> <UID> <Exchange>
    """
    if ctx.channel.id != PAYOUT_REQUEST_CHANNEL_ID:
        return

    # 1. Validate all required parameters
    if not amount or not uid or not exchange:
        msg = await ctx.send(
            "❌ Please use the format: `!requestpayout <Amount> <UID> <Exchange Name>`\nExample: `!requestpayout 1000 509958013 Binance`")
        await asyncio.sleep(10)
        await msg.delete()
        try:
            await ctx.message.delete()
        except discord.Forbidden:
            pass
        return

    if not uid.isdigit():
        msg = await ctx.send("❌ Only numeric exchange UIDs are accepted. Wallet addresses are NOT allowed.")
        await asyncio.sleep(10)
        await msg.delete()
        try:
            await ctx.message.delete()
        except discord.Forbidden:
            pass
        return

    exchange = exchange.lower()
    if exchange not in APPROVED_EXCHANGES:
        approved_list = ", ".join([e.capitalize() for e in APPROVED_EXCHANGES])
        msg = await ctx.send(f"❌ Invalid exchange. Only these are accepted: {approved_list}")
        await asyncio.sleep(10)
        await msg.delete()
        try:
            await ctx.message.delete()
        except discord.Forbidden:
            pass
        return

    user_id = str(ctx.author.id)
    user_data = user_points.get(user_id, {"all_time_points": 0.0, "available_points": 0.0})
    balance = user_data.get("available_points", 0.0)

    # 2. Validate amount and balance
    if amount < MIN_PAYOUT_AMOUNT:
        msg = await ctx.send(
            f"⚠️ {ctx.author.mention}, the minimum payout amount is **{MIN_PAYOUT_AMOUNT:.2f} points**.")
        await asyncio.sleep(10)
        await msg.delete()
        try:
            await ctx.message.delete()
        except discord.Forbidden:
            pass
        return

    fee = amount * (PAYOUT_FEE_PERCENTAGE / 100)
    total_deduction = amount + fee

    if balance < total_deduction:
        msg = await ctx.send(
            f"⚠️ {ctx.author.mention}, you do not have enough available points for that request. Your current available balance is **{balance:.2f} points**.")
        await asyncio.sleep(10)
        await msg.delete()
        try:
            await ctx.message.delete()
        except discord.Forbidden:
            pass
        return

    # 3. Store pending payout
    user_data["pending_payout"] = {
        "amount": amount,
        "uid": uid,
        "exchange": exchange,
        "fee": fee,
        "total_deduction": total_deduction,
        "timestamp": time.time()
    }
    user_points[user_id] = user_data
    save_points()

    msg = await ctx.send(
        f"🪙 {ctx.author.mention}, you are about to request a payout of **{amount:.2f} points**.\n"
        f"A **{PAYOUT_FEE_PERCENTAGE:.1f}% fee ({fee:.2f} pts)** will be applied, making the total deduction from your balance **{total_deduction:.2f} points**.\n"
        f"Type `!confirmpayout` within **{CONFIRMATION_TIMEOUT} seconds** to complete your request."
    )
    await asyncio.sleep(CONFIRMATION_TIMEOUT)
    try:
        await msg.delete()
    except discord.NotFound:
        pass
    except discord.Forbidden:
        pass
    try:
        await ctx.message.delete()
    except discord.Forbidden:
        pass


# === !CONFIRM-PAYOUT ===
@bot.command()
@commands.cooldown(1, 30, commands.BucketType.user)
async def confirmpayout(ctx):
    """
    Confirms a pending payout request and deducts points from the user's balance.
    """
    if ctx.channel.id != PAYOUT_REQUEST_CHANNEL_ID:
        return

    user_id = str(ctx.author.id)
    user_data = user_points.get(user_id, {})
    pending_payout = user_data.get("pending_payout")

    # 1. Check for valid pending request
    if not pending_payout:
        msg = await ctx.send("❌ No pending payout request found. Use `!requestpayout` first.")
        await asyncio.sleep(10)
        await msg.delete()
        try:
            await ctx.message.delete()
        except discord.Forbidden:
            pass
        return

    # Check for timeout
    if time.time() - pending_payout["timestamp"] > CONFIRMATION_TIMEOUT:
        del user_data["pending_payout"]
        user_points[user_id] = user_data
        save_points()
        msg = await ctx.send("❌ Your payout request timed out. Please start a new request with `!requestpayout`.")
        await asyncio.sleep(10)
        await msg.delete()
        try:
            await ctx.message.delete()
        except discord.Forbidden:
            pass
        return

    # 2. Final balance check
    total_deduction = pending_payout["total_deduction"]
    balance = user_data.get("available_points", 0.0)

    if balance < total_deduction:
        msg = await ctx.send("❌ You no longer meet the minimum balance for payout.")
        await asyncio.sleep(10)
        await msg.delete()
        try:
            await ctx.message.delete()
        except discord.Forbidden:
            pass
        return

    # 3. Deduct points from the user's balance and save
    user_data["available_points"] -= total_deduction

    # NEW: We no longer delete the pending_payout data here.
    # It is kept so the !paid command can access the amount and finalize the transaction.
    user_points[user_id] = user_data
    save_points()

    # 4. Notify mod
    mod_channel = bot.get_channel(MOD_PAYMENT_REVIEW_CHANNEL_ID)
    if mod_channel:
        # Payout data is sent to the admin channel to be finalized with !paid
        await mod_channel.send(
            f"📤 **Payout Request**\n"
            f"User: {ctx.author.mention} (`{ctx.author.name}`)\n"
            f"UID: `{pending_payout['uid']}`\n"
            f"Exchange: `{pending_payout['exchange'].capitalize()}`\n"
            f"Requested: **{pending_payout['amount']:.2f} pts**\n"
            f"Fee: **{pending_payout['fee']:.2f} pts**\n"
            f"Total Deduction: **{pending_payout['total_deduction']:.2f} pts**"
        )

    # 5. Notify user
    msg = await ctx.send(
        f"✅ {ctx.author.mention}, your payout request for **{pending_payout['amount']:.2f} pts** has been submitted. Your new available balance is **{user_data['available_points']:.2f} pts**.")
    await asyncio.sleep(10)
    await msg.delete()
    try:
        await ctx.message.delete()
    except discord.Forbidden:
        pass


# ===!PAID ===
@bot.command()
@commands.has_permissions(manage_roles=True)
async def paid(ctx, member: discord.Member):
    """
    (Moderator Only) Finalizes a payout request by burning the points and
    notifying the user.
    Usage: !paid <@member>
    """
    if ctx.channel.id != MOD_PAYMENT_REVIEW_CHANNEL_ID:
        try:
            await ctx.message.delete()
        except discord.Forbidden:
            pass
        return

    user_id = str(member.id)
    user_data = user_points.get(user_id, {})

    # NEW: Check for a pending payout to finalize
    pending_payout = user_data.get("pending_payout")
    if not pending_payout:
        await ctx.send(f"❌ Error: {member.mention} does not have a pending payout to mark as paid.", delete_after=10)
        return

    requested_amount = pending_payout["amount"]

    # NEW: Check if the admin has enough points to burn
    if admin_points["balance"] < requested_amount:
        await ctx.send("❌ Error: Admin balance is less than the amount to be burned. Cannot process this payout.",
                       delete_after=10)
        return

    payout_channel = bot.get_channel(PAYOUT_REQUEST_CHANNEL_ID)
    if payout_channel:
        try:
            # This is where the points are burned from the total supply
            fee = pending_payout["fee"]

            admin_points["balance"] -= requested_amount  # Burn the requested points
            admin_points["claimed_points"] -= requested_amount
            admin_points["claimed_points"] += fee  # Add the fee back to the balance
            admin_points["burned_points"] += requested_amount
            admin_points["fees_earned"] += fee

            save_json_file(ADMIN_POINTS_FILE, admin_points)

            # Clear the pending payout
            del user_data["pending_payout"]
            user_points[user_id] = user_data
            save_points()

            await payout_channel.send(
                f"🎉 {member.mention}, great news! Your recent payout request has been successfully processed."
            )
            await ctx.send(
                f"✅ A payout success message has been sent to {member.mention}. The payout has been finalized and points burned.",
                delete_after=10)
        except discord.Forbidden:
            print(
                f"Bot is missing permissions to send the payout notification in {payout_channel.name} ({payout_channel.id}).")
            await ctx.send(f"❌ Could not send the notification. Check bot permissions in the payout channel.",
                           delete_after=10)
    else:
        await ctx.send(
            f"❌ The payout channel (ID: {PAYOUT_REQUEST_CHANNEL_ID}) could not be found. Please check your configuration.",
            delete_after=10)



# === NEW: !xp command ===
@bot.command()
@commands.cooldown(2, 60, commands.BucketType.user)
async def xp(ctx, member: discord.Member = None):
    """
    Displays the user's current total XP and rank.
    Usage: !xp to check your own or !xp @user to check another's.
    This command is restricted to the XP_REWARD_CHANNEL and deleted after 15 seconds.
    """
    if ctx.channel.id != XP_REWARD_CHANNEL_ID:
        try:
            await ctx.message.delete(delay=0)
            await ctx.send(
                f"❌ The `!xp` command can only be used in the <#{XP_REWARD_CHANNEL_ID}> channel.",
                delete_after=15
            )
        except discord.Forbidden:
            print(
                f"Bot missing permissions to delete messages or send a message in {ctx.channel.name} ({ctx.channel.id}).")
        return

    # Use ctx.author if no member is specified
    target_member = member if member else ctx.author
    user_id = str(target_member.id)

    # Check if the user has any XP data
    xp_balance = user_xp.get(user_id, {}).get("xp", 0)
    if xp_balance == 0:
        try:
            await ctx.send(
                f"❌ {target_member.mention}, has not earned any XP yet.",
                delete_after=15
            )
            await ctx.message.delete(delay=15)
        except discord.Forbidden:
            print(f"Bot missing permissions to send/delete messages in {ctx.channel.name} ({ctx.channel.id}).")
        return

        # --- NEW: Filter out admins and mods from the ranking ---
    allowed_roles = [ADMIN_ROLE_ID, MOD_ROLE_ID]
    all_users = []
    guild = bot.get_guild(SERVER_ID)
    if not guild:
        await ctx.send("❌ Error: Could not find the server. Please check the SERVER_ID constant.", delete_after=15)
        return

    for uid, data in user_xp.items():
        member_obj = guild.get_member(int(uid))
        if member_obj and not any(role.id in allowed_roles for role in member_obj.roles):
            all_users.append((uid, data.get("xp", 0)))

    # Calculate the user's rank
    sorted_xp = sorted(user_xp.items(), key=lambda item: item[1].get("xp", 0), reverse=True)
    rank = next((i for i, item in enumerate(sorted_xp) if item[0] == user_id), None)

    # Check if the rank was found and display the message
    if rank is not None:
        rank_display = rank + 1
        message_content = f"🌟 {target_member.mention}, has **{xp_balance}** total XP. Your current rank is **#{rank_display}**."
    else:
        message_content = f"🌟 {target_member.mention}, has **{xp_balance}** total XP."

    try:
        await ctx.send(
            message_content,
            delete_after=30
        )
        await ctx.message.delete(delay=5)
    except discord.Forbidden:
        print(f"Bot missing permissions to send/delete messages in {ctx.channel.name} ({ctx.channel.id}).")


# === Weekly Quest Commands ===
@bot.command()
@commands.has_permissions(administrator=True)
async def quests(ctx, *, all_quests: str):
    """
    (Admin Only) Posts 3 new weekly quests to the quest board.
    Usage: !quests Quest 1 description\nQuest 2 description\nQuest 3 description
    """
    quests = all_quests.strip().split("\n")
    if len(quests) != 3:
        await ctx.send("❌ Please provide exactly 3 quests (one per line).", delete_after=15)
        return

    weekly_quests["week"] += 1
    weekly_quests["quests"] = [q.strip() for q in quests]
    save_weekly_quests()

    global quest_submissions
    quest_submissions = {}
    save_quest_submissions()

    board = bot.get_channel(QUEST_BOARD_CHANNEL_ID)
    if board:
        embed = discord.Embed(
            title=f"📋 Weekly Quests – Week {weekly_quests['week']}",
            description="\n".join([f"**Quest {i + 1}:** {q}" for i, q in enumerate(quests)]),
            color=0xf1c40f
        )
        try:
            await board.send(embed=embed)
            await ctx.send("✅ Quests posted and previous quest submissions reset!", delete_after=15)
        except discord.Forbidden:
            print(f"Bot missing permissions to send message to quest board channel ({QUEST_BOARD_CHANNEL_ID}).")
            await ctx.send("❌ Error posting quests: Missing permissions for quest board channel.", delete_after=15)
        except discord.HTTPException as e:
            print(f"HTTP Error posting quests: {e}")
            await ctx.send(f"❌ Error posting quests: An unexpected error occurred. ({e})", delete_after=15)
    else:
        print(f"Quest board channel (ID: {QUEST_BOARD_CHANNEL_ID}) not found.")
        await ctx.send("❌ Error: Quest board channel not found. Please configure the bot correctly.", delete_after=15)


@bot.command()
async def submitquest(ctx, quest_number: int, tweet_link: str):
    """
    Submits a weekly quest for review.
    Usage: !submitquest <quest_number> <tweet_link>
    """
    if ctx.channel.id != QUEST_SUBMIT_CHANNEL_ID:
        await ctx.send(f"❌ Please use the <#{QUEST_SUBMIT_CHANNEL_ID}> channel to submit quests.", delete_after=10)
        try:
            await ctx.message.delete(delay=5)
        except discord.Forbidden:
            pass
        return

    user_id = str(ctx.author.id)
    week = str(weekly_quests["week"])

    if int(week) == 0 or not weekly_quests["quests"]:
        await ctx.send(f"❌ There are no active weekly quests right now. Please wait for new quests to be posted!",
                       delete_after=15)
        try:
            await ctx.message.delete(delay=5)
        except discord.Forbidden:
            pass
        return

    if quest_number not in [1, 2, 3]:
        await ctx.send("❌ Quest number must be 1, 2, or 3.", delete_after=10)
        try:
            await ctx.message.delete(delay=5)
        except discord.Forbidden:
            pass
        return

    normalized_tweet_link = normalize_url(tweet_link)
    if not normalized_tweet_link or "twitter.com/".lower() not in normalized_tweet_link:
        await ctx.send(f"❌ Please provide a valid Twitter/X link for your quest submission.", delete_after=15)
        try:
            await ctx.message.delete(delay=5)
        except discord.Forbidden:
            pass
        return

    if normalized_tweet_link in approved_proofs:
        try:
            await ctx.message.delete(delay=0)
        except discord.Forbidden:
            print(f"Bot missing permissions to delete message in {ctx.channel.name} ({ctx.channel.id}).")
        await ctx.send(
            f"🚫 {ctx.author.mention}, your quest submission was removed! This proof (tweet) has already been submitted and approved for a quest or engagement. Please ensure your quest proofs are unique.",
            delete_after=20
        )
        return

    user_week_data = quest_submissions.setdefault(user_id, {}).setdefault(week, {})

    if str(quest_number) in user_week_data:
        if user_week_data[str(quest_number)]["status"] == "pending":
            await ctx.send(
                "⚠️ You already have a pending submission for this quest. Please wait for it to be reviewed.",
                delete_after=15)
        elif user_week_data[str(quest_number)]["status"] == "approved":
            await ctx.send("⚠️ You have already successfully completed and been approved for this quest.",
                           delete_after=15)
        try:
            await ctx.message.delete(delay=5)
        except discord.Forbidden:
            pass
        return

    user_week_data[str(quest_number)] = {
        "tweet": tweet_link,
        "normalized_tweet": normalized_tweet_link,
        "status": "pending",
        "timestamp": int(discord.utils.utcnow().timestamp())
    }
    save_quest_submissions()

    mod_review_channel = bot.get_channel(MOD_QUEST_REVIEW_CHANNEL_ID)
    if mod_review_channel:
        try:
            await mod_review_channel.send(
                f"🧩 **New Quest Submission**\n"
                f"User: {ctx.author.mention} ({ctx.author.name})\n"
                f"Week: {week}\n"
                f"Quest Number: {quest_number}\n"
                f"Link: {tweet_link}\n"
                f"To approve/reject: `!verifyquest {user_id} {quest_number} <approve|reject>`"
            )
        except discord.Forbidden:
            print(
                f"Bot missing permissions to send message to mod quest review channel ({MOD_QUEST_REVIEW_CHANNEL_ID}).")
        except discord.HTTPException as e:
            print(f"HTTP Error sending quest mod notification: {e}")
    else:
        print(f"Mod quest review channel (ID: {MOD_QUEST_REVIEW_CHANNEL_ID}) not found.")

    await ctx.send(
        f"✅ {ctx.author.mention}, your submission for **Quest {quest_number}** has been received for review!",
        delete_after=15)


# --- !verifyquest Command (Optimized and Secured) ---
@bot.command()
@commands.has_permissions(manage_messages=True)
async def verifyquest(ctx, member: discord.Member, quest_number: int, action: str):
    """
    (Moderator Only) Verifies a submitted quest.
    This command is restricted to a specific moderator channel to prevent misuse.
    Usage: !verify-quest <@member> <quest_number> <approve|reject>
    """
    if ctx.channel.id != MOD_QUEST_REVIEW_CHANNEL_ID:
        try:
            await ctx.message.delete(delay=5)
            await ctx.send(f"❌ This command can only be used in the <#{MOD_QUEST_REVIEW_CHANNEL_ID}> channel.",
                           delete_after=10)
        except discord.Forbidden:
            pass
        return

    user_id = str(member.id)
    week = str(weekly_quests["week"])
    action = action.lower()

    if user_id not in quest_submissions or week not in quest_submissions[user_id]:
        await ctx.send("❌ No quest submission found for this user for the current week.", delete_after=10)
        return

    quest_data = quest_submissions[user_id][week]

    if str(quest_number) not in quest_data:
        await ctx.send(f"⚠️ Quest {quest_number} not submitted by {member.mention} for this week.", delete_after=10)
        return

    submission_status = quest_data[str(quest_number)]["status"]
    if submission_status == "approved":
        await ctx.send(f"⚠️ Quest {quest_number} for {member.mention} is already approved.", delete_after=10)
        return

    reply_channel = bot.get_channel(QUEST_SUBMIT_CHANNEL_ID)
    if not reply_channel:
        print(f"Warning: Quest submission channel ({QUEST_SUBMIT_CHANNEL_ID}) not found. Replying in current channel.")
        reply_channel = ctx.channel

    if action == "approve":
        # NEW: Check if the admin has enough points to award
        points_to_award = 100.0
        if admin_points["balance"] < points_to_award:
            await ctx.send(f"❌ Error: Admin balance is too low to award {points_to_award:.2f} points.",
                           delete_after=10)
            print("⚠️ Admin balance is too low to award quest points. Skipping.")
            return

        if user_id not in user_points:
            user_points[user_id] = {"all_time_points": 0.0, "available_points": 0.0}

        user_points[user_id]["all_time_points"] += points_to_award
        user_points[user_id]["available_points"] += points_to_award
        quest_data[str(quest_number)]["status"] = "approved"

        # --- NEW LINE ---
        await log_points_transaction(user_id, points_to_award, f"Quest {quest_number} approval")

        # --- CORRECTED ORDER: Save user points before admin points ---
        save_json_file(POINTS_FILE, user_points)

        # NEW: Deduct points from the admin's balance and update claimed points
        admin_points["balance"] -= points_to_award
        admin_points["claimed_points"] += points_to_award
        save_json_file(ADMIN_POINTS_FILE, admin_points)

        if "normalized_tweet" in quest_data[str(quest_number)]:
            normalized_url = quest_data[str(quest_number)]["normalized_tweet"]
            if normalized_url not in approved_proofs:
                approved_proofs.append(normalized_url)
        save_approved_proofs()
        save_quest_submissions()

        try:
            await reply_channel.send(
                f"✅ {member.mention}, your **Quest {quest_number}** for Week {week} was **approved**! You earned **{points_to_award:.2f} points**. Your new available balance is **{user_points[user_id]['available_points']:.2f} points**."
            )
        except discord.Forbidden:
            print(
                f"Bot missing permissions to send approval message to quest submission channel ({QUEST_SUBMIT_CHANNEL_ID}).")

    elif action == "reject":
        quest_data[str(quest_number)]["status"] = "rejected"
        save_quest_submissions()
        try:
            await reply_channel.send(
                f"🚫 {member.mention}, your **Quest {quest_number}** for Week {week} was **rejected**. Please check the requirements and try again if necessary."
            )
        except discord.Forbidden:
            print(
                f"Bot missing permissions to send rejection message to quest submission channel ({QUEST_SUBMIT_CHANNEL_ID}).")
    else:
        await ctx.send("⚠️ Invalid action. Please use 'approve' or 'reject'.", delete_after=10)
        return

    await ctx.send(f"✅ Quest {quest_number} for {member.name} has been marked as '{action}'.", delete_after=10)


# === # ON MESSAGE EVENTS ===
@bot.event
async def on_message(message):
    """
    Handles all incoming messages and their respective features.
    """
    if message.author.bot:
        return

    # --- 1. GM/G1st Points Feature ---
    if message.channel.id == GM_G1ST_CHANNEL_ID:
        content = message.content.lower()

        if "gm" in content or "mv" in content:
            user_id = str(message.author.id)
            today = str(datetime.now().date())

            # Check if the user is eligible for points today
            if user_id not in gm_log or gm_log.get(user_id) != today:
                try:
                    # --- Check if the user is an Admin ---
                    is_admin = any(role.id == ADMIN_ROLE_ID for role in message.author.roles)

                    if is_admin:
                        # Award points to the Admin's personal balance
                        admin_points["balance"] -= GM_G1ST_POINTS_REWARD
                        admin_points["my_points"] += GM_G1ST_POINTS_REWARD
                        admin_points["claimed_points"] += GM_G1ST_POINTS_REWARD
                        save_json_file(ADMIN_POINTS_FILE, admin_points)

                        await log_points_transaction(user_id, GM_G1ST_POINTS_REWARD, "GM points")
                        await message.channel.send(
                            f"🪙 {message.author.mention}, you have been awarded **{GM_G1ST_POINTS_REWARD:.2f} points**!",
                            delete_after=5
                        )
                    else:
                        # Award points to a regular user from the main balance
                        if admin_points["balance"] < GM_G1ST_POINTS_REWARD:
                            print("⚠️ Admin balance is too low to award points. Skipping.")
                            return

                        user_data = user_points.setdefault(user_id, {"all_time_points": 0.0, "available_points": 0.0})
                        user_data["all_time_points"] += GM_G1ST_POINTS_REWARD
                        user_data["available_points"] += GM_G1ST_POINTS_REWARD
                        save_json_file(POINTS_FILE, user_points)

                        admin_points["balance"] -= GM_G1ST_POINTS_REWARD
                        admin_points["claimed_points"] += GM_G1ST_POINTS_REWARD
                        save_json_file(ADMIN_POINTS_FILE, admin_points)

                        await log_points_transaction(user_id, GM_G1ST_POINTS_REWARD, "GM points")
                        await message.channel.send(
                            f"🪙 {message.author.mention}, you've been awarded **{GM_G1ST_POINTS_REWARD:.2f} points**!",
                            delete_after=5
                        )

                    # Log that the user received points for today
                    gm_log[user_id] = today
                    save_json_file(GM_LOG_FILE, gm_log)

                except Exception as e:
                    print(f"An error occurred during GM points transaction: {e}")

    # --- 2. Process Commands ---
    # This is crucial for commands to work.
    if message.content.startswith(bot.command_prefix):
        await bot.process_commands(message)
        return

    # --- 3. Rest of your logic (XP, Banned Words, VIP, etc.) ---
    user_id = str(message.author.id)
    if user_id not in user_xp or not isinstance(user_xp[user_id], dict):
        user_xp[user_id] = {"xp": 0}

    xp_earned = random.randint(5, 15)
    user_xp[user_id]["xp"] += xp_earned
    save_xp()

    for word in banned_words:
        if word.lower() in message.content.lower():
            try:
                await message.delete(delay=0)
                await message.channel.send(f'🚫 {message.author.mention}, word not allowed!', delete_after=10)
            except discord.Forbidden:
                print(f"Bot missing permissions to delete or send message in {message.channel.name} (Banned Words).")
            return

# === VIP POST LIMIT ===
    if message.channel.id == ENGAGEMENT_CHANNEL_ID:
        member = message.author

        # Check if the user is a moderator or admin
        is_mod_or_admin = any(role.id in [ADMIN_ROLE_ID, MOD_ROLE_ID] for role in member.roles)

        if is_mod_or_admin:
            # Mods and admins can post freely, so we do nothing and let the message go through
            return

        # If the user is not a mod or admin, proceed with the VIP and post-limit checks
        if VIP_ROLE_ID not in [role.id for role in member.roles]:
            try:
                await message.delete(delay=0)
                await message.channel.send(
                    f"❌ {member.mention}, only **VIP members** can post in this channel!", delete_after=10)
            except discord.Forbidden:
                print(f"Bot missing permissions to delete or send message in {message.channel.name} (VIP restriction).")
            return
        else:
            # The user has the VIP role (and is not a mod or admin), so apply the post-limit
            user_id = str(member.id)
            vip_posts[user_id] = vip_posts.get(user_id, 0) + 1
            save_vip_posts()
            if vip_posts[user_id] > 3:
                try:
                    await message.delete(delay=0)
                    await message.channel.send(
                        f"🚫 {member.mention}, you've reached your daily post limit in this channel (3 per day).",
                        delete_after=10)
                except discord.Forbidden:
                    print(
                        f"Bot missing permissions to delete or send message in {message.channel.name} (VIP daily limit).")
                return

    if message.channel.id == PAYMENT_CHANNEL_ID:
        mod_channel = bot.get_channel(MOD_PAYMENT_REVIEW_CHANNEL_ID)
        if mod_channel:
            content = f"💰 **Payment Confirmation** from {message.author.mention}:\n{message.content}"
            files = []
            try:
                files = [await a.to_file() for a in message.attachments]
            except discord.HTTPException as e:
                print(f"Error fetching attachment files for payment confirmation: {e}")
            try:
                await mod_channel.send(content, files=files)
            except discord.Forbidden:
                print(
                    f"Bot missing permissions to send message to payment review channel ({MOD_PAYMENT_REVIEW_CHANNEL_ID}).")
        else:
            print(f"Payment review channel (ID: {MOD_PAYMENT_REVIEW_CHANNEL_ID}) not found for payment forwarding.")

        try:
            await message.delete(delay=0)
            await message.channel.send(
                f"✅ {message.author.mention}, your payment proof has been received and is under review. Please await moderator approval.",
                delete_after=15
            )
        except discord.Forbidden:
            print(
                f"Bot missing permissions to delete message or send confirmation in {message.channel.name} (Payment).")
        return


        # Handle ticket creation only if the message is in the support channel
    if message.channel.id == SUPPORT_CHANNEL_ID:
        # Check if the user already has an active ticket
        user_id = message.author.id
        if user_id in active_tickets.values():
            await message.channel.send("❌ You already have an active ticket. Please close it before opening a new one.",
                                       delete_after=10)
            await message.delete()  # Also delete their message so the channel stays clean
            return

        # Create a new private channel for the ticket
        guild = message.guild
        user = message.author
        ticket_name = f"ticket-{user.name.lower()}"

        # Set permissions for the new ticket channel
        # NOTE: You need to add permissions for the bot itself!
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.get_role(ADMIN_ROLE_ID): discord.PermissionOverwrite(view_channel=True),
            guild.get_role(MOD_ROLE_ID): discord.PermissionOverwrite(view_channel=True),
            user: discord.PermissionOverwrite(view_channel=True, send_messages=True),
            # This is the crucial line to add
            guild.get_member(bot.user.id): discord.PermissionOverwrite(view_channel=True, send_messages=True)
        }

        # Create the ticket channel
        try:
            ticket_channel = await guild.create_text_channel(
                ticket_name,
                category=guild.get_channel(TICKETS_CATEGORY_ID),
                overwrites=overwrites
            )

            # Send the original message to the new ticket channel
            await ticket_channel.send(f"**Ticket created by {user.mention}:**\n\n> {message.content}")
            await ticket_channel.send(
                f"Support team, you have a new ticket! Use `!close` to close this ticket after assisting the user.")

            # Delete the original message from the support channel
            await message.delete()

            # Add the new ticket to our active tickets dictionary
            active_tickets[ticket_channel.id] = user.id
            save_json_file(ACTIVE_TICKETS_FILE, active_tickets)

        except discord.Forbidden:
            print("Bot is missing permissions to create channels or manage roles.")
            await message.channel.send("❌ An error occurred. I don't have the permissions to create a ticket.",
                                       delete_after=10)
        except Exception as e:
            print(f"❌ An unhandled error occurred in ticket creation: {e}")
            await message.channel.send("❌ An unexpected error occurred while creating the ticket.",
                                       delete_after=10)

        # We return here so the command is not processed twice
        return


#-----------------------------------S U P P O R T --- S Y S T E M------------------------

@bot.command()
@commands.has_any_role(ADMIN_ROLE_ID, MOD_ROLE_ID)
async def close(ctx):
    """(Admin/Mod Only) Archives a ticket and schedules it for deletion."""

    ticket_channel_id = ctx.channel.id

    if ticket_channel_id not in active_tickets:
        await ctx.send("❌ This command can only be used inside a ticket channel.")
        return

    try:
        # Check if the archived category exists
        archived_category = bot.get_channel(ARCHIVED_TICKETS_CATEGORY_ID)
        if not archived_category:
            await ctx.send("❌ The archived tickets category was not found.")
            return

        # Change the channel name and category to archive it
        await ctx.channel.edit(
            name=f"closed-{ctx.channel.name}",
            category=archived_category
        )

        # Remove permissions for the user who created the ticket
        user_id = active_tickets.get(ticket_channel_id)
        user = ctx.guild.get_member(user_id)
        if user:
            # Note: The overwrite=None removes their permissions from the channel
            await ctx.channel.set_permissions(user, overwrite=None)

        await ctx.send("This ticket has been closed and will be deleted in 30 days.")

        del active_tickets[ticket_channel_id]
        save_json_file(ACTIVE_TICKETS_FILE, active_tickets)

    except discord.Forbidden:
        print("Bot is missing permissions to modify the ticket channel.")
        await ctx.send("❌ I don't have permission to modify this channel.")
    except Exception as e:
        print(f"❌ An error occurred while closing the ticket: {e}")
        await ctx.send("❌ An unexpected error occurred while closing the ticket.")



#--------------------M Y S T E R Y     B O X     S Y S T E M-------------------------------------

def _mb_get_uses_in_last_24h(uid: str) -> int:
    ts_list = mysterybox_uses.get(uid, [])
    cutoff = time.time() - 24 * 3600
    ts_list = [t for t in ts_list if t >= cutoff]
    mysterybox_uses[uid] = ts_list
    save_json_file(MYSTERYBOX_USES_FILE, mysterybox_uses)
    return len(ts_list)

def _mb_add_use(uid: str):
    ts_list = mysterybox_uses.get(uid, [])
    ts_list.append(time.time())
    mysterybox_uses[uid] = ts_list
    save_json_file(MYSTERYBOX_USES_FILE, mysterybox_uses)

@bot.command(name="mysterybox")
async def cmd_mysterybox(ctx: commands.Context):
    # Channel restriction
    if ctx.channel.id != MYSTERYBOX_CHANNEL_ID:
        await ctx.send(f"❌ Use this command in <#{MYSTERYBOX_CHANNEL_ID}> only.", delete_after=8)
        return

    user_id = str(ctx.author.id)

    # Daily usage check
    used = _mb_get_uses_in_last_24h(user_id)
    if used >= MYSTERYBOX_MAX_PER_24H:
        # find time until next reset
        oldest = min(mysterybox_uses[user_id]) if mysterybox_uses.get(user_id) else time.time()
        secs = int(24 * 3600 - (time.time() - oldest))
        if secs < 0: secs = 0
        hrs = secs // 3600
        mins = (secs % 3600) // 60
        await ctx.send(f"⏳ You’ve reached your daily limit (2/24h). Try again in **{hrs}h {mins}m**.", delete_after=8)
        return

    # User balance check
    if get_user_balance(user_id) < MYSTERYBOX_COST:
        await ctx.send(f"❌ You need **{MYSTERYBOX_COST} pts** to open a Mystery Box.", delete_after=8)
        return

    # Deduct cost from user's available points
    ensure_user(user_id)
    user_points[user_id]["available_points"] -= MYSTERYBOX_COST
    save_json_file(POINTS_FILE, user_points)

    # Draw reward
    reward = random.choices(MYSTERYBOX_REWARDS, weights=MYSTERYBOX_WEIGHTS, k=1)[0]

    # Credit reward to user (this is *new* issuance only for the delta if reward > cost)
    # We always credit the reward amount to the user balance:
    user_points[user_id]["available_points"] += reward
    user_points[user_id]["all_time_points"] += reward  # reflect gross credited to the user
    save_json_file(POINTS_FILE, user_points)

    # Economy accounting:
    # - If reward > cost: the extra (reward - cost) is newly issued by admin → reduce admin balance & increase claimed_points
    # - If reward == cost: net neutral to economy
    # - If reward < cost: the shortfall (cost - reward) is burned (burned_points += diff)
    if reward > MYSTERYBOX_COST:
        delta = reward - MYSTERYBOX_COST
        if admin_can_issue(delta):
            admin_points["balance"] -= delta
            admin_points["claimed_points"] += delta
        else:
            # If somehow admin cannot cover, fallback: undo the extra above cost
            user_points[user_id]["available_points"] -= delta
            user_points[user_id]["all_time_points"] -= delta
            save_json_file(POINTS_FILE, user_points)
            reward = MYSTERYBOX_COST  # clamp to avoid inflation
    elif reward < MYSTERYBOX_COST:
        burn = MYSTERYBOX_COST - reward
        admin_points["burned_points"] += burn
        admin_points["claimed_points"] -= burn
        await log_points_transaction(bot.user.id, float(burn), f"Mystery Box: {ctx.author.name} (burn)")

    save_json_file(ADMIN_POINTS_FILE, admin_points)

    # Log transactions (to your central logger)
    # Log net results as: +reward (credit) and -cost (spend)
    await log_points_transaction(user_id, -float(MYSTERYBOX_COST), "Mystery Box: cost")
    await log_points_transaction(user_id, float(reward), "Mystery Box: reward")

    # Use count++
    _mb_add_use(user_id)

    # Command log echo
    log_ch = bot.get_channel(COMMAND_LOG_CHANNEL)
    if log_ch:
        await log_ch.send(f"🎁 Mystery Box used by <@{user_id}> — reward: **{reward}** pts")

    # Result embed (no burn disclosure)
    color = discord.Color.green() if reward >= MYSTERYBOX_COST else discord.Color.orange()
    embed = discord.Embed(
        title="🎁 Mystery Box Opened!",
        description=f"{ctx.author.mention} you spent **{MYSTERYBOX_COST} pts** and received:",
        color=color
    )
    embed.add_field(name="Reward", value=f"💎 **{reward} points**", inline=False)
    embed.set_footer(text="Good luck next time!" if reward < MYSTERYBOX_COST else "Nice hit!")
    embed.timestamp = datetime.now(UTC)
    await ctx.send(embed=embed)


if __name__ == "__main__":
    bot.run(token, log_handler=handler, log_level=logging.DEBUG)