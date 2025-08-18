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
from datetime import datetime, UTC
import string
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
xp_leaderboard_message_id = None


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
ANNOUNCEMENT_CHANNEL_ID = 1399073900024959048
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
PERIODIC_LEADERBOARD_CHANNEL_ID = 1406757660782624789
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
MAX_WINNERS_HISTORY = 50


# --- Static Configurations ---
POINT_VALUES = {"like": 20, "retweet": 30, "comment": 15}
ROLE_MULTIPLIERS = {
    ROOKIE_ROLE_ID: 1.0,
    ELITE_ROLE_ID: 1.5,
    SUPREME_ROLE_ID: 2.0,
    VIP_ROLE_ID: 0.0
}
QUEST_POINTS = 100.0
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

    if not update_leaderboards.is_running():
        update_leaderboards.start()

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
    update_leaderboards.start()


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
        log_message = f"💵 {user_name} | {purpose} | **{sign}{points:.2f} MVpts**"

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
# ======== T H E      E C O N O M Y      M E S S A G E        M E C H A N I S M ===
def get_economy_embed(admin_data):
    """
    Builds a premium embed for the economy status message.
    """
    try:
        # Define a single color for the embed
        embed_color = discord.Color.from_rgb(255, 204, 0)  # Gold color

        # Retrieve all point values with a default of 0.0
        balance = admin_data.get("balance", 0.0)
        in_circulation = admin_data.get("claimed_points", 0.0)
        burned_points = admin_data.get("burned_points", 0.0)
        treasury = admin_data.get("fees_earned", 0.0)
        my_points = admin_data.get("my_points", 0.0)
        total_supply = admin_data.get("total_supply", 0.0)

        # NOTE: Make sure the POINTS_TO_USD constant is available in your script
        # Calculate USD values
        usd_total_supply = total_supply * POINTS_TO_USD
        usd_balance = balance * POINTS_TO_USD
        usd_in_circulation = in_circulation * POINTS_TO_USD
        usd_burned_points = burned_points * POINTS_TO_USD
        usd_treasury = treasury * POINTS_TO_USD
        usd_my_points = my_points * POINTS_TO_USD

        # Create the embed object
        embed = discord.Embed(
            title="🪙 ManaVerse Economy Status",
            description="A real-time overview of the points economy.",
            color=embed_color
        )

        # Add fields for each data point
        embed.add_field(name="Total Supply", value=f"**{total_supply:,.2f}** points\n(${usd_total_supply:,.2f})",
                        inline=False)
        embed.add_field(name="Remaining Supply", value=f"**{balance:,.2f}** points\n(${usd_balance:,.2f})", inline=True)
        embed.add_field(name="In Circulation", value=f"**{in_circulation:,.2f}** points\n(${usd_in_circulation:,.2f})",
                        inline=True)
        embed.add_field(name="Burned", value=f"**{burned_points:,.2f}** points\n(${usd_burned_points:,.2f})",
                        inline=True)
        embed.add_field(name="Treasury", value=f"**{treasury:,.2f}** points\n(${usd_treasury:,.2f})", inline=True)
        embed.add_field(name="Admin's Earned Points", value=f"**{my_points:,.2f}** points\n(${usd_my_points:,.2f})",
                        inline=True)

        # Add a footer with a timestamp
        embed.set_footer(text="Data is updated in real-time.")
        embed.timestamp = datetime.now(UTC)

        return embed

    except NameError:
        error_embed = discord.Embed(
            title="❌ Configuration Error",
            description="The `POINTS_TO_USD` constant is not defined. Please add it to your script.",
            color=discord.Color.red()
        )
        return error_embed
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ An Error Occurred",
            description=f"An error occurred while building the economy embed: ```{e}```",
            color=discord.Color.red()
        )
        return error_embed


@tasks.loop(minutes=5)
async def update_economy_message():
    """Periodically updates the economy status message in a dedicated channel."""
    global economy_message_id

    try:
        channel = bot.get_channel(FIRST_ODOGWU_CHANNEL_ID)
        if not channel:
            print(f"❌ Error: Economy updates channel (ID: {FIRST_ODOGWU_CHANNEL_ID}) not found.")
            return

        # Use the new function to get the embed object
        economy_embed = get_economy_embed(admin_points)

        if economy_message_id:
            try:
                message = await channel.fetch_message(economy_message_id)
                await message.edit(embed=economy_embed)
            except discord.NotFound:
                # If the message was deleted, send a new one and save its ID
                message = await channel.send(embed=economy_embed)
                economy_message_id = message.id
            except discord.Forbidden:
                print(f"❌ Bot missing permissions to edit message in channel ({FIRST_ODOGWU_CHANNEL_ID}).")
        else:
            # If there is no existing message ID, send a new message and save its ID
            message = await channel.send(embed=economy_embed)
            economy_message_id = message.id

    except discord.Forbidden:
        print(f"❌ Bot missing permissions to send messages in channel ({FIRST_ODOGWU_CHANNEL_ID}).")
    except Exception as e:
        print(f"❌ An unexpected error occurred in the economy update task: {e}")


#-------------------------A D M I N       U N I T-----------------------------------
@bot.command()
@commands.has_role(ADMIN_ROLE_ID)
async def admin(ctx):
    """
    (Admin Only) Displays the bot's point economy status as a premium embed.
    """
    try:
        # Get the economy to embed from the helper function
        economy_embed = get_economy_embed(admin_points)

        # Send the embed to the channel
        await ctx.send(embed=economy_embed)

    except discord.Forbidden:
        # If the bot doesn't have permissions, send a simplified message
        await ctx.send("❌ I don't have permission to send embeds or delete messages in this channel.")

    except Exception as e:
        # Catch any other unexpected errors
        print(f"An error occurred in the !admin command: {e}")
        await ctx.send("❌ An unexpected error occurred. Please check the bot's console for details.")

    # Always attempt to delete the command message for a clean look
    try:
        await ctx.message.delete()
    except discord.Forbidden:
        pass  # The bot can't delete the message, so we just move on


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

        data_text += f"**#{idx}**: {username} - **{points:.2f} MVpts** | Referrals: {referrals}\n"

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


async def append_new_winner_to_history():
    """
    Moves winners from the temporary giveaway log to the permanent history log.
    """
    # Load winners from the temporary log using your helper function
    temporary_winners = load_json_file_with_list_defaults("giveaway_log.json", [])

    # If the temporary file is empty, there's nothing to do.
    if not temporary_winners:
        return

    # Load the permanent all-time winners log
    all_time_winners = load_json_file_with_list_defaults("giveaway_all_time.json", [])

    # Append the new winners to the all-time log
    all_time_winners.extend(temporary_winners)

    # Implement the list size limit
    if len(all_time_winners) > MAX_WINNERS_HISTORY:
        entries_to_remove = len(all_time_winners) - MAX_WINNERS_HISTORY
        del all_time_winners[:entries_to_remove]

    # Save the updated all-time winners log using your save function
    save_json_file("giveaway_all_time.json", all_time_winners)

    # Clear the temporary giveaway log for the next giveaway
    save_json_file("giveaway_log.json", [])

    print("New winners appended to the all-time log and temporary log cleared.")

    await update_giveaway_winners_history()


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

    user_id = str(member.id)
    if user_id in referred_users:
        print(f"User {member.name} has rejoined but has already been referred. Skipping referral check.")
        return

    guild = member.guild

    # Get the invites AFTER the user joined
    invites_after_join = await guild.invites()

    # Get the invites BEFORE the user joined from the cache
    invites_before_join = invite_cache.get(guild.id, [])

    referrer = None

    # Use a more efficient dictionary-based lookup for comparison
    invites_before_dict = {invite.code: invite.uses for invite in invites_before_join}

    for invite_after in invites_after_join:
        uses_before = invites_before_dict.get(invite_after.code, 0)
        if invite_after.uses > uses_before:
            referrer = invite_after.inviter
            break

    # Important: Update the cache for the next time someone joins
    invite_cache[guild.id] = invites_after_join

    if referrer and referrer.id != bot.user.id:
        pending_referrals[str(member.id)] = str(referrer.id)
        save_json_file(PENDING_REFERRALS_FILE, pending_referrals)
        print(f"New pending referral for {member.name}. Referrer: {referrer.name}")

        # Send a confirmation to the referrer
        try:
            embed = discord.Embed(
                title="✨ New Referral!",
                description=f"🎉 You have successfully referred **{member.name}**!",
                color=discord.Color.gold()
            )
            embed.set_footer(text="Awaiting verification. You'll receive your points soon!")
            embed.timestamp = datetime.now(UTC)
            await referrer.send(embed=embed)
        except discord.Forbidden:
            print(f"Could not send referral notification to {referrer.name}. User has DMs disabled.")


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



#----------------- P E R I O D I C         L E A D E R B O A R D      L O G I C----------------------------------

async def get_referral_leaderboard_embed(bot, referral_data):
    """
    Generates a premium referral leaderboard embed from the referral data.
    """
    embed = discord.Embed(
        title="🏆 Top 10 Referral Leaderboard",
        description="These are the top community members who are growing the server! 🚀",
        color=discord.Color.gold()
    )

    # Count referrals for each user
    referral_counts = {}
    for user_id, referrer_id in referral_data.items():
        if int(referrer_id) != bot.user.id:
            referral_counts[referrer_id] = referral_counts.get(referrer_id, 0) + 1

    # Sort the users by their referral count
    sorted_referrals = sorted(
        referral_counts.items(),
        key=lambda item: item[1],
        reverse=True
    )

    if not sorted_referrals:
        embed.description = "The referral leaderboard is currently empty."
        return embed

    # Add a field for each top user with the medal logic
    medals = ["🥇", "🥈", "🥉"]
    leaderboard_text = ""
    for rank, (user_id, count) in enumerate(sorted_referrals[:10], 1):
        if count == 0:
            continue

        try:
            user = await bot.fetch_user(int(user_id))
            user_name = user.display_name
        except discord.NotFound:
            user_name = f"User ID: {user_id}"

        # Determine the medal emoji for the rank
        medal = medals[rank - 1] if rank <= 3 else "🏅"

        leaderboard_text += f"**{medal}** **#{rank}.** {user_name} with **{count}** referrals\n"

    embed.add_field(name="🌟 Top Referrers", value=leaderboard_text, inline=False)
    embed.set_footer(text="Updated periodically. Keep referring friends! 💖")
    embed.timestamp = datetime.now(UTC)
    return embed

#     P O I N T S     L E A D E R B O A R D

async def get_points_leaderboard_embed(bot, user_points):
    """
    Generates a formatted points leaderboard embed with medal logic.
    """
    # Filter out ineligible users (admins, mods)
    eligible_users = {}
    for user_id, data in user_points.items():
        try:
            member = bot.get_guild(SERVER_ID).get_member(int(user_id))
            if member:
                if not any(role.id in [ADMIN_ROLE_ID, MOD_ROLE_ID] for role in member.roles):
                    if data.get('all_time_points', 0) > 0:
                        eligible_users[user_id] = data
        except (ValueError, AttributeError):
            continue

    # Sort the users by their all-time points in descending order
    sorted_points = sorted(
        eligible_users.items(),
        key=lambda item: item[1].get('all_time_points', 0),
        reverse=True
    )

    # Embed setup
    embed = discord.Embed(
        title="💰 Points Leaderboard",
        description="Here are the top members with the most points! 💎",
        color=discord.Color.gold()
    )

    if not eligible_users:
        embed.description = "The points leaderboard is currently empty. Start earning points!"
        return embed

    # Add the leaderboard content to a single embed field
    medals = ["🥇", "🥈", "🥉"]
    leaderboard_text = ""
    for rank, (user_id, points_data) in enumerate(sorted_points[:10], 1):
        points = points_data.get('all_time_points', 0)

        try:
            user = await bot.fetch_user(int(user_id))
            user_name = user.display_name
        except discord.NotFound:
            user_name = f"User ID: {user_id}"

        # Get the correct medal emoji
        medal = medals[rank - 1] if rank <= 3 else "🏅"

        leaderboard_text += f"**{medal}** **#{rank}.** {user_name} with **{points:.2f} points**\n"

    embed.add_field(name="🌟 Top Point Earners", value=leaderboard_text, inline=False)
    embed.set_footer(text="Updated periodically. Keep earning points! 🚀")
    embed.timestamp = datetime.now(UTC)

    return embed

# X P    L E A D E R B O A R D

async def get_xp_leaderboard_embed(bot, user_xp):
    """
    Generates a premium XP leaderboard embed.
    """
    eligible_users = {}
    for user_id, data in user_xp.items():
        try:
            member = bot.get_guild(SERVER_ID).get_member(int(user_id))
            if member:
                if not any(role.id in [ADMIN_ROLE_ID, MOD_ROLE_ID] for role in member.roles):
                    if data.get('xp', 0) > 0:
                        eligible_users[user_id] = data
        except (ValueError, AttributeError):
            continue

    sorted_xp = sorted(
        eligible_users.items(),
        key=lambda item: item[1].get('xp', 0),
        reverse=True
    )

    embed = discord.Embed(
        title="🔥 XP Leaderboard",
        description="These members have the most Mana XP! 🌟",
        color=discord.Color.blue()
    )

    if not eligible_users:
        embed.description = "The XP leaderboard is currently empty."
        return embed

    medals = ["🥇", "🥈", "🥉"]
    leaderboard_text = ""
    for rank, (user_id, xp_data) in enumerate(sorted_xp[:10], 1):
        xp = xp_data.get('xp', 0)

        try:
            user = await bot.fetch_user(int(user_id))
            user_name = user.display_name
        except discord.NotFound:
            user_name = f"User ID: {user_id}"

        medal = medals[rank - 1] if rank <= 3 else "🏅"

        leaderboard_text += f"**{medal}** **#{rank}.** {user_name} with **{xp} XP**\n"

    embed.add_field(name="🌟 Top XP Earners", value=leaderboard_text, inline=False)
    embed.set_footer(text="Updated periodically.")
    embed.timestamp = datetime.now(UTC)

    return embed

#        T H E      L O O P
@tasks.loop(minutes=30)  # You can adjust the update interval as needed
async def update_leaderboards():
    global referral_leaderboard_message_id, points_leaderboard_message_id, xp_leaderboard_message_id

    try:
        # 1. Get the dedicated leaderboard channel
        channel = bot.get_channel(PERIODIC_LEADERBOARD_CHANNEL_ID)
        if not channel:
            print(f"❌ Error: Leaderboard channel not found (ID: {PERIODIC_LEADERBOARD_CHANNEL_ID}).")
            return

        # 2. Generate the embeds for all three leaderboards
        points_leaderboard_embed = await get_points_leaderboard_embed(bot, user_points)
        referral_leaderboard_embed = await get_referral_leaderboard_embed(bot, referral_data)
        xp_leaderboard_embed = await get_xp_leaderboard_embed(bot, user_xp)

        # 3. Update the Points Leaderboard Message
        if points_leaderboard_message_id:
            try:
                points_message = await channel.fetch_message(points_leaderboard_message_id)
                await points_message.edit(embed=points_leaderboard_embed)
                print("Points leaderboard updated successfully.")
            except discord.NotFound:
                print("Points message not found. Creating a new one.")
                new_points_message = await channel.send(embed=points_leaderboard_embed)
                points_leaderboard_message_id = new_points_message.id
                await new_points_message.pin()
        else:
            new_points_message = await channel.send(embed=points_leaderboard_embed)
            points_leaderboard_message_id = new_points_message.id
            await new_points_message.pin()

        # 4. Update the Referral Leaderboard Message
        if referral_leaderboard_message_id:
            try:
                referral_message = await channel.fetch_message(referral_leaderboard_message_id)
                await referral_message.edit(embed=referral_leaderboard_embed)
                print("Referral leaderboard updated successfully.")
            except discord.NotFound:
                print("Referral message not found. Creating a new one.")
                new_referral_message = await channel.send(embed=referral_leaderboard_embed)
                referral_leaderboard_message_id = new_referral_message.id
                await new_referral_message.pin()
        else:
            new_referral_message = await channel.send(embed=referral_leaderboard_embed)
            referral_leaderboard_message_id = new_referral_message.id
            await new_referral_message.pin()

        # 5. Update the XP Leaderboard Message
        if xp_leaderboard_message_id:
            try:
                xp_message = await channel.fetch_message(xp_leaderboard_message_id)
                await xp_message.edit(embed=xp_leaderboard_embed)
                print("XP leaderboard updated successfully.")
            except discord.NotFound:
                print("XP message not found. Creating a new one.")
                new_xp_message = await channel.send(embed=xp_leaderboard_embed)
                xp_leaderboard_message_id = new_xp_message.id
                await new_xp_message.pin()
        else:
            new_xp_message = await channel.send(embed=xp_leaderboard_embed)
            xp_leaderboard_message_id = new_xp_message.id
            await new_xp_message.pin()

    except discord.Forbidden:
        print("Bot is missing permissions to send, edit, or pin messages in the leaderboard channel.")
    except Exception as e:
        print(f"❌ An unexpected error occurred in the leaderboard update task: {e}")


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
        await ctx.message.delete(delay=5)
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

    # Create the embed
    embed = discord.Embed(
        title="👥 Your Referrals",
        color=discord.Color.blue()
    )
    embed.set_thumbnail(url=ctx.author.avatar.url if ctx.author.avatar else None)
    embed.set_footer(
        text=f"Total Referrals: {len(referred_members)}",
        icon_url=ctx.guild.icon.url if ctx.guild.icon else None
    )
    embed.timestamp = datetime.now(UTC)

    if not referred_members:
        embed.description = (
            "You have not referred anyone yet. Share your invite link to get started!"
            "\n\n🔗 **Your Invite Link:** [Click Here](<Your Invite Link Here>)"
        )
    else:
        embed.description = "Here is a list of members you have successfully referred:"

        referral_list = ""
        for referred_id in referred_members:
            try:
                user = await bot.fetch_user(int(referred_id))
                referral_list += f"• {user.mention} ({user.display_name})\n"
            except discord.NotFound:
                referral_list += f"• Unknown User (ID: {referred_id})\n"

        embed.add_field(name="Referred Users", value=referral_list, inline=False)

    await ctx.send(embed=embed, delete_after=60)


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

        # --- Refactored Confirmation Message with an Embed ---
        embed = discord.Embed(
            title="✨ Points Awarded! ✨",
            description=f"{reaction.message.author.mention} received **{points_to_add:.2f} points**!",
            color=discord.Color.green()
        )
        embed.add_field(
            name="Awarded By",
            value=user.mention,
            inline=True
        )
        embed.add_field(
            name="Reason",
            value="For a great message!",
            inline=True
        )
        embed.set_thumbnail(url=reaction.message.author.avatar.url if reaction.message.author.avatar else None)
        embed.set_footer(
            text="ManaVerse Points System - Keep the positive vibes flowing! 😊"
        )
        embed.timestamp = datetime.now(UTC)

        await reaction.message.channel.send(embed=embed, delete_after=20)

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


#----------------------P R O O F       O F       T A S K---------------------------------------------
@bot.command()
async def proof(ctx, tweet_url: str, *engagements):
    """
    Allows users to submit proof of engagements for points, applying role multipliers.
    Checks for duplicate tweet URLs or attached images.
    Usage: !proof <tweet_url> [like] [comment] [retweet]
    """
    # Channel Restriction Check
    if ctx.channel.id != TASK_SUBMIT_CHANNEL_ID:
        error_embed = discord.Embed(
            title="❌ Incorrect Channel",
            description=f"This command can only be used in the <#{TASK_SUBMIT_CHANNEL_ID}> channel.",
            color=discord.Color.red()
        )
        await ctx.send(embed=error_embed, delete_after=15)
        try:
            await ctx.message.delete(delay=5)
        except discord.Forbidden:
            pass
        return

    user_id = str(ctx.author.id)

    # --- 1. Extract and Normalize all potential proof URLs ---
    all_proof_urls = []
    if tweet_url:
        all_proof_urls.append(normalize_url(tweet_url))
    for attachment in ctx.message.attachments:
        if attachment.content_type and attachment.content_type.startswith('image/'):
            all_proof_urls.append(normalize_url(attachment.url))

    if not all_proof_urls:
        embed = discord.Embed(
            title="🚫 Submission Failed",
            description=f"{ctx.author.mention}, please provide a tweet URL and/or attach an image to your command.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=15)
        try:
            await ctx.message.delete(delay=5)
        except discord.Forbidden:
            pass
        return

    # --- 2. Check for Duplicates ---
    for url in all_proof_urls:
        if url in approved_proofs:
            embed = discord.Embed(
                title="🚫 Duplicate Submission",
                description=f"{ctx.author.mention}, your submission was removed! One or more of the proofs (tweet or image) has already been submitted and approved. Please ensure all proofs are unique.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed, delete_after=25)
            try:
                await ctx.message.delete(delay=0)
            except discord.Forbidden:
                pass
            return

    # --- 3. Process Valid Engagements ---
    valid_engagements = [e.lower() for e in engagements if e.lower() in POINT_VALUES]
    if not valid_engagements:
        embed = discord.Embed(
            title="🚫 Submission Failed",
            description=f"{ctx.author.mention}, please specify valid engagement types: `like`, `comment`, `retweet`.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=15)
        try:
            await ctx.message.delete(delay=5)
        except discord.Forbidden:
            pass
        return

    # Calculate base points
    base_points = sum(POINT_VALUES[e] for e in valid_engagements)

    # Check for pending submission
    if user_id in submissions:
        embed = discord.Embed(
            title="⏳ Pending Submission",
            description=f"{ctx.author.mention}, you already have a pending submission. Please wait for it to be reviewed or contact a moderator.",
            color=discord.Color.orange()
        )
        await ctx.send(embed=embed, delete_after=15)
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
    final_points = round(base_points * multiplier, 2)

    # Store submission details
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
    # save_submissions() # Make sure you call this function

    # --- 5. Notify Moderators with an Embed ---
    mod_channel = bot.get_channel(MOD_TASK_REVIEW_CHANNEL_ID)
    if mod_channel:
        try:
            mod_notification_embed = discord.Embed(
                title="🔍 New Submission for Review",
                description=f"User: {ctx.author.mention}\nAccount: {ctx.author.name}",
                color=discord.Color.blue(),
                url=tweet_url
            )
            mod_notification_embed.add_field(name="Tweet URL", value=tweet_url, inline=False)
            if ctx.message.attachments:
                attachments_list = "\n".join(f"[{att.filename}]({att.url})" for att in ctx.message.attachments)
                mod_notification_embed.add_field(name="Attached Images", value=attachments_list, inline=False)

            mod_notification_embed.add_field(name="Engagements", value=", ".join(valid_engagements), inline=True)
            mod_notification_embed.add_field(name="Points Requested", value=f"**{final_points}** (x{multiplier})",
                                             inline=True)
            mod_notification_embed.set_footer(
                text=f"ID: {user_id} • To approve/reject: !verify {user_id} <approve|reject>")

            await mod_channel.send(embed=mod_notification_embed)

        except discord.Forbidden:
            print(f"Bot missing permissions to send message to mod review channel ({MOD_TASK_REVIEW_CHANNEL_ID}).")
        except Exception as e:
            print(f"Error sending mod notification embed: {e}")
    else:
        print(f"Mod review channel (ID: {MOD_TASK_REVIEW_CHANNEL_ID}) not found.")

    # --- 6. Send a success message to the user with an Embed ---
    success_embed = discord.Embed(
        title="✅ Submission Logged!",
        description=f"{ctx.author.mention}, your submission has been sent for review.",
        color=discord.Color.green()
    )
    success_embed.add_field(name="Points Requested", value=f"**{final_points}**", inline=True)
    success_embed.add_field(name="Your Engagements", value=f"**{', '.join(valid_engagements)}**", inline=True)
    success_embed.set_footer(text="Please be patient while a moderator reviews your proof.")
    success_embed.set_thumbnail(url=ctx.author.avatar.url if ctx.author.avatar else None)

    await ctx.send(embed=success_embed, delete_after=20)


# --- !verify Command (Optimized and Secured) ---
@bot.command()
@commands.has_permissions(manage_messages=True)
async def verify(ctx, member: discord.Member, action: str):
    """
    (Moderator Only) Approves or rejects a user's task submission.
    This command is restricted to a specific moderator channel to prevent misuse.
    Usage: !verify <@member> <approve|reject>
    """
    # 1. Channel Restriction Check
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

    # 2. Check for Pending Submission
    if user_id not in submissions:
        no_submission_embed = discord.Embed(
            title="❌ Error",
            description="No pending submission found for this user.",
            color=discord.Color.red()
        )
        await ctx.send(embed=no_submission_embed, delete_after=10)
        return

    submission = submissions[user_id]
    points_to_award = submission["points_requested"]
    reply_channel = bot.get_channel(submission.get("channel_id", TASK_SUBMIT_CHANNEL_ID))

    if not reply_channel:
        print(f"Warning: Could not find reply channel for user {user_id}'s submission. Falling back to ctx.channel.")
        reply_channel = ctx.channel

    # 3. Process Actions
    if action == "approve":
        if admin_points["balance"] < points_to_award:
            balance_embed = discord.Embed(
                title="❌ Approval Failed",
                description=f"Admin balance is too low to award **{points_to_award:.2f}** points.",
                color=discord.Color.red()
            )
            await ctx.send(embed=balance_embed, delete_after=10)
            return

        if user_id not in user_points:
            user_points[user_id] = {"all_time_points": 0.0, "available_points": 0.0}

        user_points[user_id]["all_time_points"] += points_to_award
        user_points[user_id]["available_points"] += points_to_award
        await log_points_transaction(user_id, points_to_award, f"Task submission approved")
        save_json_file(POINTS_FILE, user_points)

        admin_points["balance"] -= points_to_award
        admin_points["claimed_points"] += points_to_award
        save_json_file(ADMIN_POINTS_FILE, admin_points)

        for url in submission.get("normalized_proof_urls", []):
            if url not in approved_proofs:
                approved_proofs.append(url)
        save_approved_proofs()

        del submissions[user_id]
        save_submissions()

        # Confirmation message to the user
        try:
            user_embed = discord.Embed(
                title="✅ Submission Approved!",
                description=f"Your engagement proof has been approved. You earned **{points_to_award:.2f} points**!",
                color=discord.Color.green()
            )
            user_embed.add_field(name="Your New Total", value=f"**{user_points[user_id]['available_points']:.2f} points**", inline=False)
            user_embed.set_footer(text="Thank you for your contribution!")
            await reply_channel.send(f"{member.mention}", embed=user_embed)
        except discord.Forbidden:
            print(f"Bot missing permissions to send approval message to {reply_channel.name}.")

        # Confirmation message to the moderator
        mod_embed = discord.Embed(
            title="✅ Action Logged",
            description=f"**Approved** submission for {member.mention}.",
            color=discord.Color.green()
        )
        mod_embed.add_field(name="Points Awarded", value=f"**{points_to_award:.2f}**", inline=True)
        mod_embed.set_footer(text=f"Action by {ctx.author.name}")
        await ctx.send(embed=mod_embed, delete_after=15)

    elif action == "reject":
        del submissions[user_id]
        save_submissions()

        # Rejection message to the user
        try:
            user_embed = discord.Embed(
                title="🚫 Submission Rejected",
                description="Your engagement proof has been rejected. Please review your proof and submit again if needed.",
                color=discord.Color.red()
            )
            await reply_channel.send(f"{member.mention}", embed=user_embed)
        except discord.Forbidden:
            print(f"Bot missing permissions to send rejection message to {reply_channel.name}.")

        # Confirmation message to the moderator
        mod_embed = discord.Embed(
            title="✅ Action Logged",
            description=f"**Rejected** submission for {member.mention}.",
            color=discord.Color.red()
        )
        mod_embed.set_footer(text=f"Action by {ctx.author.name}")
        await ctx.send(embed=mod_embed, delete_after=15)

    else:
        invalid_embed = discord.Embed(
            title="❌ Invalid Action",
            description="Please use `approve` or `reject`.",
            color=discord.Color.red()
        )
        await ctx.send(embed=invalid_embed, delete_after=10)


# === ! A P P R O V E      P A Y M E N T ===
@bot.command()
@commands.has_permissions(manage_roles=True)
async def approve_payment(ctx, member: discord.Member, amount: int):
    """
    Moderator command to approve a payment and assign a role based on amount.
    Usage: !approve_payment <@member> <amount>
    """
    if ctx.channel.id != MOD_TASK_REVIEW_CHANNEL_ID:
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
        invalid_amount_embed = discord.Embed(
            title="❌ Invalid Amount",
            description="The amount must be one of the following: **10, 15, 20, or 50**.",
            color=discord.Color.red()
        )
        await ctx.send(embed=invalid_amount_embed, delete_after=10)
        return

    role_id, role_name = role_map[amount]
    role = ctx.guild.get_role(role_id)

    if not role:
        role_not_found_embed = discord.Embed(
            title="❌ Role Not Found",
            description=f"A role with the ID `{role_id}` could not be found. Please check the bot's configuration.",
            color=discord.Color.red()
        )
        await ctx.send(embed=role_not_found_embed, delete_after=10)
        print(f"Error: Role ID {role_id} for amount {amount} not found in guild.")
        return

    if role in member.roles:
        already_has_role_embed = discord.Embed(
            title="⚠️ Role Already Assigned",
            description=f"{member.mention} already has the **{role_name}** role.",
            color=discord.Color.orange()
        )
        await ctx.send(embed=already_has_role_embed, delete_after=10)
        return

    try:
        await member.add_roles(role)

        # Send a confirmation message to the user
        confirm_channel = bot.get_channel(PAYMENT_CHANNEL_ID)
        if confirm_channel:
            try:
                user_embed = discord.Embed(
                    title="🎉 Payment Confirmed!",
                    description=f"Your payment has been confirmed and you’ve been assigned the **{role_name}** role!",
                    color=discord.Color.green()
                )
                user_embed.set_footer(text="Thank you for your support!")
                await confirm_channel.send(member.mention, embed=user_embed)
            except discord.Forbidden:
                print(f"Bot missing permissions to send message to payment confirmation channel ({PAYMENT_CHANNEL_ID}).")
        else:
            print(f"Payment confirmation channel (ID: {PAYMENT_CHANNEL_ID}) not found.")

        # Send a confirmation message to the moderator
        mod_confirm_embed = discord.Embed(
            title="✅ Payment Approved",
            description=f"Successfully assigned **{role_name}** to {member.mention}.",
            color=discord.Color.green()
        )
        mod_confirm_embed.add_field(name="Amount", value=f"${amount}", inline=True)
        mod_confirm_embed.add_field(name="User", value=member.mention, inline=True)
        mod_confirm_embed.set_footer(text=f"Action by {ctx.author.name}")

        await ctx.send(embed=mod_confirm_embed, delete_after=10)

    except discord.Forbidden:
        forbidden_embed = discord.Embed(
            title="❌ Bot Permissions Error",
            description=f"The bot does not have permissions to add the **{role_name}** role.",
            color=discord.Color.red()
        )
        await ctx.send(embed=forbidden_embed, delete_after=10)
        print(f"Bot missing permissions to add role '{role.name}' to {member.display_name}.")
    except discord.HTTPException as e:
        http_error_embed = discord.Embed(
            title="❌ An Error Occurred",
            description=f"An error occurred while adding the role: `{e}`",
            color=discord.Color.red()
        )
        await ctx.send(embed=http_error_embed, delete_after=10)
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
            error_embed = discord.Embed(
                title="❌ Incorrect Channel",
                description=f"This command can only be used in the <#{LEADERBOARD_CHANNEL_ID}> channel.",
                color=discord.Color.red()
            )
            await ctx.send(embed=error_embed, delete_after=10)
        except discord.Forbidden:
            pass
        return

    if member is None:
        member = ctx.author

    user_id = str(member.id)
    user_data = user_points.get(user_id, {"all_time_points": 0.0, "available_points": 0.0})
    all_time_points = user_data.get("all_time_points", 0.0)
    available_points = user_data.get("available_points", 0.0)

    # Calculate rank based on all-time points
    sorted_users = sorted(
        user_points.items(),
        key=lambda item: item[1].get('all_time_points', 0),
        reverse=True
    )

    rank = "Unranked"
    for i, (uid, _) in enumerate(sorted_users, 1):
        if uid == user_id:
            rank = f"#{i}"
            break

    usd_value = available_points * POINTS_TO_USD

    # Create the embed
    embed = discord.Embed(
        title="💰 Points & Rank",
        description=f"Here is the points summary for {member.mention}.",
        color=discord.Color.gold()
    )
    embed.add_field(name="All-Time Points", value=f"**{all_time_points:.2f}**", inline=True)
    embed.add_field(name="Available Points", value=f"**{available_points:.2f}**", inline=True)
    embed.add_field(name="Est. USD Value", value=f"**${usd_value:.2f}**", inline=True)
    embed.add_field(name="Current Rank", value=f"**{rank}**", inline=True)

    embed.set_thumbnail(url=member.avatar.url if member.avatar else member.default_avatar.url)
    embed.set_footer(text="Points are earned through tasks and engagements. 🚀")
    embed.timestamp = datetime.now(UTC)

    await ctx.send(embed=embed, delete_after=30)
    try:
        await ctx.message.delete(delay=5)
    except discord.Forbidden:
        pass


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
            embed = discord.Embed(
                title="❌ Incorrect Channel",
                description=f"This command can only be used in the <#{GIVEAWAY_CHANNEL_ID}> channel.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed, delete_after=10)
        except discord.Forbidden:
            pass
        return

    try:
        await ctx.message.delete()
    except discord.Forbidden:
        pass

    if not members:
        embed = discord.Embed(
            title="❌ Missing Members",
            description="You must mention at least one member to add points to.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=10)
        return

    if points_to_add <= 0:
        embed = discord.Embed(
            title="❌ Invalid Points",
            description="Points to add must be greater than zero.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=10)
        return

    total_points = points_to_add * len(members)
    if admin_points.get("balance", 0) < total_points:
        embed = discord.Embed(
            title="❌ Insufficient Balance",
            description=f"Admin balance is too low to award a total of **{total_points:.2f} points**.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=10)
        return

    winners_list = []
    new_winners_data = []  # The line is here
    for member in members:
        user_id = str(member.id)
        user_points.setdefault(user_id, {"all_time_points": 0.0, "available_points": 0.0})
        user_points[user_id]["all_time_points"] += points_to_add
        user_points[user_id]["available_points"] += points_to_add
        await log_points_transaction(user_id, points_to_add, purpose)
        winner_entry = {
            "user_id": user_id,
            "points": points_to_add,
            "purpose": purpose,
            "timestamp": datetime.now(UTC).isoformat()
        }
        giveaway_winners_log.append(winner_entry)
        all_time_giveaway_winners_log.append(winner_entry)
        new_winners_data.append(winner_entry)
        winners_list.append(member.mention)

    # --- DEDUCT POINTS AFTER THE LOOP ---
    admin_points["balance"] -= total_points
    admin_points["claimed_points"] += total_points

    save_json_file(POINTS_FILE, user_points)
    save_json_file(ADMIN_POINTS_FILE, admin_points)
    save_json_file(GIVEAWAY_LOG_FILE, giveaway_winners_log)
    save_json_file(GIVEAWAY_ALL_TIME_LOG_FILE, all_time_giveaway_winners_log)

    await append_new_winner_to_history(new_winners_data)

    embed = discord.Embed(
        title="🎉 Points Awarded!",
        description=f"The following user(s) have been awarded points:",
        color=discord.Color.gold()
    )
    embed.add_field(name="User(s)", value=', '.join(winners_list), inline=False)
    embed.add_field(name="Points per User", value=f"**{points_to_add:.2f}**", inline=True)
    embed.add_field(name="Total Points Awarded", value=f"**{total_points:.2f}**", inline=True)
    embed.add_field(name="Purpose", value=purpose, inline=False)
    embed.set_footer(text=f"Action by {ctx.author.name}")
    embed.timestamp = datetime.now(UTC)

    await ctx.send(embed=embed)


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
        title="🏆 ManaVerse Global Rankings",
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
        leaderboard_text += f"{medal} **#{i + 1} – {username}**: {data['all_time_points']:.2f} MVpts\n"

    embed.add_field(name="🌟 Top 10 Mana Legends", value=leaderboard_text, inline=False)

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
    """Displays the top 10 users by all-time points in a premium embed format."""
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

    # Correctly filter out admin, mod, and users who left
    eligible_users = {}
    for user_id, data in user_points.items():
        try:
            member = ctx.guild.get_member(int(user_id))
            if member and not any(role.id in [ADMIN_ROLE_ID, MOD_ROLE_ID] for role in member.roles):
                if data['all_time_points'] > 0:
                    eligible_users[user_id] = data
        except (ValueError, discord.NotFound):
            continue

    if not eligible_users:
        await ctx.send("The leaderboard is currently empty. Start earning points!", delete_after=20)
        return

    # Sort users
    sorted_users = sorted(
        eligible_users.items(),
        key=lambda item: item[1]['all_time_points'],
        reverse=True
    )

    # Embed formatting
    embed = discord.Embed(
        title="🏆 ManaVerse Leaderboard 🏆",
        description="The **Top 10 Legends** ranked by all-time points.",
        color=discord.Color.gold()
    )

    medals = ["🥇", "🥈", "🥉"]  # Top 3 medals
    ribbons = ["🎗️"] * 7         # Remaining ranks use ribbon

    for i, (user_id, data) in enumerate(sorted_users[:10]):
        try:
            member = ctx.guild.get_member(int(user_id))
            username = member.display_name if member else f"User ID: {user_id}"
        except (discord.NotFound, discord.HTTPException, AttributeError):
            username = f"Unknown User (ID: {user_id})"

        all_time_points = data['all_time_points']

        # Medal for top 3, ribbon for others
        rank_symbol = medals[i] if i < 3 else f"{ribbons[0]} #{i+1}"

        embed.add_field(
            name=f"{rank_symbol} {username}",
            value=f"**{all_time_points:.2f} MVpts**",
            inline=False
        )

    embed.set_footer(text="Climb the ranks by earning points and show your dominance! 🚀")
    embed.timestamp = datetime.now(UTC)

    await ctx.send(embed=embed)



# === MODIFIED: !requestpayout command with minimum amount and fee ===
@bot.command()
async def requestpayout(ctx, amount: float = None, uid: str = None, exchange: str = None):
    """
    Initiates a two-step payout request, requiring confirmation.
    Usage: !requestpayout <amount> <UID> <Exchange>
    """
    if ctx.channel.id != PAYOUT_REQUEST_CHANNEL_ID:
        try:
            await ctx.message.delete()
        except discord.Forbidden:
            pass
        return

    # 1. Validate all required parameters
    if not amount or not uid or not exchange:
        embed = discord.Embed(
            title="❌ Missing Information",
            description="Please use the correct format: `!requestpayout <Amount> <UID> <Exchange Name>`\n\n**Example:** `!requestpayout 5000 509958013 Binance`",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=15)
        try:
            await ctx.message.delete()
        except discord.Forbidden:
            pass
        return

    if not uid.isdigit():
        embed = discord.Embed(
            title="❌ Invalid UID",
            description="Only numeric exchange UIDs are accepted. Wallet addresses are NOT allowed.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=10)
        try:
            await ctx.message.delete()
        except discord.Forbidden:
            pass
        return

    exchange = exchange.lower()
    if exchange not in APPROVED_EXCHANGES:
        approved_list = ", ".join([e.capitalize() for e in APPROVED_EXCHANGES])
        embed = discord.Embed(
            title="❌ Invalid Exchange",
            description=f"Only these exchanges are accepted: **{approved_list}**",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=10)
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
        embed = discord.Embed(
            title="⚠️ Payout Amount Too Low",
            description=f"The minimum payout amount is **{MIN_PAYOUT_AMOUNT:.2f} points**.",
            color=discord.Color.orange()
        )
        await ctx.send(f"{ctx.author.mention}", embed=embed, delete_after=10)
        try:
            await ctx.message.delete()
        except discord.Forbidden:
            pass
        return

    fee = amount * (PAYOUT_FEE_PERCENTAGE / 100)
    total_deduction = amount + fee

    if balance < total_deduction:
        embed = discord.Embed(
            title="⚠️ Insufficient Points",
            description=f"You do not have enough available points for this request. Your current available balance is **{balance:.2f} points**.",
            color=discord.Color.orange()
        )
        await ctx.send(f"{ctx.author.mention}", embed=embed, delete_after=10)
        try:
            await ctx.message.delete()
        except discord.Forbidden:
            pass
        return

    # 3. Store pending payout and send a confirmation embed
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

    embed = discord.Embed(
        title="🪙 Payout Request Confirmation",
        description=f"You are about to request a payout. Please review the details below:",
        color=discord.Color.gold(),
        timestamp=datetime.now(UTC)
    )
    embed.add_field(name="Requested Amount", value=f"**{amount:.2f} points**", inline=False)
    embed.add_field(name="Exchange", value=f"**{exchange.capitalize()}**", inline=True)
    embed.add_field(name="UID", value=f"**{uid}**", inline=True)
    embed.add_field(name="Fee", value=f"**{PAYOUT_FEE_PERCENTAGE:.1f}% ({fee:.2f} points)**", inline=False)
    embed.add_field(name="Total Deduction", value=f"**{total_deduction:.2f} points**", inline=False)
    embed.set_footer(text=f"Please type `!confirmpayout` to finalize the request within {CONFIRMATION_TIMEOUT} seconds.")

    await ctx.send(f"{ctx.author.mention}", embed=embed)
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
        try:
            embed = discord.Embed(
                title="❌ Incorrect Channel",
                description=f"This command can only be used in the <#{PAYOUT_REQUEST_CHANNEL_ID}> channel.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed, delete_after=10)
            await ctx.message.delete()
        except discord.Forbidden:
            pass
        return

    user_id = str(ctx.author.id)
    user_data = user_points.get(user_id, {})
    pending_payout = user_data.get("pending_payout")

    # 1. Check for valid pending request
    if not pending_payout:
        embed = discord.Embed(
            title="❌ No Request Found",
            description="No pending payout request found. Use `!requestpayout` first.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=10)
        try:
            await ctx.message.delete()
        except discord.Forbidden:
            pass
        return

    # 2. Check for timeout
    if time.time() - pending_payout["timestamp"] > CONFIRMATION_TIMEOUT:
        if "pending_payout" in user_data:
            del user_data["pending_payout"]
            user_points[user_id] = user_data
            save_points()
        embed = discord.Embed(
            title="❌ Request Timed Out",
            description="Your payout request timed out. Please start a new request with `!requestpayout`.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=10)
        try:
            await ctx.message.delete()
        except discord.Forbidden:
            pass
        return

    # 3. Final balance check
    total_deduction = pending_payout["total_deduction"]
    balance = user_data.get("available_points", 0.0)
    if balance < total_deduction:
        embed = discord.Embed(
            title="❌ Insufficient Balance",
            description="You no longer meet the minimum balance for payout.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=10)
        try:
            await ctx.message.delete()
        except discord.Forbidden:
            pass
        return

    # 4. Deduct points from the user's balance and save
    user_data["available_points"] -= total_deduction
    user_points[user_id] = user_data
    save_points()

    # 5. Notify mod
    mod_channel = bot.get_channel(MOD_PAYMENT_REVIEW_CHANNEL_ID)
    if mod_channel:
        try:
            mod_embed = discord.Embed(
                title="📤 New Payout Request",
                description="A new payout request has been submitted for review.",
                color=discord.Color.blue(),
                timestamp=datetime.now(UTC)
            )
            mod_embed.add_field(name="User", value=f"{ctx.author.mention} (`{ctx.author.name}`)", inline=False)
            mod_embed.add_field(name="UID", value=f"**`{pending_payout['uid']}`**", inline=True)
            mod_embed.add_field(name="Exchange", value=f"**`{pending_payout['exchange'].capitalize()}`**", inline=True)
            mod_embed.add_field(name="Requested Amount", value=f"**{pending_payout['amount']:.2f} points**",
                                inline=False)
            mod_embed.add_field(name="Total Deduction", value=f"**{pending_payout['total_deduction']:.2f} points**",
                                inline=False)
            mod_embed.set_footer(text="Use `!paid <@user>` to confirm this payment.")
            await mod_channel.send(embed=mod_embed)
        except discord.Forbidden:
            print(f"Bot missing permissions to send message to mod channel ({MOD_PAYMENT_REVIEW_CHANNEL_ID}).")
        except Exception as e:
            print(f"Error sending mod embed for confirmpayout: {e}")

    # 6. Notify user
    user_embed = discord.Embed(
        title="✅ Payout Submitted",
        description=f"Your payout request for **{pending_payout['amount']:.2f} points** has been successfully submitted for review.",
        color=discord.Color.green()
    )
    user_embed.add_field(name="New Available Balance", value=f"**{user_data['available_points']:.2f} points**",
                         inline=True)
    user_embed.set_footer(text="A moderator will finalize your payment shortly.")
    await ctx.send(f"{ctx.author.mention}", embed=user_embed)
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

    pending_payout = user_data.get("pending_payout")
    if not pending_payout:
        embed = discord.Embed(
            title="❌ Error",
            description=f"**{member.mention}** does not have a pending payout to mark as paid.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=10)
        return

    requested_amount = pending_payout["amount"]

    # The points were already deducted from the user's balance
    # when they used the !confirmpayout command.
    # The `admin_points` balance was not affected by that.

    # Now, we "burn" the points from the admin's balance ledger
    if admin_points.get("balance", 0) < requested_amount:
        embed = discord.Embed(
            title="❌ Transaction Failed",
            description="The admin's balance is insufficient to burn the requested amount.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=10)
        return

    payout_channel = bot.get_channel(PAYOUT_REQUEST_CHANNEL_ID)
    if payout_channel:
        try:
            # --- BEGIN TRANSACTION: Points are burned and data is updated ---
            fee = pending_payout["fee"]

            admin_points["balance"] -= requested_amount
            admin_points["burned_points"] = admin_points.get("burned_points", 0) + requested_amount
            admin_points["fees_earned"] = admin_points.get("fees_earned", 0) + fee
            save_json_file(ADMIN_POINTS_FILE, admin_points)

            # Clear the pending payout
            del user_data["pending_payout"]
            user_points[user_id] = user_data
            save_points()

            # --- Send the premium embed to the payout channel ---
            user_embed = discord.Embed(
                title="💸 Payout Processed!",
                description=f"🎉 {member.mention}, great news! Your payout request has been **successfully processed**.",
                color=discord.Color.green()
            )
            user_embed.add_field(
                name="Status",
                value="✅ Finalized and points have been **burned** from circulation.",
                inline=False
            )
            user_embed.set_thumbnail(url=member.avatar.url if member.avatar else member.default_avatar.url)
            user_embed.set_footer(text="Thank you for being part of the community 🌍")
            user_embed.timestamp = datetime.now(UTC)

            await payout_channel.send(embed=user_embed)

            # Send a confirmation message to the admin
            mod_embed = discord.Embed(
                title="✅ Payout Finalized",
                description=f"A payout success message has been sent to **{member.mention}**.",
                color=discord.Color.green()
            )
            mod_embed.add_field(name="Amount", value=f"{requested_amount:.2f} points", inline=True)
            mod_embed.set_footer(text=f"Payout finalized by {ctx.author.name}")
            await ctx.send(embed=mod_embed, delete_after=10)

        except discord.Forbidden:
            embed = discord.Embed(
                title="❌ Permissions Error",
                description="I am missing permissions to send the payout notification in the payout channel.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed, delete_after=10)
            print(
                f"Bot missing permissions to send the payout notification in {payout_channel.name} ({payout_channel.id})."
            )
        except Exception as e:
            embed = discord.Embed(
                title="❌ An Error Occurred",
                description=f"An unexpected error occurred during the payout transaction. Please check the logs.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed, delete_after=10)
            print(f"An unexpected error occurred during the payout transaction: {e}")

    else:
        embed = discord.Embed(
            title="❌ Configuration Error",
            description=f"The payout channel (ID: `{PAYOUT_REQUEST_CHANNEL_ID}`) could not be found. Please check your configuration.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=10)


# === NEW: !xp command ===

@bot.command(name="xp")
@commands.cooldown(2, 60, commands.BucketType.user)
async def xp_command(ctx, member: discord.Member = None):
    """
    Displays the user's current total XP and rank.
    Usage: !xp to check your own or !xp @user to check another's.
    """
    try:
        await ctx.message.delete(delay=0)
    except discord.Forbidden:
        pass  # Bot lacks permission to delete the user's message

    # Channel Restriction Check
    if ctx.channel.id != XP_REWARD_CHANNEL_ID:
        await ctx.send(
            f"❌ The `!xp` command can only be used in the <#{XP_REWARD_CHANNEL_ID}> channel.",
            delete_after=15
        )
        return

    # Use ctx.author if no member is specified
    target_member = member if member else ctx.author
    user_id = str(target_member.id)

    # --- Step 1: Filter out admins and mods, then sort the remaining users ---
    guild = bot.get_guild(SERVER_ID)
    if not guild:
        await ctx.send("❌ Error: Could not find the server. Please check the SERVER_ID constant.", delete_after=15)
        return

    allowed_roles = [ADMIN_ROLE_ID, MOD_ROLE_ID]
    all_users = []

    for uid, data in user_xp.items():
        member_obj = guild.get_member(int(uid))
        if member_obj and not any(role.id in allowed_roles for role in member_obj.roles):
            all_users.append((uid, data.get("xp", 0)))

    # Sort the list of eligible users by XP
    sorted_xp_users = sorted(all_users, key=lambda item: item[1], reverse=True)

    # --- Step 2: Calculate the user's rank and XP balance ---
    xp_balance = user_xp.get(user_id, {}).get("xp", 0)

    # Find the user's rank within the sorted, filtered list
    user_rank = next((i for i, (uid, _) in enumerate(sorted_xp_users) if uid == user_id), None)

    if xp_balance == 0:
        embed = discord.Embed(
            title="📊 XP Tracker",
            description=f"❌ {target_member.mention} has not earned any XP yet.",
            color=discord.Color.red()
        )
        embed.set_footer(text="Keep chatting and completing quests to gain XP!")
        embed.timestamp = datetime.now(UTC)
        await ctx.send(embed=embed, delete_after=15)
        return

    # Assign medals for top 3
    medal = ""
    if user_rank == 0:
        medal = "🥇"
    elif user_rank == 1:
        medal = "🥈"
    elif user_rank == 2:
        medal = "🥉"

    # --- Step 3: Build and send the embed ---
    embed = discord.Embed(
        title="🌟 XP Status",
        description=f"{medal} {target_member.mention}'s XP Summary",
        color=discord.Color.blue()
    )
    embed.add_field(name="Total XP", value=f"**{xp_balance:,} XP**", inline=True)

    if user_rank is not None:
        embed.add_field(name="Rank", value=f"**#{user_rank + 1}** out of {len(sorted_xp_users)}", inline=True)
    else:
        embed.add_field(name="Rank", value="Unranked", inline=True)

    embed.set_thumbnail(url=target_member.avatar.url if target_member.avatar else target_member.default_avatar.url)
    embed.set_footer(text="ManaVerse XP System • Keep earning to climb the ranks!")
    embed.timestamp = datetime.now(UTC)

    await ctx.send(embed=embed, delete_after=30)


# === Weekly Quest Commands ===
@bot.command()
@commands.has_permissions(administrator=True)
async def quests(ctx, *, all_quests: str):
    """
    (Admin Only) Posts 3 new weekly quests to the quest board.
    Usage: !quests Quest 1 description\nQuest 2 description\nQuest 3 description
    """
    # Clean up the command message immediately
    try:
        await ctx.message.delete()
    except discord.Forbidden:
        pass

    quests_list = [q.strip() for q in all_quests.strip().split("\n")]
    if len(quests_list) != 3:
        embed = discord.Embed(
            title="❌ Invalid Input",
            description="Please provide **exactly 3 quests**, with each quest on a new line.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=15)
        return

    global weekly_quests, quest_submissions

    weekly_quests["week"] += 1
    weekly_quests["quests"] = quests_list
    save_weekly_quests()

    quest_submissions = {}
    save_quest_submissions()

    board = bot.get_channel(QUEST_BOARD_CHANNEL_ID)
    if board:
        embed = discord.Embed(
            title=f"📋 Weekly Quests – Week {weekly_quests['week']}",
            description="Complete the quests below and submit proof using `!submitquest <quest_number> <tweet_link>`",
            color=discord.Color.gold()
        )
        for i, q in enumerate(quests_list, start=1):
            embed.add_field(
                name=f"⚔️ Quest {i}",
                value=f"{q}",
                inline=False
            )

        embed.set_footer(text="Earn +100 Points for each approved quest • Good luck!")
        embed.timestamp = datetime.now(UTC)

        try:
            await board.send(embed=embed)
            success_embed = discord.Embed(
                title="✅ Quests Posted!",
                description="New quests have been successfully posted and previous submissions have been reset.",
                color=discord.Color.green()
            )
            await ctx.send(embed=success_embed, delete_after=15)
        except discord.Forbidden:
            print(f"Bot missing permissions to send message to quest board channel ({QUEST_BOARD_CHANNEL_ID}).")
            error_embed = discord.Embed(
                title="❌ Error Posting Quests",
                description="I am missing permissions to send messages to the quest board channel.",
                color=discord.Color.red()
            )
            await ctx.send(embed=error_embed, delete_after=15)
        except discord.HTTPException as e:
            print(f"HTTP Error posting quests: {e}")
            error_embed = discord.Embed(
                title="❌ An Unexpected Error Occurred",
                description=f"An error occurred while trying to post the quests. Please check the console.",
                color=discord.Color.red()
            )
            await ctx.send(embed=error_embed, delete_after=15)
    else:
        print(f"Quest board channel (ID: {QUEST_BOARD_CHANNEL_ID}) not found.")
        error_embed = discord.Embed(
            title="❌ Configuration Error",
            description="The quest board channel could not be found. Please check the `QUEST_BOARD_CHANNEL_ID`.",
            color=discord.Color.red()
        )
        await ctx.send(embed=error_embed, delete_after=15)


@bot.command()
async def submitquest(ctx, quest_number: int, tweet_link: str):
    """
    Submits a weekly quest for review.
    Usage: !submitquest <quest_number> <tweet_link>
    """
    if ctx.channel.id != QUEST_SUBMIT_CHANNEL_ID:
        embed = discord.Embed(
            title="❌ Incorrect Channel",
            description=f"Please use the <#{QUEST_SUBMIT_CHANNEL_ID}> channel to submit quests.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=10)
        try:
            await ctx.message.delete(delay=5)
        except discord.Forbidden:
            pass
        return

    user_id = str(ctx.author.id)
    week = str(weekly_quests.get("week", "0"))

    if int(week) == 0 or not weekly_quests.get("quests"):
        embed = discord.Embed(
            title="❌ No Active Quests",
            description="There are no active weekly quests right now. Please wait for new quests to be posted!",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=15)
        try:
            await ctx.message.delete(delay=5)
        except discord.Forbidden:
            pass
        return

    if quest_number not in [1, 2, 3]:
        embed = discord.Embed(
            title="❌ Invalid Quest Number",
            description="Quest number must be 1, 2, or 3.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=10)
        try:
            await ctx.message.delete(delay=5)
        except discord.Forbidden:
            pass
        return

    normalized_tweet_link = normalize_url(tweet_link)
    if not normalized_tweet_link or "twitter.com/".lower() not in normalized_tweet_link:
        embed = discord.Embed(
            title="❌ Invalid Link",
            description="Please provide a valid Twitter/X link for your quest submission.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=15)
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

        embed = discord.Embed(
            title="🚫 Duplicate Submission",
            description=f"{ctx.author.mention}, this proof (tweet) has already been submitted and approved for a quest or engagement. Please ensure your quest proofs are unique.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=20)
        return

    user_week_data = quest_submissions.setdefault(user_id, {}).setdefault(week, {})

    if str(quest_number) in user_week_data:
        status = user_week_data[str(quest_number)]["status"]
        if status == "pending":
            embed = discord.Embed(
                title="⚠️ Pending Submission",
                description="You already have a pending submission for this quest. Please wait for it to be reviewed.",
                color=discord.Color.orange()
            )
            await ctx.send(embed=embed, delete_after=15)
        elif status == "approved":
            embed = discord.Embed(
                title="⚠️ Already Completed",
                description="You have already successfully completed and been approved for this quest.",
                color=discord.Color.orange()
            )
            await ctx.send(embed=embed, delete_after=15)
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
            embed = discord.Embed(
                title="🧩 New Quest Submission",
                description=f"A new quest has been submitted for review by {ctx.author.mention}.",
                color=discord.Color.blue(),
                timestamp=datetime.now(UTC)
            )
            embed.add_field(name="User", value=f"**{ctx.author.name}**", inline=False)
            embed.add_field(name="Quest #", value=f"**{quest_number}**", inline=True)
            embed.add_field(name="Week", value=f"**{week}**", inline=True)
            embed.add_field(name="Link", value=f"[Click to view]({tweet_link})", inline=False)
            embed.set_footer(text=f"To verify: !verifyquest {user_id} {quest_number} <approve|reject>")

            await mod_review_channel.send(embed=embed)
        except discord.Forbidden:
            print(
                f"Bot missing permissions to send message to mod quest review channel ({MOD_QUEST_REVIEW_CHANNEL_ID}).")
        except discord.HTTPException as e:
            print(f"HTTP Error sending quest mod notification: {e}")
    else:
        print(f"Mod quest review channel (ID: {MOD_QUEST_REVIEW_CHANNEL_ID}) not found.")

    embed = discord.Embed(
        title="✅ Submission Received!",
        description=f"Your submission for **Quest {quest_number}** has been received and sent for review.",
        color=discord.Color.green()
    )
    await ctx.send(f"{ctx.author.mention}", embed=embed, delete_after=15)
    try:
        await ctx.message.delete(delay=5)
    except discord.Forbidden:
        pass


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
            embed = discord.Embed(
                title="❌ Incorrect Channel",
                description=f"This command can only be used in the <#{MOD_QUEST_REVIEW_CHANNEL_ID}> channel.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed, delete_after=10)
        except discord.Forbidden:
            pass
        return

    user_id = str(member.id)
    week = str(weekly_quests.get("week", "0"))
    action = action.lower()

    if user_id not in quest_submissions or week not in quest_submissions[user_id]:
        embed = discord.Embed(
            title="❌ Submission Not Found",
            description="No quest submission found for this user for the current week.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, delete_after=10)
        return

    quest_data = quest_submissions[user_id][week]

    if str(quest_number) not in quest_data:
        embed = discord.Embed(
            title="⚠️ Quest Not Submitted",
            description=f"Quest **{quest_number}** was not submitted by {member.mention} for this week.",
            color=discord.Color.orange()
        )
        await ctx.send(embed=embed, delete_after=10)
        return

    submission_status = quest_data[str(quest_number)]["status"]
    if submission_status == "approved":
        embed = discord.Embed(
            title="⚠️ Already Approved",
            description=f"Quest **{quest_number}** for {member.mention} is already approved.",
            color=discord.Color.orange()
        )
        await ctx.send(embed=embed, delete_after=10)
        return

    reply_channel = bot.get_channel(QUEST_SUBMIT_CHANNEL_ID)
    if not reply_channel:
        print(f"Warning: Quest submission channel ({QUEST_SUBMIT_CHANNEL_ID}) not found. Replying in current channel.")
        reply_channel = ctx.channel

    if action == "approve":
        points_to_award = QUEST_POINTS
        if admin_points.get("balance", 0) < points_to_award:
            embed = discord.Embed(
                title="❌ Admin Balance Too Low",
                description=f"The admin balance is too low to award **{points_to_award:.2f} points**.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed, delete_after=10)
            print("⚠️ Admin balance is too low to award quest points. Skipping.")
            return

        if user_id not in user_points:
            user_points[user_id] = {"all_time_points": 0.0, "available_points": 0.0}

        user_points[user_id]["all_time_points"] += points_to_award
        user_points[user_id]["available_points"] += points_to_award
        quest_data[str(quest_number)]["status"] = "approved"

        await log_points_transaction(user_id, points_to_award, f"Quest {quest_number} approval")
        save_json_file(POINTS_FILE, user_points)

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
            embed = discord.Embed(
                title="✨ Quest Approved! ✨",
                description=f"🎉 Congratulations, {member.mention}! Your submission for **Quest {quest_number}** has been **approved**!",
                color=discord.Color.green()
            )
            embed.add_field(name="Points Earned", value=f"💰 **+{points_to_award:.2f} points**", inline=False)
            embed.add_field(name="New Balance", value=f"🪙 **{user_points[user_id]['available_points']:.2f} points**", inline=False)
            embed.set_footer(text=f"Great job! Keep an eye out for next week's quests!")
            embed.timestamp = datetime.now(UTC)
            await reply_channel.send(embed=embed)
        except discord.Forbidden:
            print(
                f"Bot missing permissions to send approval message to quest submission channel ({QUEST_SUBMIT_CHANNEL_ID}).")

    elif action == "reject":
        quest_data[str(quest_number)]["status"] = "rejected"
        save_quest_submissions()

        try:
            embed = discord.Embed(
                title="❌ Quest Rejected",
                description=f"Hello, {member.mention}. Your submission for **Quest {quest_number}** was **rejected**.",
                color=discord.Color.red()
            )
            embed.add_field(name="Reason", value="Your submission did not meet the quest requirements. Please review the rules and try again if necessary.", inline=False)
            embed.set_footer(text="Keep trying! We look forward to your next submission.")
            embed.timestamp = datetime.now(UTC)
            await reply_channel.send(embed=embed)
        except discord.Forbidden:
            print(
                f"Bot missing permissions to send rejection message to quest submission channel ({QUEST_SUBMIT_CHANNEL_ID}).")
    else:
        embed = discord.Embed(
            title="⚠️ Invalid Action",
            description="Please use **'approve'** or **'reject'**.",
            color=discord.Color.orange()
        )
        await ctx.send(embed=embed, delete_after=10)
        return

    # Final confirmation to moderator
    confirmation_embed = discord.Embed(
        title="✅ Quest Verified",
        description=f"Quest **{quest_number}** for **{member.name}** has been marked as '{action}'.",
        color=discord.Color.green()
    )
    confirmation_embed.set_footer(text=f"Action by {ctx.author.name}")
    await ctx.send(embed=confirmation_embed, delete_after=10)


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

        # You will need to import these at the top of your script
        # from datetime import datetime, UTC
        # import discord

        if "gm" in content or "g1st" in content:
            user_id = str(message.author.id)
            today = str(datetime.now().date())

            # Check if the user is eligible for points today
            if user_id not in gm_log or gm_log.get(user_id) != today:

                # --- Check if the user is an Admin ---
                is_admin = any(role.id == ADMIN_ROLE_ID for role in message.author.roles)

                try:
                    if is_admin:
                        # Award points to the Admin's personal balance
                        admin_points["balance"] -= GM_G1ST_POINTS_REWARD
                        admin_points["my_points"] += GM_G1ST_POINTS_REWARD
                        admin_points["claimed_points"] += GM_G1ST_POINTS_REWARD
                        save_json_file(ADMIN_POINTS_FILE, admin_points)

                        await log_points_transaction(user_id, GM_G1ST_POINTS_REWARD, "GM points")

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

                    # Log that the user received points for today
                    gm_log[user_id] = today
                    save_json_file(GM_LOG_FILE, gm_log)

                    # --- Send the premium embed ---
                    embed = discord.Embed(
                        title="🎉 Morning Points Awarded! 🎉",
                        description=f"Congratulations, {message.author.mention}! You've been rewarded **{GM_G1ST_POINTS_REWARD:.2f} points** for your morning message.",
                        color=discord.Color.gold()
                    )
                    # This GIF URL is a placeholder. You can replace it with any GIF you want!
                    embed.set_image(url="https://media.tenor.com/Fw5m_qY3S2gAAAAC/puffed-celebration.gif")
                    embed.set_footer(
                        text=f"Your new balance is {user_points.get(user_id, {}).get('available_points', 0):.2f} points" if not is_admin else "Points have been added to your admin balance.")
                    embed.timestamp = datetime.now(UTC)

                    await message.channel.send(embed=embed, delete_after=10)

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

    # --- The code you are asking about goes here ---
    cleaned_content = message.content.lower().translate(str.maketrans('', '', string.punctuation))
    message_words = cleaned_content.split()

    for word in message_words:
        if word in banned_words:
            try:
                await message.delete(delay=0)
                await message.channel.send(f'🚫 {message.author.mention}, that message contains a banned word!',
                                           delete_after=10)
            except discord.Forbidden:
                print(
                    f"Bot missing permissions to delete or send message in {message.channel.name} (Banned Words).")
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
            # Create a premium embed to send to the moderator channel
            mod_embed = discord.Embed(
                title="💰 Payment Confirmation",
                description=f"Payment proof received from {message.author.mention}.",
                color=discord.Color.gold()
            )
            mod_embed.add_field(name="User ID", value=message.author.id, inline=True)
            mod_embed.add_field(name="Username", value=message.author.name, inline=True)
            mod_embed.add_field(name="Message", value=message.content, inline=False)
            mod_embed.set_thumbnail(
                url=message.author.avatar.url if message.author.avatar else message.author.default_avatar.url)
            mod_embed.timestamp = datetime.now(UTC)

            files = []
            if message.attachments:
                mod_embed.add_field(name="Attachments", value="See attached file(s) below.", inline=False)
                try:
                    files = [await a.to_file() for a in message.attachments]
                except discord.HTTPException as e:
                    print(f"Error fetching attachment files for payment confirmation: {e}")

            try:
                await mod_channel.send(embed=mod_embed, files=files)
            except discord.Forbidden:
                print(
                    f"Bot missing permissions to send message to payment review channel ({MOD_PAYMENT_REVIEW_CHANNEL_ID}).")
        else:
            print(f"Payment review channel (ID: {MOD_PAYMENT_REVIEW_CHANNEL_ID}) not found for payment forwarding.")

        try:
            await message.delete(delay=0)

            # Create a premium embed to send to the user
            user_embed = discord.Embed(
                title="✅ Payment Proof Received!",
                description=f"{message.author.mention}, your payment proof has been received and is under review. Please await moderator approval.",
                color=discord.Color.green()
            )
            user_embed.set_footer(text="Thank you for your patience.")
            user_embed.timestamp = datetime.now(UTC)
            await message.channel.send(embed=user_embed, delete_after=15)

        except discord.Forbidden:
            print(
                f"Bot missing permissions to delete message or send confirmation in {message.channel.name} (Payment).")
            # Send a basic embed if the bot lacks permissions
            error_embed = discord.Embed(
                title="🚫 Permission Error!",
                description="Could not delete your message or send a proper confirmation. Your payment proof was likely still forwarded.",
                color=discord.Color.red()
            )
            error_embed.set_footer(text="Please contact an admin if you have concerns.")
            await message.channel.send(embed=error_embed, delete_after=15)

        # Handle ticket creation only if the message is in the support channel
        if message.channel.id == SUPPORT_CHANNEL_ID:
            user_id = message.author.id

            # Check if the user already has an active ticket
            if user_id in active_tickets.values():
                embed = discord.Embed(
                    title="❌ Active Ticket Found",
                    description="You already have an active ticket. Please close it before opening a new one.",
                    color=discord.Color.red()
                )
                await message.channel.send(embed=embed, delete_after=10)
                await message.delete()
                return

            guild = message.guild
            user = message.author
            ticket_name = f"ticket-{user.name.lower()}"

            overwrites = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False),
                guild.get_role(ADMIN_ROLE_ID): discord.PermissionOverwrite(view_channel=True),
                guild.get_role(MOD_ROLE_ID): discord.PermissionOverwrite(view_channel=True),
                user: discord.PermissionOverwrite(view_channel=True, send_messages=True),
                guild.get_member(bot.user.id): discord.PermissionOverwrite(view_channel=True, send_messages=True)
            }

            try:
                ticket_channel = await guild.create_text_channel(
                    ticket_name,
                    category=guild.get_channel(TICKETS_CATEGORY_ID),
                    overwrites=overwrites
                )

                # Send a welcome embed to the new ticket channel
                welcome_embed = discord.Embed(
                    title="🎫 New Support Ticket",
                    description=f"Thank you for reaching out, {user.mention}. A support team member will be with you shortly. Please provide any relevant information to help us assist you.",
                    color=discord.Color.blue(),
                    timestamp=datetime.now(UTC)
                )
                welcome_embed.add_field(name="Original Message", value=f"> {message.content}", inline=False)
                welcome_embed.set_footer(text="A team member will respond soon.")

                await ticket_channel.send(f"{user.mention}", embed=welcome_embed)
                await ticket_channel.send(
                    f"Support team, you have a new ticket from {user.mention}! Use `!close` to close this ticket after assisting the user.")

                # Delete the original message from the support channel
                await message.delete()

                active_tickets[ticket_channel.id] = user.id
                save_json_file(ACTIVE_TICKETS_FILE, active_tickets)

            except discord.Forbidden:
                print("Bot is missing permissions to create channels or manage roles.")
                embed = discord.Embed(
                    title="❌ Permissions Error",
                    description="An error occurred. I don't have the permissions to create a ticket.",
                    color=discord.Color.red()
                )
                await message.channel.send(embed=embed, delete_after=10)
            except Exception as e:
                print(f"❌ An unhandled error occurred in ticket creation: {e}")
                embed = discord.Embed(
                    title="❌ An Error Occurred",
                    description="An unexpected error occurred while creating the ticket.",
                    color=discord.Color.red()
                )
                await message.channel.send(embed=embed, delete_after=10)

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



#-------------------------- D I S C O R D         E M B E D -------------------------------------------------

@bot.command()
@commands.has_permissions(administrator=True)
async def announce(ctx, title: str, *, message: str):
    """
    (Admin Only) Posts a new announcement to the announcement channel as a premium embed.
    Usage: !announce "Your Title Here" Your announcement message here
    """
    try:
        announcement_channel = bot.get_channel(ANNOUNCEMENT_CHANNEL_ID)

        if announcement_channel:
            embed = discord.Embed(
                title=title,
                description=message,
                color=discord.Color.blue()
            )
            embed.set_footer(text="Official Server Announcement")
            embed.timestamp = datetime.now(UTC)

            await announcement_channel.send(embed=embed)
            await ctx.send("✅ Announcement posted successfully!", delete_after=10)
        else:
            await ctx.send("❌ Announcement channel not found. Please check the ID.", delete_after=10)

    except discord.Forbidden:
        await ctx.send("❌ I don't have permission to post in the announcement channel.", delete_after=10)

    # You can choose to delete the command message for a cleaner look
    try:
        await ctx.message.delete()
    except discord.Forbidden:
        pass



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
        await ctx.send(f"❌ You need **{MYSTERYBOX_COST} MVpts** to open a Mystery Box.", delete_after=8)
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
        await log_ch.send(f"🎁 Mystery Box used by <@{user_id}> — reward: **{reward}** MVpts")

    # Result embed (no burn disclosure)
    color = discord.Color.green() if reward >= MYSTERYBOX_COST else discord.Color.orange()
    embed = discord.Embed(
        title="🎁 Mystery Box Opened!",
        description=f"{ctx.author.mention} you spent **{MYSTERYBOX_COST} MVpts** and received:",
        color=color
    )
    embed.add_field(name="Reward", value=f"💎 **{reward} points**", inline=False)
    embed.set_footer(text="Good luck next time!" if reward < MYSTERYBOX_COST else "Nice hit!")
    embed.timestamp = datetime.now(UTC)
    await ctx.send(embed=embed)


if __name__ == "__main__":
    bot.run(token, log_handler=handler, log_level=logging.DEBUG)