import discord
from discord.ext import commands
import random
import re
import asyncio
import string
import time
from datetime import datetime, UTC

# These are the only necessary local imports
from logger import bot_logger as logger
from utils import normalize_url
import config


class AdminCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

# ======== T H E      E M B E D      M E S S A G E        M E C H A N I S M ===
    #  E C O N O M Y     E M B E D     M E S S A G E
    async def get_economy_embed(self):
        """
        Builds a premium embed for the economy status message.
        """
        try:
            admin_data = await self.bot.load_single_json("admin_points", "main", {})
            # Use bot attribute for color
            embed_color = discord.Color.from_rgb(255, 204, 0)

            # Retrieve all point values with a default of 0.0
            balance = admin_data.get("balance", 0.0)
            in_circulation = admin_data.get("in_circulation", 0.0)
            burned = admin_data.get("burned", 0.0)
            treasury = admin_data.get("treasury", 0.0)
            my_points = admin_data.get("my_points", 0.0)
            total_supply = admin_data.get("total_supply", 0.0)

            # Calculate USD values using the constant from self.bot
            usd_total_supply = total_supply * config.POINTS_TO_USD
            usd_balance = balance * config.POINTS_TO_USD
            usd_in_circulation = in_circulation * config.POINTS_TO_USD
            usd_burned = burned * config.POINTS_TO_USD
            usd_treasury = treasury * config.POINTS_TO_USD
            usd_my_points = my_points * config.POINTS_TO_USD

            # Create the embed object
            embed = discord.Embed(
                title="🪙 ManaVerse Economy Status",
                description="A real-time overview of the points economy.",
                color=embed_color
            )

            # Add fields for each data point
            embed.add_field(name="Total Supply", value=f"**{total_supply:,.2f}** points\n(${usd_total_supply:,.2f})",
                            inline=False)
            embed.add_field(name="Remaining Supply", value=f"**{balance:,.2f}** points\n(${usd_balance:,.2f})",
                            inline=True)
            embed.add_field(name="In Circulation",
                            value=f"**{in_circulation:,.2f}** points\n(${usd_in_circulation:,.2f})",
                            inline=True)
            embed.add_field(name="Burned", value=f"**{burned:,.2f}** points\n(${usd_burned:,.2f})",
                            inline=True)
            embed.add_field(name="Treasury", value=f"**{treasury:,.2f}** points\n(${usd_treasury:,.2f})", inline=True)
            embed.add_field(name="Admin's Earned Points", value=f"**{my_points:,.2f}** points\n(${usd_my_points:,.2f})",
                            inline=True)

            # Add a footer with a timestamp
            embed.set_footer(text="Data is updated in real-time.")
            embed.timestamp = datetime.now(UTC)

            return embed

        except Exception as e:
            logger.error(f"❌ An error occurred while building the economy embed: {e}")
            error_embed = discord.Embed(
                title="❌ An Error Occurred",
                description="An error occurred while building the economy embed. Check the logs for details.",
                color=discord.Color.red()
            )
            return error_embed


    #    R E F E R R A L       L E A D E R B O A R D           E M B E D            M E S S A G E
    async def get_referral_leaderboard_embed(self):
        """
        Generates a premium referral leaderboard embed from the referral data.
        """
        embed = discord.Embed(
            title="🏆 Top 10 Referral Leaderboard",
            description="These are the top community members who are growing the server! 🚀",
            color=discord.Color.gold()
        )
        referral_data = await self.bot.load_all_json("referral_data")

        # Count referrals for each user
        referral_counts = {}
        for user_id, referrer_id in referral_data.items():
            if int(referrer_id) != self.bot.user.id:
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

        # Use a list comprehension and join for better performance
        medals = ["🥇", "🥈", "🥉"]
        leaderboard_lines = []

        for rank, (user_id, count) in enumerate(sorted_referrals[:10], 1):
            if count == 0:
                continue

            user = self.bot.get_user(int(user_id))
            user_name = user.display_name if user else f"User ID: {user_id}"

            # Determine the medal emoji for the rank
            medal = medals[rank - 1] if rank <= 3 else "🏅"

            leaderboard_lines.append(f"**{medal}** **#{rank}.** {user_name} with **{count}** referrals")

        leaderboard_text = "\n".join(leaderboard_lines)

        embed.add_field(name="🌟 Top Referrers", value=leaderboard_text, inline=False)
        embed.set_footer(text="Updated periodically. Keep referring friends! 💖")
        embed.timestamp = datetime.now(UTC)
        return embed


    #   P O I N T S        L E A D E R B O A R D      E M B E D      M E S S A G E
    async def get_points_leaderboard_embed(self):
        """
        Generates a formatted points leaderboard embed with medal logic.
        """
        # 1. Get the guild once for efficiency
        guild = self.bot.get_guild(config.SERVER_ID)
        if not guild:
            logger.error(f"❌ Guild with ID {config.SERVER_ID} not found.")
            return discord.Embed(description="Server not found. Please check configuration.")

        users_points = await self.bot.load_all_json(self, "user_points")

        # 2. Filter eligible users efficiently
        eligible_users = {}
        for member in guild.members:
            # Check if the member has the admin or mod role
            if any(role.id in [config.ADMIN_ROLE_ID, config.MOD_ROLE_ID] for role in member.roles):
                continue

            # Check if the user has points in the database
            user_data = users_points.get(str(member.id))
            if user_data and user_data.get('all_time_points', 0) > 0:
                eligible_users[str(member.id)] = user_data

        # 3. Sort the eligible users by their points
        sorted_points = sorted(
            eligible_users.items(),
            key=lambda item: item[1].get('all_time_points', 0),
            reverse=True
        )

        # 4. Create the embed
        embed = discord.Embed(
            title="💰 Points Leaderboard",
            description="Here are the top members with the most points! 💎",
            color=discord.Color.green()
        )

        if not sorted_points:
            embed.description = "The points leaderboard is currently empty. Start earning points!"
            return embed

        # 5. Build the leaderboard text using a list and .join()
        medals = ["🥇", "🥈", "🥉"]
        leaderboard_lines = []
        for rank, (user_id, points_data) in enumerate(sorted_points[:10], 1):
            points = points_data.get('all_time_points', 0)

            user = self.bot.get_user(int(user_id))
            user_name = user.display_name if user else f"User ID: {user_id}"

            medal = medals[rank - 1] if rank <= 3 else "🏅"
            leaderboard_lines.append(f"**{medal}** **#{rank}.** {user_name} with **{points:,.2f} points**")

        embed.add_field(name="🌟 Top Point Earners", value="\n".join(leaderboard_lines), inline=False)
        embed.set_footer(text="Updated periodically. Keep earning points! 🚀")
        embed.timestamp = datetime.now(UTC)

        return embed


    #   X P      L E A D E R B O A R D       E M B E D      M E S S A G E
    async def get_xp_leaderboard_embed(self):
        """
        Generates a premium XP leaderboard embed.
        """
        # 1. Get the guild once for efficiency
        guild = self.bot.get_guild(config.SERVER_ID)
        if not guild:
            logger.error(f"❌ Guild with ID {config.SERVER_ID} not found.")
            return discord.Embed(description="Server not found. Please check configuration.")

        user_xp = await self.bot.load_all_json("user_xp")

        # 2. Filter eligible users efficiently
        eligible_users = {}
        for member in guild.members:
            # Check if the member has the admin or mod role
            if any(role.id in [config.ADMIN_ROLE_ID, config.MOD_ROLE_ID] for role in member.roles):
                continue

            # Check if the user has XP in the database
            user_data = user_xp.get(str(member.id))
            if user_data and user_data.get('xp', 0) > 0:
                eligible_users[str(member.id)] = user_data

        # 3. Sort the eligible users by their XP
        sorted_xp = sorted(
            eligible_users.items(),
            key=lambda item: item[1].get('xp', 0),
            reverse=True
        )

        # 4. Create the embed
        embed = discord.Embed(
            title="🔥 XP Leaderboard",
            description="These members have the most Mana XP! 🌟",
            color=discord.Color.blue()
        )

        if not sorted_xp:
            embed.description = "The XP leaderboard is currently empty."
            return embed

        # 5. Build the leaderboard text using a list and .join()
        medals = ["🥇", "🥈", "🥉"]
        leaderboard_lines = []
        for rank, (user_id, xp_data) in enumerate(sorted_xp[:10], 1):
            xp = xp_data.get('xp', 0)

            # Get the user from cache for performance
            user = self.bot.get_user(int(user_id))
            user_name = user.display_name if user else f"User ID: {user_id}"

            medal = medals[rank - 1] if rank <= 3 else "🏅"
            leaderboard_lines.append(f"**{medal}** **#{rank}.** {user_name} with **{xp} XP**")

        embed.add_field(name="🌟 Top XP Earners", value="\n".join(leaderboard_lines), inline=False)
        embed.set_footer(text="Updated periodically.")
        embed.timestamp = datetime.now(UTC)

        return embed

    async def append_new_winner_to_history(self):
        """
        Moves winners from the temporary giveaway log to the permanent history log.
        This function should be called after a new winner is added to the temporary log.
        """
        # ✅ STEP 1: Load the data from the database
        temporary_winners = await self.bot.load_list_of_json("giveaway_logs")
        all_time_winners = await self.bot.load_list_of_json("all_time_giveaway_logs")

        if not temporary_winners:
            logger.info("No new giveaway winners to append to history.")
            return

        # ✅ STEP 2: Append the new winners to the all-time log
        all_time_winners.extend(temporary_winners)

        # ✅ STEP 3: Implement the list size limit
        if len(all_time_winners) > config.MAX_WINNERS_HISTORY:
            entries_to_remove = len(all_time_winners) - config.MAX_WINNERS_HISTORY
            del all_time_winners[:entries_to_remove]

        # ✅ STEP 4: Clear the temporary log in memory
        temporary_winners.clear()

        logger.info("✅ New winners appended to the all-time log and temporary log cleared.")

        # ✅ STEP 5: Save both lists back to the database
        await self.bot.save_list_of_json("all_time_giveaway_logs", self.all_time_giveaway_winners_log)
        await self.bot.save_list_of_json("giveaway_logs", self.giveaway_winners_log)

        # 6. Call the helper function to update the history message
        await self.bot.update_giveaway_winners_history_message()

    # === P O I N T S    H I S T O R Y    M E S S A G E ===
    async def update_points_history_message(self):
        """Periodically updates the point history message in a dedicated channel."""

        # ✅ STEP 1: Load the data from the database
        points_history = await self.bot.load_list_of_json("points_history")
        bot_data = await self.bot.load_single_json("bot_data", "main", {})

        # 1. Access the channel ID from the bot object
        channel = self.bot.get_channel(config.POINTS_HISTORY_CHANNEL_ID)
        if not channel:
            logger.error(f"❌ Error: Points history channel with ID {config.POINTS_HISTORY_CHANNEL_ID} not found.")
            return

        # 2. Build the history message content efficiently
        if not points_history:
            history_message = "📈 **Points History**\nNo transactions to display yet."
        else:
            recent_history = points_history[-15:]
            history_lines = []

            for entry in recent_history:
                user = self.bot.get_user(int(entry["user_id"]))
                user_name = user.display_name if user else f"User ID: {entry['user_id']}"
                points = entry["points"]
                purpose = entry["purpose"]
                timestamp = datetime.fromisoformat(entry["timestamp"]).strftime('%Y-%m-%d %H:%M')
                history_lines.append(
                    f"💵• `{timestamp}`: **{user_name}** earned **{points:.2f} points** for **{purpose}**.")

            history_message = "\n".join(history_lines)

        # --- Convert the content to a Discord Embed ---
        history_embed = discord.Embed(
            title="📈 Points History",
            description=history_message,
            color=discord.Color.blue()
        )

        # 3. Use the utility function to manage the message with the new embed
        await self.bot.manage_periodic_message(
            channel=channel,
            bot_data=bot_data,  # Pass the loaded bot_data
            message_id_key="history_message_id",
            embed=history_embed
        )

#  === V E R I F I C A T I O N      M E C H A N I S M ===
    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload):
        """Handles reaction role assignments."""
        # Ignore reactions not in the correct channel or on the correct message
        if payload.channel_id != config.VERIFY_CHANNEL_ID or payload.message_id != config.VERIFY_MESSAGE_ID:
            return

        guild = self.bot.get_guild(payload.guild_id)
        if not guild:
            return

        member = guild.get_member(payload.user_id)
        if not member or member.bot:
            return

        # Correctly handle both custom and default emojis
        emoji_lookup_key = (payload.emoji.name, payload.emoji.id)

        role_id_to_add = config.EMOJI_ROLE_MAP.get(emoji_lookup_key)

        if role_id_to_add:
            role = guild.get_role(role_id_to_add)
            if role and role not in member.roles:
                try:
                    await member.add_roles(role)
                    logger.info(f"✅ Added role '{role.name}' to {member.display_name}")
                except discord.Forbidden:
                    logger.error(f"❌ Bot missing permissions to add role '{role.name}' to {member.display_name}.")
                except discord.HTTPException as e:
                    logger.error(f"❌ HTTP Error adding role '{role.name}' to {member.display_name}: {e}")

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload):
        """Handles reaction role removals."""
        # Ignore reactions not in the correct channel or on the correct message
        if payload.channel_id != config.VERIFY_CHANNEL_ID or payload.message_id != config.VERIFY_MESSAGE_ID:
            return

        guild = self.bot.get_guild(payload.guild_id)
        if not guild: return

        member = guild.get_member(payload.user_id)
        if not member or member.bot: return

        # Correctly handle both custom and default emojis
        emoji_lookup_key = (payload.emoji.name, payload.emoji.id)
        if payload.emoji.id is None:
            emoji_lookup_key = (str(payload.emoji), None)

        role_id_to_remove = config.EMOJI_ROLE_MAP.get(emoji_lookup_key)

        if role_id_to_remove:
            role = guild.get_role(role_id_to_remove)
            if member and role and role in member.roles:
                try:
                    await member.remove_roles(role)
                    logger.info(f"✅ Removed role '{role.name}' from {member.display_name}")
                except discord.Forbidden:
                    logger.error(f"❌ Bot missing permissions to remove role '{role.name}' from {member.display_name}.")
                except discord.HTTPException as e:
                    logger.error(f"❌ HTTP Error removing role '{role.name}' from {member.display_name}: {e}")


    # -------------------------A D M I N       U N I T-----------------------------------
    @commands.command(name='admin', help="(Admin Only) Displays the bot's point economy status as a premium embed.")
    @commands.has_any_role(config.ADMIN_ROLE_ID)
    async def admin(self, ctx):
        """
        (Admin Only) Displays the bot's point economy status as a premium embed.
        """
        await ctx.message.delete()

        economy_embed = await self.get_economy_embed()
        await ctx.send(embed=economy_embed)

    @commands.command(name="data",
                      help="(Admin Only) Displays all user data sorted by points, including referral count.")
    @commands.has_any_role(config.ADMIN_ROLE_ID)
    @commands.cooldown(1, 30, commands.BucketType.user)
    async def get_server_data(self, ctx):
        """
        (Admin Only) Displays all user data sorted by points, including referral count.
        """
        await ctx.message.delete()

        # ✅ STEP 1: Load both datasets asynchronously from the database
        users_points = await self.bot.load_all_json("user_points")
        referral_data = await self.bot.load_all_json("referral_data")

        # 1. Pre-process referral data for efficient lookup
        referral_counts = {}
        for user_id in referral_data.values():
            referral_counts[user_id] = referral_counts.get(user_id, 0) + 1

        # 2. Compile user data in a single list
        all_users_data = []
        for user_id, user_data in users_points.items():
            all_time_points = user_data.get("all_time_points", 0.0)
            referral_count = referral_counts.get(user_id, 0)
            all_users_data.append({
                "id": user_id,
                "points": all_time_points,
                "referrals": referral_count
            })

        all_users_data.sort(key=lambda x: x["points"], reverse=True)

        # 3. Build the embed
        embed = discord.Embed(
            title="📊 Server Economy Data",
            description="A list of all users, sorted by points.",
            color=discord.Color.dark_purple()
        )

        data_lines = []
        for idx, user_info in enumerate(all_users_data[:50], 1):
            # Use get_user() for fast cache lookup
            user = self.bot.get_user(int(user_info["id"]))
            username = user.display_name if user else f"Unknown User (ID: {user_info['id']})"

            points = user_info["points"]
            referrals = user_info["referrals"]
            data_lines.append(f"**#{idx}**: {username} - **{points:,.2f} MVpts** | Referrals: {referrals}")

        if data_lines:
            embed.add_field(name="User Rankings", value="\n".join(data_lines), inline=False)
        else:
            embed.add_field(name="User Rankings", value="No user data available.", inline=False)

        embed.set_footer(text="Data refreshes upon command.")
        embed.timestamp = datetime.now(UTC)
        await ctx.send(embed=embed)


# ----------------------R E F E R R A L-----------------U N I T-----------------------------------------
    # === MEMBER JOIN (REFERRAL) ===
    @commands.Cog.listener()
    async def on_member_join(self, member):
        """Event handler for member joins, primarily for referral tracking."""
        if member.bot:
            return

        # ✅ STEP 1: Load data from the database. We no longer rely on self.bot.referred_users as the source of truth.
        referred_users = set(await self.bot.load_list_values("referred_users", "user_id"))
        pending_referrals = await self.bot.load_all_json("pending_referrals")

        user_id = str(member.id)
        if user_id in referred_users:
            logger.info(f"User {member.name} has rejoined but has already been referred. Skipping referral check.")
            return

        guild = member.guild

        # Get the invites AFTER the user joined
        invites_after_join = await guild.invites()

        # Get the invites BEFORE the user joined from the cache
        invites_before_join = self.bot.invite_cache.get(guild.id, [])
        referrer = None

        # Use a more efficient dictionary-based lookup for comparison
        invites_before_dict = {invite.code: invite.uses for invite in invites_before_join}

        for invite_after in invites_after_join:
            uses_before = invites_before_dict.get(invite_after.code, 0)
            if invite_after.uses > uses_before:
                referrer = invite_after.inviter
                break

        # Important: Update the cache for the next time someone joins
        self.bot.invite_cache[guild.id] = invites_after_join

        if referrer and referrer.id != self.bot.user.id:
            pending_referrals[str(member.id)] = str(referrer.id)

            logger.info(f"New pending referral for {member.name}. Referrer: {referrer.name}")

            # ✅ STEP 2: Save the updated pending referrals list back to the database
            await self.bot.save_all_json("pending_referrals", self.pending_referrals)

            # ✅ FIX: Send the message to the referral channel instead of the user's DMs
            channel = self.bot.get_channel(config.REFERRAL_CHANNEL_ID)
            if channel:
                try:
                    embed = discord.Embed(
                        title="✨ New Referral!",
                        description=f"🎉 **{member.mention}** was just referred by {referrer.mention}!",
                        color=discord.Color.gold()
                    )
                    embed.set_footer(text="Awaiting verification. They'll receive their points soon!")
                    embed.timestamp = datetime.now(UTC)
                    await channel.send(embed=embed)
                except discord.Forbidden:
                    logger.error(f"❌ Bot missing permissions to send message to referral channel.")

    # === MEMBER UPDATE (REFERRAL) ===
    @commands.Cog.listener()
    async def on_member_update(self, before, after):
        """
        Handles member updates, primarily for referral rewards and
        role-based actions.
        """
        if after.bot:
            return

        # Check for new roles being added. If none, do nothing.
        new_roles = [r for r in after.roles if r not in before.roles]
        if not new_roles:
            return

        user_id = str(after.id)
        channel = self.bot.get_channel(config.REFERRAL_CHANNEL_ID)

        # ✅ STEP 1: Load all necessary data at the beginning of the function
        # This ensures that we have the most up-to-date information from the database
        # and a "transaction" can be completed safely.
        pending_referrals = await self.bot.load_all_json("pending_referrals")
        referred_users = set(await self.bot.load_list_values("referred_users", "user_id"))
        admin_points = await self.bot.load_single_json("admin_points", "main", {})
        users_points = await self.bot.load_all_json("users_points")
        referral_data = await self.bot.load_all_json("referral_data")

        # --- Welcome Message Logic for Newly 'Tivated' Users ---
        if config.TIVATED_ROLE_ID in [role.id for role in new_roles]:
            referrer_id = pending_referrals.get(user_id)
            if channel and referrer_id:
                try:
                    # Use fetch_user for a reliable API lookup, in case the user is not in the cache
                    referrer = await self.bot.fetch_user(int(referrer_id))
                    embed = discord.Embed(
                        title="👋 Welcome to ManaVerse!",
                        description=(
                            f"🎉 {after.mention} just joined the community!\n\n"
                            f"🙌 You were referred by {referrer.mention}.\n\n"
                            f"💡 **Reminder:** {referrer.mention} and {after.mention} will receive their referral reward "
                            f"once {after.mention} gets a **paid role** in the server.\n\n"
                            f"👉 To get started, check out <#{config.HOW_TO_JOIN_CHANNEL_ID}>."
                        ),
                        color=discord.Color.blue()
                    )
                    if after.avatar:
                        embed.set_thumbnail(url=after.avatar.url)
                    embed.set_footer(
                        text="ManaVerse Referral System – Building stronger connections 💎"
                    )
                    embed.timestamp = datetime.now(UTC)
                    await channel.send(embed=embed)
                    logger.info(f"Sent welcome message for referred user {after.name}.")
                except discord.NotFound:
                    await channel.send(f"🎉 Welcome {after.mention}!")
                    logger.warning(f"Referred user {after.name} joined, but referrer was not found.")
            elif channel:
                await channel.send(f"🎉 Welcome {after.mention}!")

        # --- CRUCIAL CHECKS BEFORE AWARDING POINTS ---
        if user_id not in pending_referrals:
            return

        if user_id in referred_users:
            logger.info(f"User {after.name} has already received a referral bonus. Skipping point award.")
            return

        # --- REFERRAL POINT AWARD LOGIC ---
        referrer_id = pending_referrals[user_id]

        for role in new_roles:
            if role.id in config.REFERRAL_POINTS_PER_ROLE:
                referrer_points = config.REFERRAL_POINTS_PER_ROLE[role.id]
                new_member_points = config.NEW_MEMBER_POINTS_PER_ROLE.get(role.id, 0.0)

                referrer_member = after.guild.get_member(int(referrer_id))
                if referrer_member and any(role.id == config.ADMIN_ROLE_ID for role in referrer_member.roles):
                    referrer_points = 0.0

                total_points_to_award = referrer_points + new_member_points

                # --- CRITICAL SAFETY CHECK ---
                if admin_points["balance"] < total_points_to_award:
                    logger.error(f"❌ Not enough points in admin balance to award referral.")
                    if channel:
                        await channel.send(
                            "❌ Referral reward could not be given due to insufficient points. Please notify admin.")
                    return

                # --- BEGIN TRANSACTION: The core logic now modifies the loaded data ---
                try:
                    # Update the dictionaries in memory.
                    users_points.setdefault(user_id, {"all_time_points": 0.0, "available_points": 0.0})
                    users_points[user_id]["all_time_points"] += new_member_points
                    users_points[user_id]["available_points"] += new_member_points

                    users_points.setdefault(referrer_id, {"all_time_points": 0.0, "available_points": 0.0})
                    users_points[referrer_id]["all_time_points"] += referrer_points
                    users_points[referrer_id]["available_points"] += referrer_points

                    admin_points["balance"] -= total_points_to_award
                    admin_points["in_circulation"] += total_points_to_award
                    referral_data[user_id] = referrer_id

                    await self.bot.save_all_json("users_points", users_points)
                    await self.bot.save_single_json("admin_points", "main", admin_points)
                    await self.bot.save_all_json("referral_data", self.referral_data)

                    # ✅ STEP 3: Now that data is saved, modify and save the other tables.
                    # We can now safely delete the user from pending and add it into referred.
                    del pending_referrals[user_id]
                    referred_users.add(user_id)
                    await self.bot.save_all_json("pending_referrals", self.pending_referrals)
                    await self.bot.save_list_values("referred_users", list(self.referred_users), "user_id")

                    # Log the transactions using the refactored helper function
                    if new_member_points > 0:
                        await self.bot.log_points_transaction(user_id, new_member_points,
                                                              f"Joined via referral by {referrer_member.display_name}")
                    if referrer_points > 0:
                        await self.bot.log_points_transaction(referrer_id, referrer_points,
                                                              f"Successful referral of {after.display_name}")

                    logger.info(f"Successful referral awarded to {referrer_member.display_name}.")

                    # Send the final successful referral embed
                    if channel:
                        embed = discord.Embed(
                            title="🎉 Successful Referral!",
                            description=(
                                f"🔥 {referrer_member.mention} just referred {after.mention}!\n\n"
                                f"💰 **Rewards Distributed:**\n"
                                f"• {referrer_member.mention} earned **{referrer_points:.2f} points** 🪙\n"
                                f"• {after.mention} earned **{new_member_points:.2f} points** 🎁"
                            ),
                            color=discord.Color.green()
                        )
                        if after.avatar:
                            embed.set_thumbnail(url=after.avatar.url)
                        embed.set_footer(
                            text="ManaVerse Referral System – Keep growing the community 🚀"
                        )
                        embed.timestamp = datetime.now(UTC)
                        await channel.send(embed=embed)

                    # Break the loop once the reward is given to avoid duplicate rewards
                    break

                except Exception as e:
                    # If an error occurs, log it. We no longer need to manually revert as the in-memory changes
                    # were never saved to the database.
                    logger.error(f"❌ An error occurred during point transaction: {e}", exc_info=True)
                    if channel:
                        await channel.send(
                            f"❌ An error occurred during point transaction. Please contact an admin.")
                    return

        # === Role Stripping Logic ===
        if before.roles == after.roles:
            return
        if after.bot:
            return
        verified_role = after.guild.get_role(config.TIVATED_ROLE_ID)
        if verified_role in before.roles and verified_role not in after.roles:
            try:
                roles_to_remove = [role for role in after.roles if role.id != after.guild.default_role.id]
                await after.remove_roles(*roles_to_remove, reason="Verified role was removed.")
                logger.info(f"Removed all roles from {after.name} because their verified role was removed.")
            except discord.Forbidden:
                logger.error(f"❌ Permission error: Bot could not remove roles from {after.name}.")


    # === INVITE LINK MECHANISM ===
    @commands.command(name="invite", help="Generates a unique referral link for the user.")
    async def invite_link(self, ctx):
        """Generates a unique referral link for the user."""
        # Delete the command message immediately
        await ctx.message.delete()

        # 1. Check if the command is in the correct channel
        if ctx.channel.id != config.REFERRAL_CHANNEL_ID:
            await ctx.send(
                f"❌ The `!invite` command can only be used in the <#{config.REFERRAL_CHANNEL_ID}> channel.",
                delete_after=10)
            return

        # 2. Search for an existing invite link
        existing_invites = await ctx.guild.invites()
        user_invite = next(
            (invite for invite in existing_invites if invite.inviter == ctx.author and invite.max_uses == 0), None
        )

        if user_invite:
            await ctx.send(
                f"🔗 Here is your personal referral link, {ctx.author.mention}: `{user_invite.url}`\n"
                "Share this link with friends to earn bonus points when they join!",
                delete_after=45
            )
        else:
            # 3. Create a new, permanent invite link
            invite = await ctx.channel.create_invite(
                max_uses=0, max_age=0, reason="Referral link for a user"
            )
            await ctx.send(
                f"🔗 Here is your personal referral link, {ctx.author.mention}: `{invite.url}`\n"
                "Share this link with friends to earn bonus points when they join!",
                delete_after=30
            )

    @commands.command(name="ref", help="Shows the user a list of people they have successfully referred.")
    @commands.cooldown(2, 60, commands.BucketType.user)
    async def ref_command(self, ctx):
        """Shows the user a list of people they have successfully referred."""
        # Delete the user's command message
        await ctx.message.delete()

        # 1. Check if the command is in the correct channel
        if ctx.channel.id != config.REFERRAL_CHANNEL_ID:
            await ctx.send(f"❌ This command can only be used in the <#{config.REFERRAL_CHANNEL_ID}> channel.",
                           delete_after=10)
            return

        # ✅ FIX: Load the referral data from the database
        referral_data = await self.bot.load_all_json("referral_data")
        referrer_id = str(ctx.author.id)
        referred_members = [user_id for user_id, ref_id in referral_data.items() if ref_id == referrer_id]

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
            embed.description = "You have not referred anyone yet. Share your invite link to get started!"
        else:
            embed.description = "Here is a list of members you have successfully referred:"
            referral_list = []
            for referred_id in referred_members:
                # 2. Use get_user for a fast cache lookup
                user = self.bot.get_user(int(referred_id))
                if user:
                    referral_list.append(f"• {user.mention} ({user.display_name})")
                else:
                    referral_list.append(f"• Unknown User (ID: {referred_id})")

            embed.add_field(name="Referred Users", value="\n".join(referral_list), inline=False)

        await ctx.send(embed=embed, delete_after=60)

    # ----------------------P R O O F       O F       T A S K---------------------------------------------
    @commands.command(name="proof",
                      help="Allows users to submit proof of engagements for points, applying role multipliers.")
    async def proof(self, ctx, tweet_url: str, *engagements):
        """Allows users to submit proof of engagements for points, applying role multipliers."""
        # Delete the command message immediately
        await ctx.message.delete()

        # 1. Check for the correct channel
        if ctx.channel.id != config.TASK_SUBMIT_CHANNEL_ID:
            error_embed = discord.Embed(
                title="❌ Incorrect Channel",
                description=f"This command can only be used in the <#{config.TASK_SUBMIT_CHANNEL_ID}> channel.",
                color=discord.Color.red()
            )
            await ctx.send(embed=error_embed, delete_after=15)
            return

        user_id = str(ctx.author.id)
        all_proof_urls = [normalize_url(tweet_url)]
        all_proof_urls.extend([normalize_url(att.url) for att in ctx.message.attachments if
                               att.content_type and att.content_type.startswith('image/')])

        # ✅ FIX: Load the submissions and approved proofs from the database
        approved_proofs = await self.bot.load_list_values("approved_proofs", "normalized_url")
        submissions = await self.bot.load_list_of_json("submissions")

        # 2. Validate Proofs
        if not all_proof_urls:
            embed = discord.Embed(title="🚫 Submission Failed",
                                  description=f"{ctx.author.mention}, please provide a tweet URL and/or attach an image to your command.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=15)
            return

        if any(url in approved_proofs for url in all_proof_urls):
            embed = discord.Embed(title="🚫 Duplicate Submission",
                                  description=f"{ctx.author.mention}, your submission was removed! One or more of the proofs has already been submitted and approved. Please ensure all proofs are unique.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=25)
            return

        # 3. Validate Engagements
        valid_engagements = [e.lower() for e in engagements if e.lower() in config.POINT_VALUES]
        if not valid_engagements:
            embed = discord.Embed(title="🚫 Submission Failed",
                                  description=f"{ctx.author.mention}, please specify valid engagement types: `like`, `comment`, `retweet`.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=15)
            return

        # 4. Check for Pending Submissions
        if user_id in submissions:
            embed = discord.Embed(title="⏳ Pending Submission",
                                  description=f"{ctx.author.mention}, you already have a pending submission.",
                                  color=discord.Color.orange())
            await ctx.send(embed=embed, delete_after=15)
            return

        # 5. Calculate Points and Store Submission
        base_points = sum(config.POINT_VALUES[e] for e in valid_engagements)
        multiplier = max((config.ROLE_MULTIPLIERS.get(role.id, 1.0) for role in ctx.author.roles), default=1.0)
        final_points = round(base_points * multiplier, 2)

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

        # ✅ FIX: Save the updated submissions dictionary immediately
        await self.bot.save_all_json("submissions", self.submissions)

        # 6. Notify Moderators
        mod_channel = self.bot.get_channel(config.MOD_TASK_REVIEW_CHANNEL_ID)
        if mod_channel:
            mod_notification_embed = discord.Embed(title="🔍 New Submission for Review",
                                                   description=f"User: {ctx.author.mention}\nAccount: {ctx.author.name}",
                                                   color=discord.Color.blue(), url=tweet_url)
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

        # 7. Send Success Message to User
        success_embed = discord.Embed(title="✅ Submission Logged!",
                                      description=f"{ctx.author.mention}, your submission has been sent for review.",
                                      color=discord.Color.green())
        success_embed.add_field(name="Points Requested", value=f"**{final_points}**", inline=True)
        success_embed.add_field(name="Your Engagements", value=f"**{', '.join(valid_engagements)}**", inline=True)
        success_embed.set_footer(text="Please be patient while a moderator reviews your proof.")
        success_embed.set_thumbnail(url=ctx.author.avatar.url if ctx.author.avatar else None)
        await ctx.send(embed=success_embed, delete_after=20)

    @commands.command(name='verify', help="(Moderator Only) Approves or rejects a user's task submission.")
    @commands.has_permissions(manage_messages=True)
    async def verify(self, ctx, member: discord.Member, action: str):
        """(Moderator Only) Approves or rejects a user's task submission."""
        # Delete the command message immediately
        await ctx.message.delete()

        # 1. Check for the correct channel
        if ctx.channel.id != config.MOD_TASK_REVIEW_CHANNEL_ID:
            error_embed = discord.Embed(
                title="❌ Incorrect Channel",
                description=f"This command can only be used in the <#{config.MOD_TASK_REVIEW_CHANNEL_ID}> channel.",
                color=discord.Color.red()
            )
            await ctx.send(embed=error_embed, delete_after=15)
            return

        # ✅ FIX: Load all necessary data from the database at the start
        submissions = await self.bot.load_list_of_json("submissions")
        admin_points = await self.bot.load_single_json("admin_points", "main", {})
        users_points = await self.bot.load_all_json("users_points")
        approved_proofs = await self.bot.load_list_of_json("approved_proofs")

        user_id = str(member.id)
        action = action.lower()

        # 2. Validate the submission
        if user_id not in submissions:
            no_submission_embed = discord.Embed(title="❌ Error",
                                                description="No pending submission found for this user.",
                                                color=discord.Color.red())
            await ctx.send(embed=no_submission_embed, delete_after=10)
            return

        submission = submissions[user_id]
        reply_channel = self.bot.get_channel(submission.get("channel_id", config.TASK_SUBMIT_CHANNEL_ID))

        if not reply_channel:
            logger.warning(f"Could not find reply channel for user {user_id}. Falling back to command channel.")
            reply_channel = ctx.channel

        if action == "approve":
            points_to_award = submission["points_requested"]

            # 3. Critical Safety Check
            if admin_points["balance"] < points_to_award:
                balance_embed = discord.Embed(title="❌ Approval Failed",
                                              description=f"Admin balance is too low to award **{points_to_award:.2f}** points.",
                                              color=discord.Color.red())
                await ctx.send(embed=balance_embed, delete_after=10)
                return

            # 4. Process the approval transaction on the loaded data
            users_points.setdefault(user_id, {"all_time_points": 0.0, "available_points": 0.0})
            users_points[user_id]["all_time_points"] += points_to_award
            users_points[user_id]["available_points"] += points_to_award

            admin_points["balance"] -= points_to_award
            admin_points["in_circulation"] += points_to_award

            for url in submission.get("normalized_proof_urls", []):
                if url not in approved_proofs:
                    approved_proofs.append(url)

            # ✅ FIX: Save the updated data immediately after the transaction
            await self.bot.save_all_json("users_points", users_points)
            await self.bot.save_single_json("admin_points", "main", admin_points)
            await self.bot.save_list_of_json("approved_proofs", approved_proofs)

            # 5. Log the transaction and clear the submission from the loaded data
            await self.bot.log_points_transaction(user_id, points_to_award, "Task submission approved")
            del submissions[user_id]

            # ✅ FIX: Save the updated submissions dictionary
            await self.bot.save_list_of_json("submissions", submissions)

            user_embed = discord.Embed(title="✅ Submission Approved!",
                                       description=f"Your engagement proof has been approved. You earned **{points_to_award:.2f} points**!",
                                       color=discord.Color.green())
            user_embed.add_field(name="Your New Total",
                                 value=f"**{users_points[user_id]['available_points']:.2f} points**",
                                 inline=False)
            user_embed.set_footer(text="Thank you for your contribution!")
            await reply_channel.send(f"{member.mention}", embed=user_embed)

            mod_embed = discord.Embed(title="✅ Action Logged",
                                      description=f"**Approved** submission for {member.mention}.",
                                      color=discord.Color.green())
            mod_embed.add_field(name="Points Awarded", value=f"**{points_to_award:.2f}**", inline=True)
            mod_embed.set_footer(text=f"Action by {ctx.author.name}")
            await ctx.send(embed=mod_embed, delete_after=15)

        elif action == "reject":
            # ✅ FIX: Load the submissions dictionary, modify it, and save it.
            del submissions[user_id]
            await self.bot.save_data(self.bot, "submissions", submissions)

            user_embed = discord.Embed(title="🚫 Submission Rejected",
                                       description="Your engagement proof has been rejected. Please review your proof and submit again if needed.",
                                       color=discord.Color.red())
            await reply_channel.send(f"{member.mention}", embed=user_embed)

            mod_embed = discord.Embed(title="✅ Action Logged",
                                      description=f"**Rejected** submission for {member.mention}.",
                                      color=discord.Color.red())
            mod_embed.set_footer(text=f"Action by {ctx.author.name}")
            await ctx.send(embed=mod_embed, delete_after=15)

        else:
            invalid_embed = discord.Embed(title="❌ Invalid Action", description="Please use `approve` or `reject`.",
                                          color=discord.Color.red())
            await ctx.send(embed=invalid_embed, delete_after=10)


    # === ! A P P R O V E      P A Y M E N T ===
    @commands.command(name='approve_payment',
                      help="Moderator command to approve a payment and assign a role based on amount.")
    @commands.has_permissions(manage_roles=True)
    async def approve_payment(self, ctx, member: discord.Member, amount: int):
        """Moderator command to approve a payment and assign a role based on amount."""
        # Delete the user's command message
        await ctx.message.delete()

        # 1. Check for the correct channel
        if ctx.channel.id != config.MOD_PAYMENT_REVIEW_CHANNEL_ID:
            error_embed = discord.Embed(
                title="❌ Incorrect Channel",
                description=f"This command can only be used in the <#{config.MOD_PAYMENT_REVIEW_CHANNEL_ID}> channel.",
                color=discord.Color.red()
            )
            await ctx.send(embed=error_embed, delete_after=15)
            return

        # 2. Map the amount to the correct role
        role_map = {
            10: (config.ROOKIE_ROLE_ID, "Odogwu Rookie"),
            15: (config.ELITE_ROLE_ID, "Odogwu Elite"),
            20: (config.SUPREME_ROLE_ID, "Odogwu Supreme"),
            50: (config.VIP_ROLE_ID, "1st Circle (VIP)")
        }

        if amount not in role_map:
            invalid_amount_embed = discord.Embed(title="❌ Invalid Amount",
                                                 description="The amount must be one of the following: **10, 15, 20, or 50**.",
                                                 color=discord.Color.red())
            await ctx.send(embed=invalid_amount_embed, delete_after=10)
            return

        # 3. Get the role and validate
        role_id, role_name = role_map[amount]
        role = ctx.guild.get_role(role_id)
        if not role:
            role_not_found_embed = discord.Embed(title="❌ Role Not Found",
                                                 description=f"A role with the ID `{role_id}` could not be found.",
                                                 color=discord.Color.red())
            await ctx.send(embed=role_not_found_embed, delete_after=15)
            logger.error(f"Error❌: Role ID {role_id} for amount {amount} not found in guild.")
            return

        if role in member.roles:
            already_has_role_embed = discord.Embed(title="⚠️ Role Already Assigned",
                                                   description=f"{member.mention} already has the **{role_name}** role.",
                                                   color=discord.Color.orange())
            await ctx.send(embed=already_has_role_embed, delete_after=10)
            return

        try:
            # 4. Add the role to the user
            await member.add_roles(role)

            # 5. Send confirmation messages
            confirm_channel = self.bot.get_channel(config.PAYMENT_CHANNEL_ID)
            if confirm_channel:
                try:
                    user_embed = discord.Embed(title="🎉 Payment Confirmed!",
                                               description=f"Your payment has been confirmed and you’ve been assigned the **{role_name}** role!",
                                               color=discord.Color.green())
                    user_embed.set_footer(text="Thank you for your support!")
                    await confirm_channel.send(member.mention, embed=user_embed)
                except discord.Forbidden:
                    logger.error(f"Bot is missing permissions to send message to payment channel ({config.PAYMENT_CHANNEL_ID}).")
            else:
                logger.warning(f"Payment confirmation channel (ID: {config.PAYMENT_CHANNEL_ID}) not found.")

            # Send a confirmation message to the moderator
            mod_confirm_embed = discord.Embed(title="✅ Payment Approved",
                                              description=f"Successfully assigned **{role_name}** to {member.mention}.",
                                              color=discord.Color.green())
            mod_confirm_embed.add_field(name="Amount", value=f"${amount}", inline=True)
            mod_confirm_embed.add_field(name="User", value=member.mention, inline=True)
            mod_confirm_embed.set_footer(text=f"Action by {ctx.author.name}")
            await ctx.send(embed=mod_confirm_embed)

        except discord.Forbidden:
            forbidden_embed = discord.Embed(
                title="❌ Bot Permissions Error",
                description=f"The bot does not have permissions to add the **{role_name}** role.",
                color=discord.Color.red()
            )
            await ctx.send(embed=forbidden_embed, delete_after=10)
            logger.error(f"Bot missing permissions to add role '{role.name}' to {member.display_name}.")
        except discord.HTTPException as e:
            http_error_embed = discord.Embed(
                title="❌ An Error Occurred",
                description=f"An error occurred while adding the role: `{e}`",
                color=discord.Color.red()
            )
            await ctx.send(embed=http_error_embed, delete_after=10)
            logger.error(f"HTTP Error adding role '{role.name}' to {member.display_name}: {e}")


    # === CONSOLIDATED: !points command to show all-time points, available points, and rank ===
    @commands.command(name='points', help="Displays the points of a specific member or the user who ran the command.")
    async def points(self, ctx, member: discord.Member = None):
        """Displays the points of a specific member or the user who ran the command."""
        await ctx.message.delete()

        if ctx.channel.id != config.LEADERBOARD_CHANNEL_ID:
            error_embed = discord.Embed(title="❌ Incorrect Channel",
                                        description=f"This command can only be used in the <#{config.LEADERBOARD_CHANNEL_ID}> channel.",
                                        color=discord.Color.red())
            await ctx.send(embed=error_embed, delete_after=10)
            return

        # ✅ FIX: Load the user points table from the database
        users_points = await self.bot.load_all_json("users_points")

        target_member = member if member else ctx.author
        user_id = str(target_member.id)
        user_data = users_points.get(user_id, {"all_time_points": 0.0, "available_points": 0.0})
        all_time_points = user_data.get("all_time_points", 0.0)
        available_points = user_data.get("available_points", 0.0)

        # 2. Calculate the user's rank
        sorted_users = sorted(
            users_points.items(),
            key=lambda item: item[1].get('all_time_points', 0),
            reverse=True
        )

        rank = "Unranked"
        for i, (uid, _) in enumerate(sorted_users, 1):
            if uid == user_id:
                rank = f"#{i}"
                break

        # 3. Build and send the embed
        usd_value = available_points * config.POINTS_TO_USD
        embed = discord.Embed(title="💰 Points & Rank",
                              description=f"Here is the points summary for {target_member.mention}.",
                              color=discord.Color.gold())
        embed.add_field(name="All-Time Points", value=f"**{all_time_points:.2f}**", inline=True)
        embed.add_field(name="Available Points", value=f"**{available_points:.2f}**", inline=True)
        embed.add_field(name="Est. USD Value", value=f"**${usd_value:.2f}**", inline=True)
        embed.add_field(name="Current Rank", value=f"**{rank}**", inline=True)
        embed.set_thumbnail(url=target_member.avatar.url if target_member.avatar else target_member.default_avatar.url)
        embed.set_footer(text="Points are earned through tasks and engagements. 🚀")
        embed.timestamp = datetime.now(UTC)

        await ctx.send(embed=embed, delete_after=30)

    # === MANUALLY ADD POINTS ===
    @commands.command(name="addpoints",
                      help="(Admin Only) Manually adds a specified number of points to one or more users.")
    @commands.has_any_role(config.ADMIN_ROLE_ID)
    async def addpoints(self, ctx, members: commands.Greedy[discord.Member], points_to_add: float, *,
                        purpose: str = "Giveaway winner"):
        """(Admin Only) Manually adds a specified number of points to one or more users."""
        await ctx.message.delete()

        if ctx.channel.id != config.GIVEAWAY_CHANNEL_ID:
            embed = discord.Embed(title="❌ Incorrect Channel",
                                  description=f"This command can only be used in the <#{config.GIVEAWAY_CHANNEL_ID}> channel.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=10)
            return
        if not members:
            embed = discord.Embed(title="❌ Missing Members",
                                  description="You must mention at least one member to add points to.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=10)
            return
        if points_to_add <= 0:
            embed = discord.Embed(title="❌ Invalid Points", description="Points to add must be greater than zero.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=10)
            return

        # ✅ FIX: Load data from the database before modifying it
        admin_points = await self.bot.load_single_json("admin_points", "main", {})
        users_points = await self.bot.load_all_json("users_points")
        giveaway_winners_log = await self.bot.load_list_of_json("giveaway_logs")
        all_time_giveaway_winners_log = await self.bot.load_list_of_json("all_time_giveaway_logs")

        total_points = points_to_add * len(members)
        if admin_points.get("balance", 0) < total_points:
            embed = discord.Embed(title="❌ Insufficient Balance",
                                  description=f"Admin balance is too low to award a total of **{total_points:.2f} points**.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=10)
            return

        # Process the transaction on the loaded data
        winners_list = []
        for member in members:
            user_id = str(member.id)
            users_points.setdefault(user_id, {"all_time_points": 0.0, "available_points": 0.0})
            users_points[user_id]["all_time_points"] += points_to_add
            users_points[user_id]["available_points"] += points_to_add
            await self.bot.log_points_transaction(user_id, points_to_add, purpose)
            winner_entry = {"user_id": user_id, "points": points_to_add, "purpose": purpose,
                            "timestamp": datetime.now(UTC).isoformat()}
            giveaway_winners_log.append(winner_entry)
            all_time_giveaway_winners_log.append(winner_entry)
            winners_list.append(member.mention)

        admin_points["balance"] -= total_points
        admin_points["in_circulation"] += total_points

        # ✅ FIX: Save all the updated data back to the database
        await self.bot.save_all_json("users_points", users_points)
        await self.bot.save_single_json("admin_points", "main", admin_points)
        await self.bot.save_list_of_json("giveaway_logs", self.giveaway_winners_log)
        await self.bot.save_list_of_json("all_time_giveaway_logs", self.all_time_giveaway_winners_log)

        embed = discord.Embed(title="🎉 Points Awarded!", description=f"The following user(s) have been awarded points:",
                              color=discord.Color.gold())
        embed.add_field(name="User(s)", value=', '.join(winners_list), inline=False)
        embed.add_field(name="Points per User", value=f"**{points_to_add:.2f}**", inline=True)
        embed.add_field(name="Total Points Awarded", value=f"**{total_points:.2f}**", inline=True)
        embed.add_field(name="Purpose", value=purpose, inline=False)
        embed.set_footer(text=f"Action by {ctx.author.name}")
        embed.timestamp = datetime.now(UTC)
        await ctx.send(embed=embed)


    @commands.command(name='addpoints_flex',
                      help="(Admin Only) Manually adds different point amounts to multiple users.")
    @commands.has_any_role(config.ADMIN_ROLE_ID)
    async def addpoints_flex(self, ctx, *args):
        """(Admin Only) Manually adds different point amounts to multiple users."""
        # Delete the command message immediately
        await ctx.message.delete()

        # 1. Check for the correct channel
        if ctx.channel.id != config.GIVEAWAY_CHANNEL_ID:
            embed = discord.Embed(title="❌ Incorrect Channel",
                                  description=f"This command can only be used in the <#{config.GIVEAWAY_CHANNEL_ID}> channel.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=10)
            return

        # 2. Parse the arguments
        if not args:
            await ctx.send("❌ Error: Please provide at least one user and point pair.", delete_after=20)
            return

        points_to_award = {}
        purpose = "Manual addition"

        try:
            float(args[-1])
            # If it's a number, the purpose is the default, and we proceed with the whole args tuple
        except ValueError:
            purpose = args[-1]
            args = args[:-1]

        # Iterate through pairs of user mentions and points
        i = 0
        while i < len(args):
            try:
                member = await commands.MemberConverter().convert(ctx, args[i])
                points = float(args[i + 1])
                if points <= 0:
                    await ctx.send("❌ Error: Points must be greater than zero.", delete_after=20)
                    return
                points_to_award[member] = points
                i += 2
            except (commands.BadArgument, ValueError):
                # If parsing fails, something is wrong with the arguments.
                await ctx.send("❌ Error: Could not find any valid user and point pairs.", delete_after=20)
                return

        if not points_to_award:
            await ctx.send("❌ Error: Could not find any valid user and point pairs.", delete_after=20)
            return

        # 3. Validate the transaction
        total_points = sum(points_to_award.values())
        if self.bot.admin_points.get("balance", 0) < total_points:
            await ctx.send(f"❌ Error: Admin balance is too low to award a total of {total_points:.2f} points.",
                           delete_after=20)
            return

        # 4. Process the transaction in memory
        winners_list = []
        for member, points in points_to_award.items():
            user_id = str(member.id)

            self.bot.users_points.setdefault(user_id, {"all_time_points": 0.0, "available_points": 0.0})
            self.bot.users_points[user_id]["all_time_points"] += points
            self.bot.users_points[user_id]["available_points"] += points
            await self.bot.save_all_json("users_points", self.users_points)

            await self.bot.log_points_transaction(user_id, points, purpose)

            winner_entry = {"user_id": user_id, "points": points, "purpose": purpose,
                            "timestamp": datetime.now(UTC).isoformat()}
            self.bot.giveaway_winners_log.append(winner_entry)
            self.bot.all_time_giveaway_winners_log.append(winner_entry)

            winners_list.append(f"{member.mention} ({points:.2f})")

        self.bot.admin_points["balance"] -= total_points
        self.bot.admin_points["in_circulation"] += total_points

        await self.bot.save_single_json("admin_points", "main", self.admin_points)


        # 5. Send a confirmation embed
        embed = discord.Embed(title="🎉 Points Awarded!",
                              description=f"The following user(s) have been awarded points:",
                              color=discord.Color.gold())
        embed.add_field(name="Winners", value='\n'.join(winners_list), inline=False)
        embed.add_field(name="Purpose", value=purpose, inline=False)
        embed.set_footer(text=f"Action by {ctx.author.name}")
        embed.timestamp = datetime.now(UTC)
        await ctx.send(embed=embed, delete_after=86400)

    # -------------------------------- R A N K I N G --- S Y S T E M ---------------------------------------

    @commands.command(name="rank", help="Shows the user's rank and the top 10 leaderboards.")
    @commands.cooldown(1, 60, commands.BucketType.user)
    async def rank(self, ctx):
        """Shows the user's rank and the top 10 leaderboards."""
        await ctx.message.delete()
        if ctx.channel.id != config.LEADERBOARD_CHANNEL_ID:
            error_embed = discord.Embed(title="❌ Incorrect Channel",
                                        description=f"This command can only be used in the <#{config.LEADERBOARD_CHANNEL_ID}> channel.",
                                        color=discord.Color.red())
            await ctx.send(embed=error_embed, delete_after=10)
            return

        # ✅ FIX: Load the user points from the database
        users_points = await self.bot.load_all_json("users_points")

        user_id = str(ctx.author.id)
        user_data = users_points.get(user_id, {"all_time_points": 0.0})
        user_score = user_data.get("all_time_points", 0.0)

        # Filter and sort the eligible users
        eligible_users = {}
        for uid, data in users_points.items():
            member = ctx.guild.get_member(int(uid))
            if member and not any(
                    role.id in [config.ADMIN_ROLE_ID, config.MOD_ROLE_ID] for role in member.roles) and data.get(
                'all_time_points', 0) > 0:
                eligible_users[uid] = data

        if not eligible_users:
            await ctx.send("The leaderboard is currently empty. Start earning points!", delete_after=20)
            return

        sorted_users = sorted(eligible_users.items(), key=lambda item: item[1].get('all_time_points', 0.0),
                              reverse=True)
        rank_position = next((i for i, (uid, _) in enumerate(sorted_users, start=1) if uid == user_id), None)

        # Build and send the embed
        embed = discord.Embed(title="🏆 ManaVerse Global Rankings",
                              description=f"Your progress and the **Top 10 Legends** of {ctx.guild.name}.",
                              color=discord.Color.gold())
        if rank_position:
            embed.add_field(name="👑 Your Rank", value=f"**#{rank_position}** with **{user_score:.2f} points**",
                            inline=False)
        else:
            embed.add_field(name="👑 Your Rank", value="You are not ranked yet. Start earning points!", inline=False)

        medals = ["🥇", "🥈", "🥉"]
        leaderboard_text = ""
        for i, (uid, data) in enumerate(sorted_users[:10]):
            member = ctx.guild.get_member(int(uid))
            username = member.name if member else f"Unknown User ({uid})"
            if uid == user_id:
                username = f"⭐ **{username}** ⭐"
            medal = medals[i] if i < len(medals) else "🏅"
            leaderboard_text += f"{medal} **#{i + 1} – {username}**: {data['all_time_points']:.2f} MVpts\n"

        embed.add_field(name="🌟 Top 10 Mana Legends", value=leaderboard_text, inline=False)
        embed.set_footer(text="Grind, engage, and claim your spot at the top!",
                         icon_url=ctx.guild.icon.url if ctx.guild.icon else None)
        embed.timestamp = datetime.now(UTC)
        await ctx.send(embed=embed)

    @commands.command(name="leaderboard",
                      help="Displays the top 10 users by all-time points in a premium embed format.")
    @commands.cooldown(1, 60, commands.BucketType.user)
    async def leaderboard(self, ctx):
        """Displays the top 10 users by all-time points in a premium embed format."""
        # Delete the user's command message
        await ctx.message.delete()

        # 1. Check for the correct channel
        if ctx.channel.id != config.LEADERBOARD_CHANNEL_ID:
            error_embed = discord.Embed(title="❌ Incorrect Channel",
                                        description=f"This command can only be used in the <#{config.LEADERBOARD_CHANNEL_ID}> channel.",
                                        color=discord.Color.red())
            await ctx.send(embed=error_embed, delete_after=10)
            return

        # ✅ FIX: Load the user points from the database
        users_points = await self.bot.load_all_json("users_points")

        # 2. Filter and sort the eligible users
        eligible_users = {}
        for user_id, data in users_points.items():
            member = ctx.guild.get_member(int(user_id))
            if member and not any(
                    role.id in [config.ADMIN_ROLE_ID, config.MOD_ROLE_ID] for role in member.roles) and data[
                'all_time_points'] > 0:
                eligible_users[user_id] = data

        if not eligible_users:
            await ctx.send("The leaderboard is currently empty. Start earning points!", delete_after=20)
            return

        sorted_users = sorted(eligible_users.items(), key=lambda item: item[1]['all_time_points'], reverse=True)

        # 3. Build and send the embed
        embed = discord.Embed(title="🏆 ManaVerse Leaderboard 🏆",
                              description="The **Top 10 Legends** ranked by all-time points.",
                              color=discord.Color.gold())
        medals = ["🥇", "🥈", "🥉"]
        ribbons = ["🎗️"] * 7
        for i, (user_id, data) in enumerate(sorted_users[:10]):
            member = ctx.guild.get_member(int(user_id))
            username = member.display_name if member else f"User ID: {user_id}"
            all_time_points = data['all_time_points']
            rank_symbol = medals[i] if i < 3 else f"{ribbons[0]} #{i + 1}"
            embed.add_field(name=f"{rank_symbol} {username}", value=f"**{all_time_points:.2f} MVpts**", inline=False)

        embed.set_footer(text="Climb the ranks by earning points and show your dominance! 🚀")
        embed.timestamp = datetime.now(UTC)
        await ctx.send(embed=embed)

    # === MODIFIED: !requestpayout command with minimum amount and fee ===
    @commands.command(name="requestpayout", help="Initiates a two-step payout request, requiring confirmation.")
    async def requestpayout(self, ctx, amount: float, uid: str, exchange: str):
        """Initiates a two-step payout request, requiring confirmation."""
        await ctx.message.delete()
        if ctx.channel.id != config.PAYOUT_REQUEST_CHANNEL_ID: return

        # 2. Validate input
        if not all([amount, uid, exchange]):
            embed = discord.Embed(title="❌ Missing Information",
                                  description="Please use the correct format: `!requestpayout <Amount> <UID> <Exchange Name>`\n\n**Example:** `!requestpayout 5000 509958013 Binance`",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=15)
            return

        if not uid.isdigit():
            embed = discord.Embed(title="❌ Invalid UID", description="Only numeric exchange UIDs are accepted.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=10)
            return

        exchange = exchange.lower()
        if exchange not in config.APPROVED_EXCHANGES:
            approved_list = ", ".join([e.capitalize() for e in config.APPROVED_EXCHANGES])
            embed = discord.Embed(title="❌ Invalid Exchange",
                                  description=f"Only these exchanges are accepted: **{approved_list}**",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=10)
            return

        # 3. Validate user balance
        users_points = await self.bot.load_all_json("users_points")
        user_id = str(ctx.author.id)
        user_data = users_points.get(user_id, {"all_time_points": 0.0, "available_points": 0.0})
        balance = user_data.get("available_points", 0.0)

        if amount < config.MIN_PAYOUT_AMOUNT:
            embed = discord.Embed(title="⚠️ Payout Amount Too Low",
                                  description=f"The minimum payout amount is **{config.MIN_PAYOUT_AMOUNT:.2f} points**.",
                                  color=discord.Color.orange())
            await ctx.send(f"{ctx.author.mention}", embed=embed, delete_after=10)
            return

        fee = amount * (config.PAYOUT_FEE_PERCENTAGE / 100)
        total_deduction = amount + fee

        if balance < total_deduction:
            embed = discord.Embed(title="⚠️ Insufficient Points",
                                  description=f"You do not have enough available points for this request. Your current available balance is **{balance:.2f} points**.",
                                  color=discord.Color.orange())
            await ctx.send(f"{ctx.author.mention}", embed=embed, delete_after=10)
            return

        # 4. Store the pending payout data in memory
        user_data["pending_payout"] = {
            "amount": amount, "uid": uid, "exchange": exchange, "fee": fee, "total_deduction": total_deduction,
            "timestamp": time.time()
        }
        users_points[user_id] = user_data

        # 5. Send confirmation embed for the two-step process
        embed = discord.Embed(title="🪙 Payout Request Confirmation",
                              description=f"You are about to request a payout. Please review the details below:",
                              color=discord.Color.gold(), timestamp=datetime.now(UTC))
        embed.add_field(name="Requested Amount", value=f"**{amount:.2f} points**", inline=False)
        embed.add_field(name="Exchange", value=f"**{exchange.capitalize()}**", inline=True)
        embed.add_field(name="UID", value=f"**{uid}**", inline=True)
        embed.add_field(name="Fee", value=f"**{config.PAYOUT_FEE_PERCENTAGE:.1f}% ({fee:.2f} points)**", inline=False)
        embed.add_field(name="Total Deduction", value=f"**{total_deduction:.2f} points**", inline=False)
        embed.set_footer(
            text=f"Please type `!confirmpayout` to finalize the request within {config.CONFIRMATION_TIMEOUT} seconds.")
        await ctx.send(f"{ctx.author.mention}", embed=embed)

    # === !CONFIRM-PAYOUT ===
    @commands.command(name="confirmpayout",
                      help="Confirms a pending payout request and deducts points from the user's balance.")
    @commands.cooldown(1, 30, commands.BucketType.user)
    async def confirmpayout(self, ctx):
        """Confirms a pending payout request and deducts points from the user's balance."""
        # Delete the command message immediately
        await ctx.message.delete()

        # 1. Check for the correct channel
        if ctx.channel.id != config.PAYOUT_REQUEST_CHANNEL_ID:
            embed = discord.Embed(title="❌ Incorrect Channel",
                                  description=f"This command can only be used in the <#{config.PAYOUT_REQUEST_CHANNEL_ID}> channel.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=10)
            return

        # ✅ FIX: Load the user points from the database
        users_points = await self.bot.load_all_json("users_points")

        user_id = str(ctx.author.id)
        user_data = users_points.get(user_id, {})
        pending_payout = user_data.get("pending_payout")

        # 2. Validate the pending request
        if not pending_payout:
            embed = discord.Embed(title="❌ No Request Found",
                                  description="No pending payout request found. Use `!requestpayout` first.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=10)
            return

        if time.time() - pending_payout["timestamp"] > config.CONFIRMATION_TIMEOUT:
            if "pending_payout" in user_data:
                del user_data["pending_payout"]
                users_points[user_id] = user_data

                # ✅ FIX: Save the updated users_points immediately if the request timed out
                await self.bot.save_all_json("users_points", users_points)

            embed = discord.Embed(title="❌ Request Timed Out",
                                  description="Your payout request timed out. Please start a new request with `!requestpayout`.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=10)
            return

        total_deduction = pending_payout["total_deduction"]
        balance = user_data.get("available_points", 0.0)
        if balance < total_deduction:
            embed = discord.Embed(title="❌ Insufficient Balance",
                                  description="You no longer meet the minimum balance for payout.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=10)
            return

        # 3. Process the transaction on the loaded data
        user_data["available_points"] -= total_deduction

        # ✅ FIX: Save the updated user points dictionary after the point deduction
        await self.bot.save_all_json("users_points", users_points)

        # 4. Notify the user and moderators
        mod_channel = self.bot.get_channel(config.MOD_PAYMENT_REVIEW_CHANNEL_ID)
        if mod_channel:
            mod_embed = discord.Embed(title="📤 New Payout Request",
                                      description="A new payout request has been submitted for review.",
                                      color=discord.Color.blue(), timestamp=datetime.now(UTC))
            mod_embed.add_field(name="User", value=f"{ctx.author.mention} (`{ctx.author.name}`)", inline=False)
            mod_embed.add_field(name="UID", value=f"**`{pending_payout['uid']}`**", inline=True)
            mod_embed.add_field(name="Exchange", value=f"**`{pending_payout['exchange'].capitalize()}`**", inline=True)
            mod_embed.add_field(name="Requested Amount", value=f"**{pending_payout['amount']:.2f} points**",
                                inline=False)
            mod_embed.add_field(name="Total Deduction", value=f"**{pending_payout['total_deduction']:.2f} points**",
                                inline=False)
            mod_embed.set_footer(text="Use `!paid <@user>` to confirm this payment.")
            await mod_channel.send(embed=mod_embed)
        else:
            logger.warning(f"Could not find mod channel with ID: {config.MOD_PAYMENT_REVIEW_CHANNEL_ID}")

        user_embed = discord.Embed(title="✅ Payout Submitted",
                                   description=f"Your payout request for **{pending_payout['amount']:.2f} points** has been successfully submitted for review.",
                                   color=discord.Color.green())
        user_embed.add_field(name="New Available Balance", value=f"**{user_data['available_points']:.2f} points**",
                             inline=True)
        user_embed.set_footer(text="A moderator will finalize your payment shortly.")
        await ctx.send(f"{ctx.author.mention}", embed=user_embed)


    # ===!PAID ===
    @commands.command(name="paid",
                      help="(Moderator Only) Finalizes a payout request by burning the points and notifying the user.")
    @commands.has_permissions(manage_roles=True)
    async def paid(self, ctx, member: discord.Member):
        """(Moderator Only) Finalizes a payout request by burning the points and notifying the user."""
        # Delete the command message immediately
        await ctx.message.delete()

        # 1. Check for the correct channel
        if ctx.channel.id != config.MOD_PAYMENT_REVIEW_CHANNEL_ID:
            return

        # ✅ FIX: Load both user and admin points from the database
        users_points = await self.bot.load_all_json("users_points")
        admin_points = await self.bot.load_single_json("admin_points", "main", {})

        user_id = str(member.id)
        user_data = users_points.get(user_id, {})
        pending_payout = user_data.get("pending_payout")

        if not pending_payout:
            embed = discord.Embed(title="❌ Error",
                                  description=f"**{member.mention}** does not have a pending payout to mark as paid.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=10)
            return

        requested_amount = pending_payout["amount"]
        if admin_points.get("balance", 0) < requested_amount:
            embed = discord.Embed(title="❌ Transaction Failed",
                                  description="The admin's balance is insufficient to burn the requested amount.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=10)
            return

        payout_channel = self.bot.get_channel(config.PAYOUT_REQUEST_CHANNEL_ID)
        if not payout_channel:
            embed = discord.Embed(title="❌ Configuration Error",
                                  description=f"The payout channel (ID: `{config.PAYOUT_REQUEST_CHANNEL_ID}`) could not be found. Please check your configuration.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=10)
            return

        # 2. Process the transaction on the loaded data
        fee = pending_payout["fee"]

        admin_points["balance"] -= requested_amount
        admin_points["in_circulation"] -= requested_amount
        admin_points["burned"] = admin_points.get("burned", 0) + requested_amount
        admin_points["treasury"] = admin_points.get("treasury", 0) + fee

        del user_data["pending_payout"]
        users_points[user_id] = user_data

        # ✅ FIX: Save both updated dictionaries to the database
        await self.bot.save_all_json("users_points", users_points)
        await self.bot.save_single_json("admin_points", "main", admin_points)

        # 3. Notify the user and moderator
        user_embed = discord.Embed(title="💸 Payout Processed!",
                                   description=f"🎉 {member.mention}, great news! Your payout request has been **successfully processed**.",
                                   color=discord.Color.green())
        user_embed.add_field(name="Status", value="✅ Finalized and points have been **burned** from circulation.",
                             inline=False)
        user_embed.set_thumbnail(url=member.avatar.url if member.avatar else member.default_avatar.url)
        user_embed.set_footer(text="Thank you for being part of the community 🌍")
        user_embed.timestamp = datetime.now(UTC)
        await payout_channel.send(embed=user_embed)

        mod_embed = discord.Embed(title="✅ Payout Finalized",
                                  description=f"A payout success message has been sent to **{member.mention}**.",
                                  color=discord.Color.green())
        mod_embed.add_field(name="Amount", value=f"{requested_amount:.2f} points", inline=True)
        mod_embed.set_footer(text=f"Payout finalized by {ctx.author.name}")
        await ctx.send(embed=mod_embed, delete_after=10)

    # === NEW: !xp command ===
    @commands.command(name="xp", help="Displays the user's current total XP and rank.")
    @commands.cooldown(2, 60, commands.BucketType.user)
    async def xp_command(self, ctx, member: discord.Member = None):
        """Displays the user's current total XP and rank."""
        # Delete the user's command message
        await ctx.message.delete()

        # 1. Check for the correct channel
        if ctx.channel.id != config.XP_REWARD_CHANNEL_ID:
            await ctx.send(f"❌ The `!xp` command can only be used in the <#{config.XP_REWARD_CHANNEL_ID}> channel.",
                           delete_after=15)
            return

        # ✅ FIX: Load the user XP data from the database
        user_xp = await self.bot.load_all_json("user_xp")

        target_member = member if member else ctx.author
        user_id = str(target_member.id)
        guild = self.bot.get_guild(config.SERVER_ID)
        if not guild:
            logger.error("Could not find the server. Please check the SERVER_ID constant.")
            await ctx.send("❌ Error: Could not find the server. Please check the SERVER_ID constant.", delete_after=15)
            return

        allowed_roles = [config.ADMIN_ROLE_ID, config.MOD_ROLE_ID]
        all_users = []
        # ✅ FIX: Use the loaded user_xp dictionary instead of the in-memory one
        for uid, data in user_xp.items():
            member_obj = guild.get_member(int(uid))
            if member_obj and not any(role.id in allowed_roles for role in member_obj.roles):
                all_users.append((uid, data.get("xp", 0)))

        sorted_xp_users = sorted(all_users, key=lambda item: item[1], reverse=True)
        xp_balance = user_xp.get(user_id, {}).get("xp", 0)
        user_rank = next((i for i, (uid, _) in enumerate(sorted_xp_users) if uid == user_id), None)

        if xp_balance == 0:
            embed = discord.Embed(title="📊 XP Tracker",
                                  description=f"❌ {target_member.mention} has not earned any XP yet.",
                                  color=discord.Color.red())
            embed.set_footer(text="Keep chatting and completing quests to gain XP!")
            embed.timestamp = datetime.now(UTC)
            await ctx.send(embed=embed, delete_after=15)
            return

        medal = ""
        if user_rank == 0:
            medal = "🥇"
        elif user_rank == 1:
            medal = "🥈"
        elif user_rank == 2:
            medal = "🥉"

        embed = discord.Embed(title="🌟 XP Status", description=f"{medal} {target_member.mention}'s XP Summary",
                              color=discord.Color.blue())
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
    @commands.command(name="quests", help="(Admin Only) Posts 3 new weekly quests to the quest board.")
    @commands.has_permissions(administrator=True)
    async def quests(self, ctx, *, all_quests: str):
        """(Admin Only) Posts 3 new weekly quests to the quest board."""
        # Delete the command message immediately
        await ctx.message.delete()

        quests_list = [q.strip() for q in all_quests.strip().split("\n")]
        if len(quests_list) != 3:
            embed = discord.Embed(title="❌ Invalid Input",
                                  description="Please provide **exactly 3 quests**, with each quest on a new line.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=15)
            return

        # ✅ FIX: Load quest data before updating
        weekly_quests = await self.bot.load_single_json("weekly_quests", "main", {"week": 0, "quests": []})

        # 1. Update quests on the loaded dictionaries
        weekly_quests["week"] += 1
        weekly_quests["quests"] = quests_list
        quest_submissions = {}  # Resetting previous submissions

        # ✅ FIX: Save the updated data immediately
        await self.bot.save_single_json("weekly_quests", "main", self.weekly_quests)
        await self.bot.save_all_json("quest_submissions", self.quest_submissions)

        # 2. Post new quests and send confirmation
        board = self.bot.get_channel(config.QUEST_BOARD_CHANNEL_ID)
        if not board:
            logger.warning(f"Quest board channel (ID: {config.QUEST_BOARD_CHANNEL_ID}) not found.")
            error_embed = discord.Embed(title="❌ Configuration Error",
                                        description="The quest board channel could not be found. Please check the `QUEST_BOARD_CHANNEL_ID`.",
                                        color=discord.Color.red())
            await ctx.send(embed=error_embed, delete_after=15)
            return

        # ✅ FIX: Use the loaded `weekly_quests` dictionary to get the current week and quests
        embed = discord.Embed(title=f"📋 Weekly Quests – Week {weekly_quests['week']}",
                              description="Complete the quests below and submit proof using `!submitquest <quest_number> <tweet_link>`",
                              color=discord.Color.gold())
        for i, q in enumerate(weekly_quests['quests'], start=1):
            embed.add_field(name=f"⚔️ Quest {i}", value=f"{q}", inline=False)
        embed.set_footer(text="Earn +100 Points for each approved quest • Good luck!")
        embed.timestamp = datetime.now(UTC)

        await board.send(embed=embed)
        success_embed = discord.Embed(title="✅ Quests Posted!",
                                      description="New quests have been successfully posted and previous submissions have been reset.",
                                      color=discord.Color.green())
        await ctx.send(embed=success_embed, delete_after=15)

    @commands.command(name="submitquest", help="Submits a weekly quest for review.")
    async def submitquest(self, ctx, quest_number: int, tweet_link: str):
        """Submits a weekly quest for review."""
        # Delete the command message immediately
        await ctx.message.delete()

        # 1. Check for the correct channel
        if ctx.channel.id != config.QUEST_SUBMIT_CHANNEL_ID:
            embed = discord.Embed(title="❌ Incorrect Channel",
                                  description=f"Please use the <#{config.QUEST_SUBMIT_CHANNEL_ID}> channel to submit quests.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=10)
            return

        # ✅ FIX: Load all necessary data from the database
        weekly_quests = await self.bot.load_single_json("weekly_quests", "main", {"week": 0, "quests": []})
        quest_submissions = await self.bot.load_all_json("quest_submissions")
        approved_proofs = await self.bot.load_list_values("approved_proofs", "normalized_url")

        user_id = str(ctx.author.id)
        week = str(weekly_quests.get("week", "0"))

        # 2. Validate quest and submission data
        if int(week) == 0 or not weekly_quests.get("quests"):
            embed = discord.Embed(title="❌ No Active Quests",
                                  description="There are no active weekly quests right now. Please wait for new quests to be posted!",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=15)
            return

        if quest_number not in [1, 2, 3]:
            embed = discord.Embed(title="❌ Invalid Quest Number", description="Quest number must be 1, 2, or 3.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=10)
            return

        normalized_tweet_link = normalize_url(tweet_link)
        if not normalized_tweet_link or "twitter.com/".lower() not in normalized_tweet_link:
            embed = discord.Embed(title="❌ Invalid Link",
                                  description="Please provide a valid Twitter/X link for your quest submission.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=15)
            return

        if normalized_tweet_link in approved_proofs:
            embed = discord.Embed(title="🚫 Duplicate Submission",
                                  description=f"{ctx.author.mention}, this proof (tweet) has already been submitted and approved for a quest or engagement. Please ensure your quest proofs are unique.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=20)
            return

        # ✅ FIX: Access and modify the loaded quest_submissions dictionary
        user_week_data = quest_submissions.setdefault(user_id, {}).setdefault(week, {})
        if str(quest_number) in user_week_data:
            status = user_week_data[str(quest_number)]["status"]
            if status == "pending":
                embed = discord.Embed(title="⚠️ Pending Submission",
                                      description="You already have a pending submission for this quest. Please wait for it to be reviewed.",
                                      color=discord.Color.orange())
                await ctx.send(embed=embed, delete_after=15)
            elif status == "approved":
                embed = discord.Embed(title="⚠️ Already Completed",
                                      description="You have already successfully completed and been approved for this quest.",
                                      color=discord.Color.orange())
                await ctx.send(embed=embed, delete_after=15)
            return

        # 3. Store the submission in the loaded dictionary
        user_week_data[str(quest_number)] = {
            "tweet": tweet_link, "normalized_tweet": normalized_tweet_link, "status": "pending",
            "timestamp": int(discord.utils.utcnow().timestamp())
        }

        # ✅ FIX: Save the updated quest submissions immediately
        await self.bot.save_all_json("quest_submissions", self.quest_submissions)

        # 4. Notify moderators and the user
        mod_review_channel = self.bot.get_channel(config.MOD_QUEST_REVIEW_CHANNEL_ID)
        if mod_review_channel:
            embed = discord.Embed(title="🧩 New Quest Submission",
                                  description=f"A new quest has been submitted for review by {ctx.author.mention}.",
                                  color=discord.Color.blue(), timestamp=datetime.now(UTC))
            embed.add_field(name="User", value=f"**{ctx.author.name}**", inline=False)
            embed.add_field(name="Quest #", value=f"**{quest_number}**", inline=True)
            embed.add_field(name="Week", value=f"**{week}**", inline=True)
            embed.add_field(name="Link", value=f"[Click to view]({tweet_link})", inline=False)
            embed.set_footer(text=f"To verify: !verifyquest {user_id} {quest_number} <approve|reject>")
            await mod_review_channel.send(embed=embed)
        else:
            logger.warning(f"Mod quest review channel (ID: {config.MOD_QUEST_REVIEW_CHANNEL_ID}) not found.")

        embed = discord.Embed(title="✅ Submission Received!",
                              description=f"Your submission for **Quest {quest_number}** has been received and sent for review.",
                              color=discord.Color.green())
        await ctx.send(f"{ctx.author.mention}", embed=embed, delete_after=15)

    @commands.command(name="verifyquest", help="(Moderator Only) Verifies a submitted quest.")
    @commands.has_permissions(manage_messages=True)
    async def verifyquest(self, ctx, member: discord.Member, quest_number: int, action: str):
        """(Moderator Only) Verifies a submitted quest."""
        # Delete the command message immediately
        await ctx.message.delete()

        # 1. Check for the correct channel
        if ctx.channel.id != config.MOD_QUEST_REVIEW_CHANNEL_ID:
            embed = discord.Embed(title="❌ Incorrect Channel",
                                  description=f"This command can only be used in the <#{config.MOD_QUEST_REVIEW_CHANNEL_ID}> channel.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=10)
            return

        # ✅ FIX: Load all necessary data from the database
        weekly_quests = await self.bot.load_single_json("weekly_quests", "main", {"week": 0, "quests": []})
        quest_submissions = await self.bot.load_all_json("quest_submissions")
        admin_points = await self.bot.load_single_json("admin_points", "main", {})
        users_points = await self.bot.load_all_json("users_points")
        approved_proofs = await self.bot.load_list_values("approved_proofs", "normalized_url")

        user_id = str(member.id)
        week = str(weekly_quests.get("week", "0"))
        action = action.lower()

        # 2. Validate the submission
        if user_id not in quest_submissions or week not in quest_submissions[user_id]:
            embed = discord.Embed(title="❌ Submission Not Found",
                                  description="No quest submission found for this user for the current week.",
                                  color=discord.Color.red())
            await ctx.send(embed=embed, delete_after=10)
            return

        quest_data = quest_submissions[user_id][week]
        if str(quest_number) not in quest_data:
            embed = discord.Embed(title="⚠️ Quest Not Submitted",
                                  description=f"Quest **{quest_number}** was not submitted by {member.mention} for this week.",
                                  color=discord.Color.orange())
            await ctx.send(embed=embed, delete_after=10)
            return

        submission_status = quest_data[str(quest_number)]["status"]
        if submission_status == "approved":
            embed = discord.Embed(title="⚠️ Already Approved",
                                  description=f"Quest **{quest_number}** for {member.mention} is already approved.",
                                  color=discord.Color.orange())
            await ctx.send(embed=embed, delete_after=10)
            return

        # 3. Process action (approve/reject)
        if action == "approve":
            points_to_award = config.QUEST_POINTS
            if admin_points.get("balance", 0) < points_to_award:
                embed = discord.Embed(title="❌ Admin Balance Too Low",
                                      description=f"The admin balance is too low to award **{points_to_award:.2f} points**.",
                                      color=discord.Color.red())
                await ctx.send(embed=embed, delete_after=10)
                logger.warning("Admin balance is too low to award quest points. Skipping.")
                return

            # Update data on the loaded dictionaries
            users_points.setdefault(user_id, {"all_time_points": 0.0, "available_points": 0.0})
            users_points[user_id]["all_time_points"] += points_to_award
            users_points[user_id]["available_points"] += points_to_award

            quest_data[str(quest_number)]["status"] = "approved"

            admin_points["balance"] -= points_to_award
            admin_points["in_circulation"] += points_to_award

            if "normalized_tweet" in quest_data[str(quest_number)]:
                normalized_url = quest_data[str(quest_number)]["normalized_tweet"]
                if normalized_url not in approved_proofs:
                    approved_proofs.append(normalized_url)

            await self.bot.log_points_transaction(user_id, points_to_award, f"Quest {quest_number} approval")

            # ✅ FIX: Save all the updated dictionaries back to the database
            await self.bot.save_all_json("users_points", users_points)
            await self.bot.save_all_json("quest_submissions", self.quest_submissions)
            await self.bot.save_single_json("admin_points", "main", admin_points)
            await self.bot.save_list_values("approved_proofs", self.approved_proofs, "normalized_url")

            # Send an approval message to the user channel
            user_channel = self.bot.get_channel(config.QUEST_SUBMIT_CHANNEL_ID)
            user_embed = discord.Embed(title="✨ Quest Approved! ✨",
                                       description=f"🎉 Congratulations, {member.mention}! Your submission for **Quest {quest_number}** has been **approved**!",
                                       color=discord.Color.green())
            user_embed.add_field(name="Points Earned", value=f"💰 **+{points_to_award:.2f} points**", inline=False)
            # ✅ FIX: Use the updated users_points dictionary to get the new balance
            user_embed.add_field(name="New Balance",
                                 value=f"🪙 **{users_points[user_id]['available_points']:.2f} points**",
                                 inline=False)
            user_embed.set_footer(text="Great job! Keep an eye out for next week's quests!")
            user_embed.timestamp = datetime.now(UTC)
            await user_channel.send(embed=user_embed)

        elif action == "reject":
            # Update data on the loaded dictionary
            quest_data[str(quest_number)]["status"] = "rejected"

            # ✅ FIX: Save the updated quest submissions dictionary
            await self.bot.save_all_json("quest_submissions", self.quest_submissions)

            # Send a rejection message to the user channel
            user_channel = self.bot.get_channel(config.QUEST_SUBMIT_CHANNEL_ID)
            user_embed = discord.Embed(title="❌ Quest Rejected",
                                       description=f"Hello, {member.mention}. Your submission for **Quest {quest_number}** was **rejected**.",
                                       color=discord.Color.red())
            user_embed.add_field(name="Reason",
                                 value="Your submission did not meet the quest requirements. Please review the rules and try again if necessary.",
                                 inline=False)
            user_embed.set_footer(text="Keep trying! We look forward to your next submission.")
            user_embed.timestamp = datetime.now(UTC)
            await user_channel.send(embed=user_embed)

        else:
            embed = discord.Embed(title="⚠️ Invalid Action", description="Please use **'approve'** or **'reject'**.",
                                  color=discord.Color.orange())
            await ctx.send(embed=embed, delete_after=10)
            return

        confirmation_embed = discord.Embed(title="✅ Quest Verified",
                                           description=f"Quest **{quest_number}** for **{member.name}** has been marked as '{action}'.",
                                           color=discord.Color.green())
        confirmation_embed.set_footer(text=f"Action by {ctx.author.name}")
        await ctx.send(embed=confirmation_embed, delete_after=10)


    # === REACT TO AWARD POINTS TO USERS ===
    @commands.Cog.listener()
    async def on_reaction_add(self, reaction, user):
        """Handles reaction-based point awards."""

        # Check 1: Ignore bot and self-awards
        if user.bot or reaction.message.author.id == user.id:
            return

        # Check 2: Category and emoji
        if reaction.message.channel.category is None or \
                reaction.message.channel.category.id not in config.REACTION_CATEGORY_IDS or \
                str(reaction.emoji) != self.bot.REACTION_EMOJI:
            return

        # ✅ FIX: Load all necessary data from the database
        users_points = await self.bot.load_all_json("users_points")
        admin_points = await self.bot.load_single_json("admin_points", "main", {})
        processed_reactions = set(await self.bot.load_list_values("processed_reactions", "reaction_identifier"))

        # Check 3: Role and admin balance
        reactor_member = reaction.message.guild.get_member(user.id)
        allowed_roles = [config.ADMIN_ROLE_ID, config.MOD_ROLE_ID]
        if not reactor_member or (not reactor_member.guild_permissions.administrator and not any(
                role.id in allowed_roles for role in reactor_member.roles)):
            return

        points_to_add = random.uniform(config.MIN_REACTION_POINTS, config.MAX_REACTION_POINTS)
        if 'balance' not in admin_points or admin_points['balance'] < points_to_add:
            logger.warning(f"Admin balance too low. Award of {points_to_add:.2f} points failed.")
            return

        # Check 4: Processed reaction check
        reaction_identifier = f"{reaction.message.id}-{user.id}"
        if reaction_identifier in processed_reactions:
            return

        # --- All checks passed. Begin awarding processes on loaded data ---
        user_id = str(reaction.message.author.id)

        # Award points to the message author
        users_points.setdefault(user_id, {"all_time_points": 0.0, "available_points": 0.0})
        users_points[user_id]["all_time_points"] += points_to_add
        users_points[user_id]["available_points"] += points_to_add

        # Deduct points from the admin balance
        admin_points["balance"] -= points_to_add
        admin_points["in_circulation"] = admin_points.get("in_circulation", 0.0) + points_to_add

        # Log the transaction in memory
        await self.bot.log_points_transaction(user_id, points_to_add, f"Reaction award from {user.name}")

        # Add reaction to the processed set
        processed_reactions.add(reaction_identifier)

        # ✅ FIX: Save all the updated data back to the database
        await self.bot.save_all_json("users_points", users_points)
        await self.bot.save_single_json("admin_points", "main", admin_points)
        await self.bot.save_list_values("processed_reactions", list(self.processed_reactions), "reaction_identifier")

        # Confirmation Message with an Embed
        embed = discord.Embed(
            title="✨ Points Awarded! ✨",
            description=f"{reaction.message.author.mention} received **{points_to_add:.2f} points**!",
            color=discord.Color.green()
        )
        embed.add_field(name="Awarded By", value=user.mention, inline=True)
        embed.add_field(name="Reason", value="For a great message!", inline=True)
        embed.set_thumbnail(
            url=reaction.message.author.avatar.url if reaction.message.author.avatar else reaction.message.author.default_avatar.url)
        embed.set_footer(text="ManaVerse Points System - Keep the positive vibes flowing! 😊")
        embed.timestamp = datetime.now(UTC)

        await reaction.message.channel.send(embed=embed, delete_after=20)
        logger.info(f"Successfully awarded {points_to_add:.2f} points to {reaction.message.author.name}")

    # -----------------------------------S U P P O R T --- S Y S T E M------------------------
    @commands.command(name="close", help="(Admin/Mod Only) Archives a ticket and schedules it for deletion.")
    @commands.has_any_role(config.ADMIN_ROLE_ID, config.MOD_ROLE_ID)
    async def close(self, ctx):
        """(Admin/Mod Only) Archives a ticket and schedules it for deletion."""
        # Delete the command message
        await ctx.message.delete()

        # ✅ FIX: Load active tickets from the database
        self.active_tickets = await self.bot.load_all_json("active_tickets")

        ticket_channel_id = ctx.channel.id
        if ticket_channel_id not in self.active_tickets:
            await ctx.send("❌ This command can only be used inside a ticket channel.", delete_after=10)
            return

        # 1. Archive the channel and remove user permissions
        archived_category = self.bot.get_channel(config.ARCHIVED_TICKETS_CATEGORY_ID)
        if not archived_category:
            await ctx.send("❌ The archived tickets category was not found.", delete_after=10)
            return

        await ctx.channel.edit(name=f"closed-{ctx.channel.name}", category=archived_category)
        user_id = self.active_tickets.get(ticket_channel_id)
        user = ctx.guild.get_member(user_id)
        if user:
            await ctx.channel.set_permissions(user, overwrite=None)

        await ctx.send("This ticket has been closed and will be deleted in 30 days.")

        # 2. Update the active tickets on the loaded dictionary
        del self.active_tickets[ticket_channel_id]

        # ✅ FIX: Save the updated active tickets dictionary
        await self.bot.save_all_json("active_tickets", self.active_tickets)

    # -------------------------- D I S C O R D         E M B E D -------------------------------------------------
    @commands.command(name="announce",
                      help="(Admin Only) Posts a new announcement to the announcement channel as a premium embed.")
    @commands.has_permissions(administrator=True)
    async def announce(self, ctx, title: str, *, message: str):
        """(Admin Only) Posts a new announcement to the announcement channel as a premium embed."""
        # Delete the command message immediately
        await ctx.message.delete()

        announcement_channel = self.bot.get_channel(config.ANNOUNCEMENT_CHANNEL_ID)
        if not announcement_channel:
            await ctx.send("❌ Announcement channel not found. Please check the ID.", delete_after=10)
            logger.error(f"Announcement channel not found. ID: {config.ANNOUNCEMENT_CHANNEL_ID}")
            return

        embed = discord.Embed(title=title, description=message, color=discord.Color.blue())
        embed.set_footer(text="Official Server Announcement")
        embed.timestamp = datetime.now(UTC)

        await announcement_channel.send(embed=embed)
        await ctx.send("✅ Announcement posted successfully!", delete_after=10)
        logger.info(f"New announcement posted by {ctx.author.name}.")

    # --------------------M Y S T E R Y     B O X     S Y S T E M-------------------------------------
    @commands.command(name="mysterybox", help="Open a Mystery Box to win a random amount of points.")
    async def cmd_mysterybox(self, ctx: commands.Context):
        # Delete the command message immediately
        await ctx.message.delete()
        user_id = str(ctx.author.id)

        # ✅ FIX: Load all necessary data from the database
        users_points = await self.bot.load_all_json("users_points")
        admin_points = await self.bot.load_single_json("admin_points", "main", {})
        mysterybox_uses = await self.bot.load_all_json("mysterybox_uses")

        # 1. Validation Checks
        if ctx.channel.id != config.MYSTERYBOX_CHANNEL_ID:
            await ctx.send(f"❌ Use this command in <#{config.MYSTERYBOX_CHANNEL_ID}> only.", delete_after=8)
            return

        used = self.bot.mb_get_uses_in_last_24h(user_id, mysterybox_uses)
        if used >= self.bot.MYSTERYBOX_MAX_PER_24H:
            oldest = min(mysterybox_uses[user_id]) if mysterybox_uses.get(user_id) else time.time()
            secs = int(24 * 3600 - (time.time() - oldest))
            hrs = secs // 3600
            mins = (secs % 3600) // 60
            await ctx.send(f"⏳ You’ve reached your daily limit. Try again in **{hrs}h {mins}m**.",
                           delete_after=8)
            return

        user_balance = users_points.get(user_id, {}).get("available_points", 0.0)
        if user_balance < config.MYSTERYBOX_COST:
            await ctx.send(f"❌ You need **{config.MYSTERYBOX_COST} MVpts** to open a Mystery Box.", delete_after=8)
            return

        # 2. Process Transaction on loaded data
        users_points.setdefault(user_id, {"all_time_points": 0.0, "available_points": 0.0})
        users_points[user_id]["available_points"] -= config.MYSTERYBOX_COST
        await self.bot.log_points_transaction(user_id, -float(config.MYSTERYBOX_COST), "Mystery Box: cost")

        reward = random.choices(config.MYSTERYBOX_REWARDS, weights=config.MYSTERYBOX_WEIGHTS, k=1)[0]

        users_points[user_id]["available_points"] += reward
        users_points[user_id]["all_time_points"] += reward
        await self.bot.log_points_transaction(user_id, float(reward), "Mystery Box: reward")

        # Handle point flow based on the reward
        if reward > config.MYSTERYBOX_COST:
            delta = reward - config.MYSTERYBOX_COST
            if self.bot.admin_can_issue(delta, admin_points):
                admin_points["balance"] -= delta
                admin_points["in_circulation"] += delta
            else:
                users_points[user_id]["available_points"] -= delta
                users_points[user_id]["all_time_points"] -= delta
                logger.warning("Admin balance too low to cover Mystery Box win. Award capped at cost.")
                reward = config.MYSTERYBOX_COST
        elif reward < config.MYSTERYBOX_COST:
            burn = config.MYSTERYBOX_COST - reward
            admin_points["burned"] = admin_points.get("burned", 0) + burn
            admin_points["in_circulation"] -= burn

        self.bot.mb_add_use(user_id, mysterybox_uses)

        # ✅ FIX: Save all updated data back to the database
        await self.bot.save_all_json("users_points", users_points)
        await self.bot.save_single_json("admin_points", "main", admin_points)
        await self.bot.save_all_json("mysterybox_uses", mysterybox_uses)

        # 3. Notifications and Logging
        log_ch = self.bot.get_channel(config.COMMAND_LOG_CHANNEL_ID)
        if log_ch:
            await log_ch.send(f"🎁 Mystery Box used by <@{user_id}> — reward: **{reward}** MVpts")

        color = discord.Color.green() if reward >= config.MYSTERYBOX_COST else discord.Color.orange()
        embed = discord.Embed(
            title="🎁 Mystery Box Opened!",
            description=f"{ctx.author.mention} you spent **{config.MYSTERYBOX_COST} MVpts** and received:",
            color=color
        )
        embed.add_field(name="Reward", value=f"💎 **{reward} points**", inline=False)
        embed.set_footer(text="Good luck next time!" if reward < config.MYSTERYBOX_COST else "Nice hit!")
        embed.timestamp = datetime.now(UTC)
        await ctx.send(embed=embed)

    @commands.Cog.listener()
    async def on_command(self, ctx):
        """Logs every command that is successfully completed."""

        # Log the command to the centralized logger
        logger.info(
            f"Command '{ctx.command.name}' executed by {ctx.author.name} "
            f"(ID: {ctx.author.id}) in channel {ctx.channel.name}."
        )

        log_channel = ctx.guild.get_channel(config.COMMAND_LOG_CHANNEL_ID)
        if log_channel:
            embed = discord.Embed(
                title="Command Executed",
                description=f"**User:** {ctx.author.mention}\n"
                            f"**Command:** `{ctx.command.name}`\n"
                            f"**Channel:** {ctx.channel.mention}",
                color=discord.Color.blue()
            )
            embed.set_footer(text=f"User ID: {ctx.author.id}")
            await log_channel.send(embed=embed)

    @commands.Cog.listener()
    async def on_command_error(self, ctx, error):
        # Log the full error for debugging purposes
        logger.error(f"❌ An error occurred with command '{ctx.command}': {error}")

        # Handle specific errors with user-friendly messages
        if isinstance(error, commands.CommandOnCooldown):
            seconds = int(error.retry_after)
            await ctx.send(
                f"⚠️ {ctx.author.mention}, you're on cooldown for this command. Try again in **{seconds} seconds**.",
                delete_after=10
            )
            await ctx.message.delete(delay=10)

        elif isinstance(error, commands.MissingPermissions):
            await ctx.send(
                "❌ You do not have the required permissions to use this command.",
                delete_after=10
            )

        elif isinstance(error, commands.MissingRole):
            await ctx.send(
                "❌ You do not have the required role to use this command.",
                delete_after=10
            )

        elif isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(
                f"❌ Missing a required argument. The correct usage is: `{ctx.prefix}{ctx.command.name} {ctx.command.signature}`",
                delete_after=15
            )

        else:
            # For all other unhandled errors, send a generic message to the user.
            # The full error details are already logged for debugging.
            await ctx.send(
                f"❌ An unexpected error occurred while running the command. Please try again or contact a bot administrator.",
                delete_after=15
            )

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.author.bot:
            return

        # --- 1. Ticket System Logic (if applicable) ---
        if message.channel.id == config.SUPPORT_CHANNEL_ID:
            # ✅ FIX: Load active tickets using the correct key
            active_tickets = await self.bot.load_all_json("active_tickets")
            user_id = message.author.id

            if user_id in active_tickets.values():
                embed = discord.Embed(
                    title="❌ Active Ticket Found",
                    description="You already have an active ticket. Please close it before opening a new one.",
                    color=discord.Color.red()
                )
                await message.channel.send(embed=embed, delete_after=20)
                logger.info(f"Blocked new ticket from {message.author.name}. Active ticket already exists.")
                await message.delete()
                return

            guild = message.guild
            user = message.author
            ticket_name = f"ticket-{user.name.lower()}"

            overwrites = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False),
                guild.get_role(config.ADMIN_ROLE_ID): discord.PermissionOverwrite(view_channel=True),
                guild.get_role(config.MOD_ROLE_ID): discord.PermissionOverwrite(view_channel=True),
                user: discord.PermissionOverwrite(view_channel=True, send_messages=True),
                guild.get_member(self.bot.user.id): discord.PermissionOverwrite(view_channel=True,
                                                                                send_messages=True)
            }

            try:
                ticket_channel = await guild.create_text_channel(
                    ticket_name,
                    category=guild.get_channel(config.TICKETS_CATEGORY_ID),
                    overwrites=overwrites
                )

                welcome_embed = discord.Embed(
                    title="🎫 New Support Ticket",
                    description=f"Thank you for reaching out, {user.mention}. A support team member will be with you shortly.",
                    color=discord.Color.blue(),
                    timestamp=datetime.now(UTC)
                )
                welcome_embed.add_field(name="Original Message", value=f"> {message.content}", inline=False)
                welcome_embed.set_footer(text="A team member will respond soon.")

                await ticket_channel.send(f"{user.mention}", embed=welcome_embed)
                await ticket_channel.send(
                    f"Support team, you have a new ticket from {user.mention}! Use `!close` to close this ticket.")

                confirm_embed = discord.Embed(
                    title="Ticket Created! 🎉",
                    description=f"Your ticket has been created at {ticket_channel.mention}. A support team member will be with you shortly.",
                    color=discord.Color.green()
                )
                await message.channel.send(embed=confirm_embed, delete_after=30)

                await message.delete()

                active_tickets[ticket_channel.id] = user.id
                await self.bot.save_all_json("active_tickets", active_tickets)

                logger.info(f"Created new ticket for {user.name} in channel #{ticket_channel.name}.")

            except discord.Forbidden:
                logger.error("Bot is missing permissions to create channels or manage roles.")
                embed = discord.Embed(
                    title="❌ Permissions Error",
                    description="An error occurred. I don't have the permissions to create a ticket.",
                    color=discord.Color.red()
                )
                await message.channel.send(embed=embed, delete_after=20)
            except Exception as e:
                logger.error(f"❌ An unhandled error occurred in ticket creation: {e}")
                embed = discord.Embed(
                    title="❌ An Error Occurred",
                    description="An unexpected error occurred while creating the ticket.",
                    color=discord.Color.red()
                )
                await message.channel.send(embed=embed, delete_after=20)
            return

        # --- 2. VIP Post Logic ---
        if message.channel.id == config.ENGAGEMENT_CHANNEL_ID:
            # ✅ FIX: Load vip posts data for persistence
            vip_posts = await self.bot.load_all_json("vip_posts")

            member = message.author
            is_mod_or_admin = any(role.id in [config.ADMIN_ROLE_ID, config.MOD_ROLE_ID] for role in member.roles)

            if is_mod_or_admin:
                await self.bot.process_commands(message)
                return

            user_id = str(member.id)
            vip_posts.setdefault(user_id, {"count": 0, "last_date": ""})

            today = str(datetime.now(UTC).date())
            if vip_posts[user_id]["last_date"] != today:
                vip_posts[user_id]["count"] = 0
                vip_posts[user_id]["last_date"] = today

            if config.VIP_ROLE_ID not in [role.id for role in member.roles]:
                await message.delete()
                await message.channel.send(f"❌ {member.mention}, only **VIP members** can post in this channel!",
                                           delete_after=10)
                logger.info(f"Deleted message from non-VIP user {member.name} in engagement channel.")

                # ✅ FIX: Save the updated vip_posts before returning
                await self.bot.save_all_json("vip_posts", vip_posts)
                return

            vip_posts[user_id]["count"] += 1

            if vip_posts[user_id]["count"] > 3:
                await message.delete()
                await message.channel.send(
                    f"🚫 {member.mention}, you've reached your daily post limit in this channel (3 per day).",
                    delete_after=20)
                logger.info(f"Deleted message from {member.name} for exceeding VIP daily limit.")

            # ✅ FIX: Save the updated vip_posts dictionary
            await self.bot.save_all_json("vip_posts", vip_posts)
            return

        # The Payment Message Logic is already safe as it doesn't modify data.
        if message.channel.id == config.PAYMENT_CHANNEL_ID:
            # This logic block is fine as-is.
            return

        # Process only GM/MV points in the designated channel
        if message.channel.id == config.GM_MV_CHANNEL_ID:
            content = message.content.lower().strip()
            if content == "gm" or content == "mv":
                user_id = str(message.author.id)
                today = str(datetime.now(UTC).date())

                # ✅ FIX: Load all necessary data from the database
                users_points = await self.bot.load_all_json("users_points")
                admin_points = await self.bot.load_single_json("admin_points", "main", {})
                gm_log = await self.bot.load_all_json("gm_log")

                if gm_log.get(user_id) != today:
                    is_author_admin = any(role.id == config.ADMIN_ROLE_ID for role in message.author.roles)

                    if is_author_admin:
                        # ✅ FIX: Modify loaded admin_points
                        admin_points["balance"] -= config.GM_MV_POINTS_REWARD
                        admin_points["my_points"] += config.GM_MV_POINTS_REWARD
                        admin_points["in_circulation"] += config.GM_MV_POINTS_REWARD
                        await self.bot.save_single_json("admin_points", "main", admin_points)
                    else:
                        # ✅ FIX: Check and modify loaded admin_points and users_points
                        if admin_points["balance"] < config.GM_MV_POINTS_REWARD:
                            logger.warning("⚠️ Admin balance is too low to award GM points. Skipping.")
                            await message.channel.send("⚠️ An error occurred. Please contact an admin.",
                                                       delete_after=10)
                            return
                        user_data = users_points.setdefault(user_id, {"all_time_points": 0.0,
                                                                     "available_points": 0.0})
                        user_data["all_time_points"] += config.GM_MV_POINTS_REWARD
                        user_data["available_points"] += config.GM_MV_POINTS_REWARD
                        admin_points["balance"] -= config.GM_MV_POINTS_REWARD
                        admin_points["in_circulation"] += config.GM_MV_POINTS_REWARD

                    await self.bot.log_points_transaction(user_id, config.GM_MV_POINTS_REWARD, "GM points")

                    # ✅ FIX: Update and save all three data tables
                    gm_log[user_id] = today
                    await self.bot.save_all_json("users_points", users_points)
                    await self.bot.save_single_json("admin_points", "main", admin_points)
                    await self.bot.save_all_json("gm_log", gm_log)

                    embed = discord.Embed(
                        title="🎉 GM/MV Points Awarded! 🎉",
                        description=f"Congratulations, {message.author.mention}! You've been rewarded **{config.GM_MV_POINTS_REWARD:.2f} points** for your GM/MV message.",
                        color=discord.Color.gold()
                    )
                    embed.set_image(url="https://media.tenor.com/Fw5m_qY3S2gAAAAC/puffed-celebration.gif")
                    embed.set_footer(
                        text=f"Your new balance is {users_points.get(user_id, {}).get('available_points', 0):.2f} points" if not is_author_admin else "Points have been added to your balance.")
                    embed.timestamp = datetime.now(UTC)
                    await message.channel.send(embed=embed, delete_after=20)

        # --- 4. XP and Moderation Logic (applies to ALL messages) ---
        user_id = str(message.author.id)
        # ✅ FIX: Load user_xp and save immediately after modification
        user_xp = await self.bot.load_all_json("user_xp")
        user_xp.setdefault(user_id, {"xp": 0})
        xp_earned = random.randint(5, 15)
        user_xp[user_id]["xp"] += xp_earned

        await self.bot.save_all_json("user_xp", user_xp)

        # Banned Words Check
        cleaned_content = message.content.lower().translate(str.maketrans('', '', string.punctuation))
        if any(word in config.banned_words for word in cleaned_content.split()):
            await message.delete()
            await message.channel.send(f'🚫 {message.author.mention}, that message contains a banned word!',
                                       delete_after=20)
            logger.info(f"Deleted message from {message.author.name} containing a banned word.")
            return

async def setup(bot):
    await bot.add_cog(AdminCommands(bot))