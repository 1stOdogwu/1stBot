import discord
from discord.ext import commands, tasks
from logger import bot_logger as logger
import config
from datetime import datetime, UTC
import random
import string

class TasksCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    def cog_load(self):
        """Starts all background tasks when the cog is loaded."""
        logger.info("Starting background tasks...")
        # ✅ Start all tasks here
        self.update_economy_message.start()
        self.update_leaderboards.start()
        self.weekly_xp_bonus.start()
        self.update_giveaway_winners_history.start()
        self.reset_vip_posts.start()
        logger.info("All background tasks started.")

    def cog_unload(self):
        """Cancels all background tasks when the cog is unloaded."""
        logger.info("Cancelling background tasks...")
        self.update_economy_message.cancel()
        self.update_leaderboards.cancel()
        self.weekly_xp_bonus.cancel()
        self.update_giveaway_winners_history.cancel()
        self.reset_vip_posts.cancel()


    # --- T H E      E C O N O M Y      M E S S A G E       L O O P ---
    @tasks.loop(minutes=5)
    async def update_economy_message(self):
        """Periodically updates the economy status message in a dedicated channel."""
        await self.bot.wait_until_ready()
        commands_cog = self.bot.get_cog("AdminCommands")
        if not commands_cog:
            logger.error("❌ AdminCommands cog not found. Cannot generate economy embed.")
            return

        try:
            channel = self.bot.get_channel(config.FIRST_ODOGWU_CHANNEL_ID)
            if not channel:
                logger.error(f"❌ Error: Economy updates channel (ID: {config.FIRST_ODOGWU_CHANNEL_ID}) not found.")
                return

            economy_embed = await commands_cog.get_economy_embed()
            economy_message_id = self.bot.bot_data.get("economy_message_id")

            if economy_message_id:
                try:
                    message = await channel.fetch_message(economy_message_id)
                    await message.edit(embed=economy_embed)
                    logger.info("✅ Economy message updated successfully.")
                except discord.NotFound:
                    message = await channel.send(embed=economy_embed)
                    self.bot.bot_data["economy_message_id"] = message.id
                    logger.info("✅ Old economy message not found. A new one has been sent.")
                except discord.Forbidden:
                    logger.error(
                        f"❌ Bot missing permissions to edit message in channel ({config.FIRST_ODOGWU_CHANNEL_ID}).")
            else:
                message = await channel.send(embed=economy_embed)
                self.bot.bot_data["economy_message_id"] = message.id
                logger.info("✅ New economy message sent and its ID saved.")

            # ✅ Await the save_data call
            await self.bot.save_data("bot_data", self.bot.bot_data)
        except discord.Forbidden:
            logger.error(f"❌ Bot missing permissions to send messages in channel ({config.FIRST_ODOGWU_CHANNEL_ID}).")
        except Exception as e:
            logger.error(f"❌ An unexpected error occurred in the economy update task: {e}", exc_info=True)

    # --- A L L      L E A D E R B O A R D      M E S S A G E       L O O P ---
    @tasks.loop(minutes=5)
    async def update_leaderboards(self):
        """Periodically updates the three leaderboard messages."""
        await self.bot.wait_until_ready()
        try:
            commands_cog = self.bot.get_cog("AdminCommands")
            if not commands_cog:
                logger.error("❌ AdminCommands cog not found.")
                return

            channel = self.bot.get_channel(config.PERIODIC_LEADERBOARD_CHANNEL_ID)
            if not channel:
                logger.error(
                    f"❌ Error: Leaderboard channel not found (ID: {config.PERIODIC_LEADERBOARD_CHANNEL_ID}).")
                return

            # Load the latest bot_data from the database before performing any operations
            self.bot.bot_data = await self.bot.load_data(self.bot, "bot_data_table", {})

            await self.bot.manage_periodic_message(
                channel=channel,
                bot_data=self.bot.bot_data,
                message_id_key="points_leaderboard_message_id",
                embed=await commands_cog.get_points_leaderboard_embed(),
                pin=True
            )
            await self.bot.manage_periodic_message(
                channel=channel,
                bot_data=self.bot.bot_data,
                message_id_key="referral_leaderboard_message_id",
                embed=await commands_cog.get_referral_leaderboard_embed(),
                pin=True
            )
            await self.bot.manage_periodic_message(
                channel=channel,
                bot_data=self.bot.bot_data,
                message_id_key="xp_leaderboard_message_id",
                embed=await commands_cog.get_xp_leaderboard_embed(),
                pin=True
            )

            # ✅ FIX: Use the consistent database key "bot_data_table"
            await self.bot.save_data(self.bot, "bot_data_table", self.bot.bot_data)
            logger.info("✅ All leaderboards updated successfully.")
        except discord.Forbidden:
            logger.error("Bot is missing permissions to send, edit, or pin messages in the leaderboard channel.")
        except Exception as e:
            logger.error(f"❌ An unexpected error occurred in the leaderboard update task: {e}")

    # --- W E E K L Y       X P        B O N U S       L  O O P ---
    @tasks.loop(hours=168)
    async def weekly_xp_bonus(self):
        """Awards bonus points to top 3 XP earners weekly."""
        await self.bot.wait_until_ready()
        logger.info("Starting weekly XP bonus award.")
        guild = self.bot.get_guild(config.SERVER_ID)
        if not guild:
            logger.error("Error: Server not found. Cannot award weekly XP bonus.")
            return

        # ✅ FIX: Load the user points and XP data from the database
        users_points = await self.bot.load_data(self.bot, "users_points", {})
        user_xp = await self.bot.load_data(self.bot, "user_xp", {})
        admin_points = await self.bot.load_data(self.bot, "admin_points", {})

        eligible = {}
        allowed_roles = [config.ADMIN_ROLE_ID, config.MOD_ROLE_ID]
        # ✅ FIX: Use the loaded user_xp dictionary
        for uid, data in user_xp.items():
            xp_val = data.get("xp", 0) if isinstance(data, dict) else data
            if xp_val >= 500:
                member = guild.get_member(int(uid))
                if member and not any(role.id in allowed_roles for role in member.roles):
                    eligible[uid] = xp_val
        top_users = sorted(eligible.items(), key=lambda x: x[1], reverse=True)[:3]

        if not top_users:
            logger.info("No eligible users for weekly XP bonus this week.")
            return

        points_to_award_per_user = 200
        total_points_to_award = len(top_users) * points_to_award_per_user

        # ✅ FIX: Use the loaded admin_points dictionary
        if admin_points.get("balance", 0) < total_points_to_award:
            logger.warning("⚠️ Admin balance is too low to award weekly XP bonus. Skipping.")
            return

        commands_cog = self.bot.get_cog("AdminCommands")
        if not commands_cog:
            logger.error("❌ AdminCommands cog not found. Cannot log transactions.")
            return

        for uid, _ in top_users:
            user_id = str(uid)
            # ✅ FIX: Modify the loaded users_points dictionary
            users_points.setdefault(user_id, {"all_time_points": 0.0, "available_points": 0.0})
            users_points[user_id]["all_time_points"] += points_to_award_per_user
            users_points[user_id]["available_points"] += points_to_award_per_user
            await commands_cog.log_points_transaction(user_id, float(points_to_award_per_user), "Weekly XP bonus")

        # ✅ FIX: Modify the loaded admin_points and save all data
        admin_points["balance"] -= total_points_to_award
        admin_points["in_circulation"] += total_points_to_award

        await self.bot.save_data(self.bot, "users_points", users_points)
        await self.bot.save_data(self.bot, "admin_points", admin_points)

        reward_channel = self.bot.get_channel(config.XP_REWARD_CHANNEL_ID)
        if reward_channel:
            mentions = []
            for uid, _ in top_users:
                try:
                    user = await self.bot.fetch_user(int(uid))
                    mentions.append(user.mention)
                except (discord.NotFound, discord.HTTPException):
                    mentions.append(f"Unknown User (ID: {uid})")
            if mentions:
                await reward_channel.send(f"🔥 Congrats to Mana XP legends: {', '.join(mentions)} 🎉")
            embed = discord.Embed(
                title="🏆 Weekly XP Rewards",
                description="The **Top 3 XP Earners** of the week have been awarded their bonus! 🎉",
                color=discord.Color.gold()
            )
            for idx, (uid, _) in enumerate(top_users, 1):
                try:
                    user = await self.bot.fetch_user(int(uid))
                    embed.add_field(
                        name=f"⭐ Rank #{idx}",
                        value=f"{user.mention} — **+{points_to_award_per_user:.2f} points**",
                        inline=False
                    )
                except (discord.NotFound, discord.HTTPException):
                    embed.add_field(
                        name=f"⭐ Rank #{idx}",
                        value=f"Unknown User (ID: {uid}) — **+{points_to_award_per_user:.2f} points**",
                        inline=False
                    )
            embed.set_footer(text="Keep chatting, questing, and engaging to climb the ranks! 🚀")
            embed.timestamp = datetime.now(UTC)
            try:
                await reward_channel.send(embed=embed)
            except discord.Forbidden:
                logger.error(
                    f"Bot missing permissions to send message to XP reward channel ({config.XP_REWARD_CHANNEL_ID}).")
        else:
            logger.error(f"XP Reward Channel (ID: {config.XP_REWARD_CHANNEL_ID}) not found.")
        logger.info("Weekly XP bonus awarded.")

    # --- G I V E A W A Y     W I N N E R S     H I S T O R Y     U P D A T E      L O O P ---
    @tasks.loop(hours=168)
    async def update_giveaway_winners_history(self):
        """
        Updates the all-time giveaway winners history message and clears the weekly log.
        """
        await self.bot.wait_until_ready()

        # ✅ FIX: Load both giveaway logs from the database
        giveaway_winners_log = await self.bot.load_data(self.bot, "giveaway_logs", [])
        all_time_giveaway_winners_log = await self.bot.load_data(self.bot, "all_time_giveaway_logs", [])

        if not giveaway_winners_log:
            logger.info("No new giveaway winners to update. Skipping.")
            return

        channel = self.bot.get_channel(config.GIVEAWAY_CHANNEL_ID)
        if not channel:
            logger.error(f"❌ Error: Giveaway channel with ID {config.GIVEAWAY_CHANNEL_ID} not found.")
            return

        # ✅ FIX: Append the new winners to the all-time log
        all_time_giveaway_winners_log.extend(giveaway_winners_log)

        embed = discord.Embed(
            title="🎉 All-Time Giveaway Winners 🎉",
            description="Here’s the full hall of fame for all giveaways so far 🏆",
            color=discord.Color.gold()
        )
        # ✅ FIX: Use the loaded all_time_giveaway_winners_log
        for winner in all_time_giveaway_winners_log:
            user = self.bot.get_user(int(winner['user_id']))
            user_name = user.mention if user else f"User ID: {winner['user_id']}"
            embed.add_field(
                name=f"✨ {user_name}",
                value=f"**{winner['points']:.2f} points** 🎁\n*Reason:* {winner['purpose']}",
                inline=False
            )
        embed.set_footer(text="Updated automatically as giveaways happen 🚀")
        embed.timestamp = datetime.now(UTC)

        # ✅ FIX: Correct manage_periodic_message call
        await self.bot.manage_periodic_message(
            channel=channel,
            bot_data=self.bot.bot_data,
            message_id_key="giveaway_history_message_id",
            embed=embed,
            pin=False
        )

        # ✅ FIX: Clear the weekly log and save both to the database
        giveaway_winners_log.clear()
        await self.bot.save_data(self.bot, "giveaway_logs", giveaway_winners_log)
        await self.bot.save_data(self.bot, "all_time_giveaway_logs", all_time_giveaway_winners_log)
        await self.bot.save_data(self.bot, "bot_data", self.bot.bot_data)
        logger.info("✅ Giveaway history updated and temporary log cleared.")

    # --- V I P      P O S T      R E S E T       L O O P ---
    @tasks.loop(hours=24)
    async def reset_vip_posts(self):
        """Resets the daily VIP post-limit."""
        await self.bot.wait_until_ready()
        try:
            vip_posts = {}
            await self.bot.save_data(self.bot, "vip_posts", vip_posts)
            logger.info("🔄 VIP post limit reset.")
        except Exception as e:
            logger.error(f"❌ An error occurred during the VIP post reset task: {e}")

# The setup function is required for the cog to be loaded by the bot
async def setup(bot):
    await bot.add_cog(TasksCog(bot))