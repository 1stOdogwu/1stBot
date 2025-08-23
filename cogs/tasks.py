import discord
from discord.ext import commands, tasks
from logger import bot_logger as logger
from database import save_data, load_data
import config
from datetime import datetime, UTC

class TasksCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    # This method runs automatically when the cog is loaded
    def cog_load(self):
        """Starts all background tasks when the cog is loaded."""
        logger.info("Starting background tasks...")
        # ✅ We start the save task here
        self.save_all_data_task.start()
        # You can start other tasks here as well, e.g.,
        # self.weekly_xp_bonus.start()

    # This method runs automatically when the cog is unloaded
    def cog_unload(self):
        """Cancels all background tasks when the cog is unloaded."""
        logger.info("Cancelling background tasks...")
        self.save_all_data_task.cancel()
        # self.weekly_xp_bonus.cancel()

    # The periodic task that saves all your data
    @tasks.loop(seconds=5)
    async def save_all_data_task(self):
        """Periodically saves all bot data to the database."""
        await self.bot.wait_until_ready()
        logger.info("Starting periodic data save...")

        # This is the single, clean call to the helper function
        await self._save_all_data_to_db()

    # The helper function that does the actual saving
    async def _save_all_data_to_db(self):
        """Saves all bot data to the database."""
        try:
            await self.bot.save_data("points_history_table", self.bot.points_history)
            await self.bot.save_data("giveaway_logs_table", self.bot.giveaway_winners_log)
            await self.bot.save_data("all_time_giveaway_logs_table", self.bot.all_time_giveaway_winners_log)
            await self.bot.save_data("gm_log_table", self.bot.gm_log)
            await self.bot.save_data("user_points_table", self.bot.user_points)
            await self.bot.save_data("admin_points_table", self.bot.admin_points)
            await self.bot.save_data("submissions_table", self.bot.submissions)
            await self.bot.save_data("quest_submissions_table", self.bot.quest_submissions)
            await self.bot.save_data("weekly_quests_table", self.bot.weekly_quests)
            await self.bot.save_data("referral_data_table", self.bot.referral_data)
            await self.bot.save_data("approved_proofs_table", self.bot.approved_proofs)
            await self.bot.save_data("user_xp_table", self.bot.user_xp)
            await self.bot.save_data("bot_data_table", self.bot.bot_data)
            await self.bot.save_data("vip_posts_table", self.bot.vip_posts)
            await self.bot.save_data("pending_referrals_table", self.bot.pending_referrals)
            await self.bot.save_data("active_tickets_table", self.bot.active_tickets)
            await self.bot.save_data("mysterybox_uses_table", self.bot.mysterybox_uses)
            await self.bot.save_data("processed_reactions_table", list(self.bot.processed_reactions))
            await self.bot.save_data("referred_users_table", list(self.bot.referred_users))

            logger.info("✅ All bot data saved successfully.")
        except Exception as e:
            logger.error(f"❌ An error occurred during the periodic save task: {e}")


    #  T H E      E C O N O M Y      M E S S A G E       L O O P
    @tasks.loop(minutes=5)
    async def update_economy_message(self):
        """Periodically updates the economy status message in a dedicated channel."""
        await self.bot.wait_until_ready()

        # Get the cog that has the get_economy_embed function
        commands_cog = self.bot.get_cog("AdminCommands")
        if not commands_cog:
            logger.error("❌ AdminCommands cog not found. Cannot generate economy embed.")
            return

        try:
            channel = self.bot.get_channel(config.FIRST_ODOGWU_CHANNEL_ID)
            if not channel:
                logger.error(f"❌ Error: Economy updates channel (ID: {config.FIRST_ODOGWU_CHANNEL_ID}) not found.")
                return

            # Get the embed from the AdminCommands cog
            economy_embed = await commands_cog.get_economy_embed()

            # Access the message ID from the bot's data
            economy_message_id = self.bot.bot_data.get("economy_message_id")

            if economy_message_id:
                try:
                    message = await channel.fetch_message(economy_message_id)
                    await message.edit(embed=economy_embed)
                    logger.info("✅ Economy message updated successfully.")
                except discord.NotFound:
                    # If the message was deleted, send a new one and save its ID
                    message = await channel.send(embed=economy_embed)
                    self.bot.bot_data["economy_message_id"] = message.id
                    logger.info("✅ Old economy message not found. A new one has been sent.")
                except discord.Forbidden:
                    logger.error(
                        f"❌ Bot missing permissions to edit message in channel ({config.FIRST_ODOGWU_CHANNEL_ID}).")
            else:
                # If there is no existing message ID, send a new message and save its ID
                message = await channel.send(embed=economy_embed)
                self.bot.bot_data["economy_message_id"] = message.id
                logger.info("✅ New economy message sent and its ID saved.")

            # We need to save the data immediately after updating the message ID
            self.bot.save_data("bot_data_table", self.bot.bot_data)

        except discord.Forbidden:
            logger.error(f"❌ Bot missing permissions to send messages in channel ({config.FIRST_ODOGWU_CHANNEL_ID}).")
        except Exception as e:
            logger.error(f"❌ An unexpected error occurred in the economy update task: {e}", exc_info=True)

    #  A L L      L E A D E R B O A R D      M E S S A G E       L O O P
    @tasks.loop(minutes=30)
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

            # Use the new utility function for each leaderboard
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

            self.bot.save_data("bot_data_table", self.bot.bot_data)
            logger.info("✅ All leaderboards updated successfully.")

        except discord.Forbidden:
            logger.error("Bot is missing permissions to send, edit, or pin messages in the leaderboard channel.")
        except Exception as e:
            logger.error(f"❌ An unexpected error occurred in the leaderboard update task: {e}")
    # === W E E K L Y       X P        B O N U S       L  O O P ===
    @tasks.loop(hours=168)
    async def weekly_xp_bonus(self):
        """Awards bonus points to top 3 XP earners weekly."""
        await self.bot.wait_until_ready()

        logger.info("Starting weekly XP bonus award.")

        guild = self.bot.get_guild(self.bot.SERVER_ID)
        if not guild:
            logger.error("Error: Server not found. Cannot award weekly XP bonus.")
            return

        eligible = {}
        allowed_roles = [self.bot.ADMIN_ROLE_ID, self.bot.MOD_ROLE_ID]
        for uid, data in self.bot.user_xp.items():
            xp_val = data.get("xp", 0) if isinstance(data, dict) else data
            if xp_val >= 500:
                member = guild.get_member(int(uid))
                if member and not any(role.id in allowed_roles for role in member.roles):
                    eligible[uid] = xp_val

        top_users = sorted(eligible.items(), key=lambda x: x[1], reverse=True)[:3]

        if not top_users:
            logger.info("No eligible users for weekly XP bonus this week.")
            return

        points_to_award = len(top_users) * 200
        if self.bot.admin_points.get("balance", 0) < points_to_award:
            logger.warning("⚠️ Admin balance is too low to award weekly XP bonus. Skipping.")
            return

        commands_cog = self.bot.get_cog("AdminCommands")
        if not commands_cog:
            logger.error("❌ AdminCommands cog not found. Cannot log transactions.")
            return

        for uid, _ in top_users:
            user_id = str(uid)
            if user_id not in self.bot.user_points:
                self.bot.user_points[user_id] = {"all_time_points": 0.0, "available_points": 0.0}

            self.bot.user_points[user_id]["all_time_points"] += 200
            self.bot.user_points[user_id]["available_points"] += 200

            await commands_cog.log_points_transaction(user_id, 200.0, "Weekly XP bonus")

        self.bot.admin_points["balance"] -= points_to_award
        self.bot.admin_points["in_circulation"] += points_to_award

        self.bot.save_data("user_points_table", self.bot.user_points)
        self.bot.save_data("admin_points_table", self.bot.admin_points)

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

            try:
                await reward_channel.send(embed=embed)
            except discord.Forbidden:
                logger.error(
                    f"Bot missing permissions to send message to XP reward channel ({config.XP_REWARD_CHANNEL_ID}).")
        else:
            logger.error(f"XP Reward Channel (ID: {config.XP_REWARD_CHANNEL_ID}) not found.")

        logger.info("Weekly XP bonus awarded.")

    #  G I V E A W A Y     W I N N E R S     H I S T O R Y     U P D A T E      L O O P
    @tasks.loop(hours=168)
    async def update_giveaway_winners_history(self):
        """
        Updates the all-time giveaway winners history message and clears the weekly log.
        """
        await self.bot.wait_until_ready()

        if not self.bot.giveaway_winners_log:
            logger.info("No new giveaway winners to update. Skipping.")
            return

        channel = self.bot.get_channel(config.GIVEAWAY_CHANNEL_ID)
        if not channel:
            logger.error(f"❌ Error: Giveaway channel with ID {config.GIVEAWAY_CHANNEL_ID} not found.")
            return

        embed = discord.Embed(
            title="🎉 All-Time Giveaway Winners 🎉",
            description="Here’s the full hall of fame for all giveaways so far 🏆",
            color=discord.Color.gold()
        )

        for winner in self.bot.all_time_giveaway_winners_log:
            user = self.bot.get_user(int(winner['user_id']))
            user_name = user.mention if user else f"User ID: {winner['user_id']}"
            embed.add_field(
                name=f"✨ {user_name}",
                value=f"**{winner['points']:.2f} points** 🎁\n*Reason:* {winner['purpose']}",
                inline=False
            )

        embed.set_footer(text="Updated automatically as giveaways happen 🚀")
        embed.timestamp = datetime.now(UTC)

        # Replaced the original message management logic
        await self.bot.manage_periodic_message(
            bot=self.bot,
            channel=channel,
            bot_data=self.bot.bot_data,
            message_id_key="giveaway_history_message_id",
            embed=embed,
            pin=False
        )

        self.bot.giveaway_winners_log.clear()
        self.bot.save_data("giveaway_logs_table", self.bot.giveaway_winners_log)
        self.bot.save_data("all_time_giveaway_logs_table", self.bot.all_time_giveaway_winners_log)
        self.bot.save_data("bot_data_table", self.bot.bot_data)

        logger.info("✅ Giveaway history updated and temporary log cleared.")

    #  V I P      P O S T      R E S E T       L O O P
    @tasks.loop(hours=24)
    async def reset_vip_posts(self):
        """Resets the daily VIP post-limit."""
        await self.bot.wait_until_ready()

        try:
            self.bot.vip_posts = {}
            self.bot.save_data("vip_posts_table", self.bot.vip_posts)
            logger.info("🔄 VIP post limit reset.")
        except Exception as e:
            logger.error(f"❌ An error occurred during the VIP post reset task: {e}")


# The setup function is required for the cog to be loaded by the bot
async def setup(bot):
    await bot.add_cog(TasksCog(bot))