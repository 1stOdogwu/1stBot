import discord
import re
from urllib.parse import urlparse, urlunparse

from logger import bot_logger as logger

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

async def manage_periodic_message(
        bot,
        channel,
        bot_data: dict,
        message_id_key: str,
        embed: discord.Embed,
        pin: bool = False
) -> None:
    """
    Manages a message updated periodically.

    Checks if a message with the given ID exists. If it does, edit it.
    If not, sends a new message, saves its ID, and optionally pins it.
    """
    try:
        message_id = bot_data.get(message_id_key)

        if message_id:
            try:
                message = await channel.fetch_message(message_id)
                await message.edit(embed=embed)
            except discord.NotFound:
                new_message = await channel.send(embed=embed)
                bot_data[message_id_key] = new_message.id
                bot.save_data("bot_data_table", bot_data)
                if pin:
                    await new_message.pin()
            except discord.Forbidden:
                logger.error(f"Bot missing permissions to edit/pin message in channel ({channel.id}).")
        else:
            new_message = await channel.send(embed=embed)
            bot_data[message_id_key] = new_message.id
            bot.save_data("bot_data_table", bot_data)
            if pin:
                await new_message.pin()

    except discord.Forbidden:
        logger.error(f"Bot missing permissions to send message in channel ({channel.id}).")
    except Exception as e:
        logger.error(f"An unexpected error occurred in manage_periodic_message: {e}")