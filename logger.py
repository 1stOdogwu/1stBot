import logging
from logging.handlers import RotatingFileHandler
import discord
from discord.ext import commands
from discord.utils import setup_logging
import os

# Create the logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Create a custom log handler that saves to a file
handler = RotatingFileHandler(
    filename='logs/discord.log',
    encoding='utf-8',
    maxBytes=32 * 1024 * 1024,  # 32 MiB
    backupCount=5,
)

# Set up logging with the handler
setup_logging(handler=handler, root=False)

# Get the root logger for your bot's custom logs
bot_logger = logging.getLogger('my_bot')
bot_logger.setLevel(logging.INFO)  # Set the desired logging level

# Create a formatter for the bot's logger
log_format = logging.Formatter(
    fmt='[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Add the formatter to the handler
handler.setFormatter(log_format)

# Add the handler to the bot's logger
bot_logger.addHandler(handler)