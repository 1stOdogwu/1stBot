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
