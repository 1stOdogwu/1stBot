import os
import json
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

# Optional: your own logger
try:
    from logger import bot_logger as logger
except ImportError:
    import logging
    logger = logging.getLogger("bot")
    logging.basicConfig(level=logging.INFO)

# --- Global Executor for Database Operations ---
# We use a ThreadPoolExecutor to run blocking database calls
executor = ThreadPoolExecutor()

UTC = timezone.utc
DATABASE_URL = os.environ.get("DATABASE_URL")


# ------------------------------------------------
# 🔄 SYNCHRONOUS WORKER FUNCTIONS (for the executor)
# These functions MUST NOT use 'await'
# ------------------------------------------------
def _get_db_connection():
    """Establishes and returns a database connection (synchronous)."""
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL is not set in environment.")
        return None
    try:
        # Using RealDictCursor for fetches
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return None

def _init_db_sync():
    """Create all tables used by the generic store."""
    conn = _get_db_connection()
    if not conn:
        return
    try:
        cur = conn.cursor()

        # Generic KV store for your old JSON "files"
        cur.execute("""
            CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

        # Points transaction log
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                amount NUMERIC NOT NULL,
                purpose TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

        # Approved proofs (to prevent duplicates)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS approved_proofs (
                normalized_url TEXT PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

        # Processed reactions (idempotency)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_reactions (
                reaction_identifier TEXT PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

        # Mysterybox uses (for 24h limit checks)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mysterybox_uses (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                used_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

        conn.commit()
        cur.close()
        logger.info("✅ Database initialized.")
    except Exception as e:
        logger.error(f"❌ init_db failed: {e}")
    finally:
        if conn: conn.close()


def _save_data_sync(key: str, data):
    """Stores JSON in kv_store under the provided key."""
    conn = _get_db_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO kv_store (key, value, updated_at)
            VALUES (%s, %s::jsonb, NOW())
            ON CONFLICT (key) DO UPDATE
             SET value = EXCLUDED.value,
                 updated_at = NOW();
            """,
            (key, json.dumps(data))
        )
        conn.commit()
        cur.close()
        logger.info(f"💾 Saved key '{key}' to kv_store.")
    except Exception as e:
        logger.error(f"❌ _save_data_sync('{key}') failed: {e}")
    finally:
        if conn: conn.close()

def _load_data_sync(key: str, default_value=None):
    """Loads JSON from kv_store by key."""
    conn = _get_db_connection()
    if not conn: return default_value
    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM kv_store WHERE key = %s;", (key,))
        row = cur.fetchone()
        cur.close()
        if row and row['value'] is not None:
            return row['value']
        return default_value
    except Exception as e:
        logger.error(f"❌ _load_data_sync('{key}') failed: {e}")
        return default_value
    finally:
        if conn: conn.close()

def _log_points_transaction_sync(user_id: str, amount: float, purpose: str = None):
    """Logs a transaction to the transactions table."""
    conn = _get_db_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO transactions (user_id, amount, purpose) VALUES (%s, %s, %s);",
            (user_id, amount, purpose)
        )
        conn.commit()
        cur.close()
        logger.info(f"🧾 Logged transaction: user={user_id}, amount={amount:.2f}, purpose={purpose}")
    except Exception as e:
        logger.error(f"❌ _log_points_transaction_sync failed: {e}")
    finally:
        if conn: conn.close()

def _approved_proof_exists_sync(normalized_url: str) -> bool:
    """Checks if a proof exists in approved_proofs table."""
    conn = _get_db_connection()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM approved_proofs WHERE normalized_url = %s;", (normalized_url,))
        exists = cur.fetchone() is not None
        cur.close()
        return exists
    except Exception as e:
        logger.error(f"❌ _approved_proof_exists_sync failed: {e}")
        return False
    finally:
        if conn: conn.close()

def _add_approved_proof_sync(normalized_url: str) -> bool:
    """Adds a proof to the approved_proofs table."""
    conn = _get_db_connection()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO approved_proofs (normalized_url)
            VALUES (%s)
            ON CONFLICT (normalized_url) DO NOTHING;
            """,
            (normalized_url,)
        )
        inserted = cur.rowcount > 0
        conn.commit()
        cur.close()
        return inserted
    except Exception as e:
        logger.error(f"❌ _add_approved_proof_sync failed: {e}")
        return False
    finally:
        if conn: conn.close()


def _add_processed_reaction_if_new_sync(reaction_identifier: str) -> bool:
    """Adds a reaction to processed_reactions table if new."""
    conn = _get_db_connection()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO processed_reactions (reaction_identifier)
            VALUES (%s)
            ON CONFLICT (reaction_identifier) DO NOTHING;
            """,
            (reaction_identifier,)
        )
        inserted = cur.rowcount > 0
        conn.commit()
        cur.close()
        return inserted
    except Exception as e:
        logger.error(f"❌ _add_processed_reaction_if_new_sync failed: {e}")
        return False
    finally:
        if conn: conn.close()


def _mb_add_use_sync(user_id: str):
    """Logs a mysterybox use."""
    conn = _get_db_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO mysterybox_uses (user_id) VALUES (%s);", (user_id,))
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error(f"❌ _mb_add_use_sync failed: {e}")
    finally:
        if conn: conn.close()


def _mb_get_uses_in_last_24h_sync(user_id: str) -> int:
    """Gets the count of mysterybox uses for a user in the last 24h."""
    conn = _get_db_connection()
    if not conn: return 0
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) FROM mysterybox_uses
            WHERE user_id = %s AND used_at >= (NOW() - INTERVAL '24 hours');
            """,
            (user_id,)
        )
        count = cur.fetchone()['count']
        cur.close()
        return int(count)
    except Exception as e:
        logger.error(f"❌ _mb_get_uses_in_last_24h_sync failed: {e}")
        return 0
    finally:
        if conn: conn.close()


# --------------------------------------------------
# ✅ ASYNC WRAPPER FUNCTIONS (for your bot's cogs)
# These functions should be used with 'await'
# --------------------------------------------------
async def init_db(bot):
    """Initializes the database asynchronously."""
    await bot.loop.run_in_executor(executor, _init_db_sync)

async def save_data(bot, key: str, data):
    """Stores JSON data in the kv_store asynchronously."""
    await bot.loop.run_in_executor(executor, _save_data_sync, key, data)

async def load_data(bot, key: str, default_value=None):
    """Loads JSON data from the kv_store asynchronously."""
    return await bot.loop.run_in_executor(executor, _load_data_sync, key, default_value)

async def log_points_transaction(bot, user_id: str, amount: float, purpose: str = None):
    """Logs a transaction to the transactions table asynchronously."""
    await bot.loop.run_in_executor(executor, _log_points_transaction_sync, user_id, amount, purpose)

async def approved_proof_exists(bot, normalized_url: str) -> bool:
    """Checks if a proof exists asynchronously."""
    return await bot.loop.run_in_executor(executor, _approved_proof_exists_sync, normalized_url)

async def add_approved_proof(bot, normalized_url: str) -> bool:
    """Adds a proof to the approved_proofs table asynchronously."""
    return await bot.loop.run_in_executor(executor, _add_approved_proof_sync, normalized_url)

async def add_processed_reaction_if_new(bot, reaction_identifier: str) -> bool:
    """Adds a reaction to processed_reactions table if new asynchronously."""
    return await bot.loop.run_in_executor(executor, _add_processed_reaction_if_new_sync, reaction_identifier)

async def mb_add_use(bot, user_id: str):
    """Logs a mysterybox use asynchronously."""
    await bot.loop.run_in_executor(executor, _mb_add_use_sync, user_id)

async def mb_get_uses_in_last_24h(bot, user_id: str) -> int:
    """Gets the count of mysterybox uses for a user in the last 24h asynchronously."""
    return await bot.loop.run_in_executor(executor, _mb_get_uses_in_last_24h_sync, user_id)