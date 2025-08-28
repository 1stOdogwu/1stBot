import os
import json
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, UTC

try:
    from logger import bot_logger as logger
except ImportError:
    import logging

    logger = logging.getLogger("bot")
    logging.basicConfig(level=logging.INFO)

executor = ThreadPoolExecutor()

DATABASE_URL = os.environ.get("DATABASE_URL")


def _get_db_connection():
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL is not set.")
        return None
    try:
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return None


def _init_db_sync():
    conn = _get_db_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS bot_data
                    (
                        key  TEXT PRIMARY KEY,
                        data JSONB
                    );
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS admin_points
                    (
                        key  TEXT PRIMARY KEY,
                        data JSONB
                    );
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS weekly_quests
                    (
                        key  TEXT PRIMARY KEY,
                        data JSONB
                    );
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS user_points
                    (
                        user_id TEXT PRIMARY KEY,
                        data    JSONB
                    );
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS user_xp
                    (
                        user_id TEXT PRIMARY KEY,
                        data    JSONB
                    );
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS referral_data
                    (
                        user_id TEXT PRIMARY KEY,
                        data    JSONB
                    );
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS pending_referrals
                    (
                        user_id TEXT PRIMARY KEY,
                        data    JSONB
                    );
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS gm_log
                    (
                        user_id TEXT PRIMARY KEY,
                        data    JSONB
                    );
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS quest_submissions
                    (
                        user_id TEXT PRIMARY KEY,
                        data    JSONB
                    );
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS submissions
                    (
                        user_id TEXT PRIMARY KEY,
                        data    JSONB
                    );
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS vip_posts
                    (
                        key  TEXT PRIMARY KEY,
                        data JSONB
                    );
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS active_tickets
                    (
                        channel_id TEXT PRIMARY KEY,
                        user_id    TEXT NOT NULL
                    );
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS all_time_giveaway_logs
                    (
                        id   BIGSERIAL PRIMARY KEY,
                        data JSONB
                    );
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS giveaway_logs
                    (
                        id   BIGSERIAL PRIMARY KEY,
                        data JSONB
                    );
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS points_history
                    (
                        id   BIGSERIAL PRIMARY KEY,
                        data JSONB
                    );
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS approved_proofs
                    (
                        normalized_url TEXT PRIMARY KEY
                    );
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS processed_reactions
                    (
                        reaction_identifier TEXT PRIMARY KEY
                    );
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS mysterybox_uses
                    (
                        id      BIGSERIAL PRIMARY KEY,
                        user_id TEXT        NOT NULL,
                        used_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS referred_users
                    (
                        user_id TEXT PRIMARY KEY
                    );
                    """)

        conn.commit()
        cur.close()
        logger.info("✅ Database initialized.")
    except Exception as e:
        logger.error(f"❌ init_db failed: {e}")
    finally:
        if conn: conn.close()


def _save_single_json_sync(table_name: str, key: str, data: dict):
    conn = _get_db_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        pk_column = 'user_id' if table_name in ['user_points', 'user_xp', 'referral_data', 'pending_referrals',
                                                'gm_log', 'quest_submissions', 'submissions'] else 'key'
        query = sql.SQL("""
                        INSERT INTO {table} ({pk_column}, data)
                        VALUES (%s, %s) ON CONFLICT ({pk_column})
                        DO UPDATE
                        SET data = EXCLUDED.data;
                        """).format(table=sql.Identifier(table_name), pk_column=sql.Identifier(pk_column))
        cur.execute(query, (key, json.dumps(data)))
        conn.commit()
        cur.close()
        logger.info(f"✅ Data saved to '{table_name}' with key '{key}'.")
    except Exception as e:
        logger.error(f"❌ _save_single_json_sync to '{table_name}' failed: {e}")
    finally:
        if conn: conn.close()


def _load_single_json_sync(table_name: str, key: str, default_value=None):
    conn = _get_db_connection()
    if not conn: return default_value
    try:
        cur = conn.cursor()
        pk_column = 'user_id' if table_name in ['user_points', 'user_xp', 'referral_data', 'pending_referrals',
                                                'gm_log', 'quest_submissions', 'submissions'] else 'key'
        query = sql.SQL("SELECT data FROM {table} WHERE {pk_column} = %s;").format(
            table=sql.Identifier(table_name), pk_column=sql.Identifier(pk_column)
        )
        cur.execute(query, (key,))
        row = cur.fetchone()
        cur.close()
        if row and row['data'] is not None:
            return row['data']
        return default_value
    except Exception as e:
        logger.error(f"❌ _load_single_json_sync from '{table_name}' failed: {e}")
        return default_value
    finally:
        if conn: conn.close()


def _save_all_json_sync(table_name: str, data_dict: dict):
    conn = _get_db_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        pk_column = 'user_id' if table_name in ['user_points', 'user_xp', 'referral_data', 'pending_referrals',
                                                'gm_log', 'quest_submissions', 'submissions'] else 'key'

        delete_query = sql.SQL("DELETE FROM {table};").format(table=sql.Identifier(table_name))
        cur.execute(delete_query)

        if data_dict:
            insert_query = sql.SQL("""
                                   INSERT INTO {table} ({pk_column}, data)
                                   VALUES (%s, %s);
                                   """).format(table=sql.Identifier(table_name), pk_column=sql.Identifier(pk_column))

            records_to_insert = [
                (key, json.dumps(value)) for key, value in data_dict.items()
            ]

            psycopg2.extras.execute_batch(cur, insert_query, records_to_insert)

        conn.commit()
        cur.close()
        logger.info(f"✅ All data saved to '{table_name}'.")
    except Exception as e:
        logger.error(f"❌ _save_all_json_sync to '{table_name}' failed: {e}")
    finally:
        if conn: conn.close()


def _load_all_json_sync(table_name: str):
    conn = _get_db_connection()
    if not conn: return {}
    try:
        cur = conn.cursor()
        pk_column = 'user_id' if table_name in ['user_points', 'user_xp', 'referral_data', 'pending_referrals',
                                                'gm_log', 'quest_submissions', 'submissions'] else 'key'
        query = sql.SQL("SELECT {pk_column}, data FROM {table};").format(
            table=sql.Identifier(table_name), pk_column=sql.Identifier(pk_column)
        )
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        data_dict = {row[pk_column]: row['data'] for row in rows}
        return data_dict
    except Exception as e:
        logger.error(f"❌ _load_all_json_sync from '{table_name}' failed: {e}")
        return {}
    finally:
        if conn: conn.close()


def _save_list_values_sync(table_name: str, data_list: list, column_name: str):
    conn = _get_db_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        delete_query = sql.SQL("DELETE FROM {table};").format(table=sql.Identifier(table_name))
        cur.execute(delete_query)

        if data_list:
            insert_query = sql.SQL("INSERT INTO {table} ({column_name}) VALUES (%s);").format(
                table=sql.Identifier(table_name),
                column_name=sql.Identifier(column_name)
            )
            records_to_insert = [(item,) for item in data_list]
            psycopg2.extras.execute_batch(cur, insert_query, records_to_insert)

        conn.commit()
        cur.close()
        logger.info(f"✅ List data saved to '{table_name}'.")
    except Exception as e:
        logger.error(f"❌ _save_list_values_sync to '{table_name}' failed: {e}")
    finally:
        if conn: conn.close()


def _load_list_values_sync(table_name: str, column_name: str):
    conn = _get_db_connection()
    if not conn: return []
    try:
        cur = conn.cursor()
        query = sql.SQL("SELECT {column_name} FROM {table};").format(
            column_name=sql.Identifier(column_name),
            table=sql.Identifier(table_name)
        )
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return [row[column_name] for row in rows]
    except Exception as e:
        logger.error(f"❌ _load_list_values_sync from '{table_name}' failed: {e}")
        return []
    finally:
        if conn: conn.close()


def _save_list_of_json_sync(table_name: str, data_list: list):
    conn = _get_db_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        delete_query = sql.SQL("DELETE FROM {table};").format(table=sql.Identifier(table_name))
        cur.execute(delete_query)

        if data_list:
            insert_query = sql.SQL("""
                                   INSERT INTO {table} (data)
                                   VALUES (%s);
                                   """).format(table=sql.Identifier(table_name))

            records_to_insert = [
                (json.dumps(item),) for item in data_list
            ]

            psycopg2.extras.execute_batch(cur, insert_query, records_to_insert)

        conn.commit()
        cur.close()
        logger.info(f"✅ List of JSON data saved to '{table_name}'.")
    except Exception as e:
        logger.error(f"❌ _save_list_of_json_sync to '{table_name}' failed: {e}")
    finally:
        if conn: conn.close()


def _load_list_of_json_sync(table_name: str):
    conn = _get_db_connection()
    if not conn: return []
    try:
        cur = conn.cursor()
        query = sql.SQL("SELECT data FROM {table};").format(table=sql.Identifier(table_name))
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return [row['data'] for row in rows]
    except Exception as e:
        logger.error(f"❌ _load_list_of_json_sync from '{table_name}' failed: {e}")
        return []
    finally:
        if conn: conn.close()


async def init_db(bot):
    await bot.loop.run_in_executor(executor, _init_db_sync)


async def load_single_json(bot, table_name: str, key: str, default_value=None):
    return await bot.loop.run_in_executor(executor, _load_single_json_sync, table_name, key, default_value)


async def save_single_json(bot, table_name: str, key: str, data):
    await bot.loop.run_in_executor(executor, _save_single_json_sync, table_name, key, data)


async def load_all_json(bot, table_name: str):
    return await bot.loop.run_in_executor(executor, _load_all_json_sync, table_name)


async def save_all_json(bot, table_name: str, data_dict: dict):
    await bot.loop.run_in_executor(executor, _save_all_json_sync, table_name, data_dict)


async def load_list_values(bot, table_name: str, column_name: str):
    return await bot.loop.run_in_executor(executor, _load_list_values_sync, table_name, column_name)


async def save_list_values(bot, table_name: str, data_list: list, column_name: str):
    await bot.loop.run_in_executor(executor, _save_list_values_sync, table_name, data_list, column_name)


async def save_list_of_json(bot, table_name: str, data_list: list):
    await bot.loop.run_in_executor(executor, _save_list_of_json_sync, table_name, data_list)


async def load_list_of_json(bot, table_name: str):
    return await bot.loop.run_in_executor(executor, _load_list_of_json_sync, table_name)


async def approved_proof_exists(bot, normalized_url: str) -> bool:
    conn = await bot.loop.run_in_executor(executor, _get_db_connection)
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM approved_proofs WHERE normalized_url = %s;", (normalized_url,))
        exists = cur.fetchone() is not None
        cur.close()
        return exists
    except Exception as e:
        logger.error(f"❌ approved_proof_exists failed: {e}")
        return False
    finally:
        if conn: conn.close()


async def add_approved_proof(bot, normalized_url: str) -> bool:
    conn = await bot.loop.run_in_executor(executor, _get_db_connection)
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO approved_proofs (normalized_url) VALUES (%s) ON CONFLICT DO NOTHING;",
            (normalized_url,)
        )
        inserted = cur.rowcount > 0
        conn.commit()
        cur.close()
        return inserted
    except Exception as e:
        logger.error(f"❌ add_approved_proof failed: {e}")
        return False
    finally:
        if conn: conn.close()


async def add_processed_reaction_if_new(bot, reaction_identifier: str) -> bool:
    conn = await bot.loop.run_in_executor(executor, _get_db_connection)
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO processed_reactions (reaction_identifier) VALUES (%s) ON CONFLICT DO NOTHING;",
            (reaction_identifier,)
        )
        inserted = cur.rowcount > 0
        conn.commit()
        cur.close()
        return inserted
    except Exception as e:
        logger.error(f"❌ add_processed_reaction_if_new failed: {e}")
        return False
    finally:
        if conn: conn.close()


def _log_points_transaction_sync(user_id: str, amount: float, purpose: str = None):
    conn = _get_db_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        transaction_data = {
            "user_id": user_id,
            "amount": amount,
            "purpose": purpose,
            "timestamp": datetime.now(UTC).isoformat()
        }
        query = sql.SQL("INSERT INTO points_history (data) VALUES (%s);").format()
        cur.execute(query, (json.dumps(transaction_data),))
        conn.commit()
        cur.close()
        logger.info(f"✅ Transaction logged for user {user_id}: {amount} for {purpose}.")
    except Exception as e:
        logger.error(f"❌ _log_points_transaction_sync failed: {e}")
    finally:
        if conn: conn.close()


async def log_points_transaction(bot, user_id: str, amount: float, purpose: str = None):
    await bot.loop.run_in_executor(executor, _log_points_transaction_sync, user_id, amount, purpose)