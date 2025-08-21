import os
import json
import psycopg2
from psycopg2 import sql
from logger import bot_logger as logger

# --- Database Setup ---
DATABASE_URL = os.environ.get("DATABASE_URL")


# --- Helper Functions for Database Interaction ---
def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        logger.error(f"❌ Failed to connect to the database: {e}")
        return None


def load_data(table_name, default_value=None):
    """
    Loads data from a specified database table.
    Returns a default value if no data is found.
    """
    conn = None
    data = default_value
    try:
        conn = get_db_connection()
        if not conn:
            return default_value

        cur = conn.cursor()

        # ✅ Securely create the table using the psycopg2.sql module
        cur.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS {} (key VARCHAR(255) PRIMARY KEY, value JSONB);").format(
                sql.Identifier(table_name))
        )
        conn.commit()

        # ✅ Securely select the data using the psycopg2.sql module
        cur.execute(
            sql.SQL("SELECT value FROM {} WHERE key = 'data';").format(sql.Identifier(table_name))
        )
        result = cur.fetchone()

        if result:
            data = result[0]
            logger.info(f"✅ Data loaded from '{table_name}'.")
        else:
            logger.info(f"⚠️ No data found in '{table_name}'. Returning default value.")

        cur.close()

    except Exception as e:
        logger.error(f"❌ Error loading data from '{table_name}': {e}")
    finally:
        if conn:
            conn.close()
    return data


def save_data(table_name, data):
    """Saves data to a specified database table."""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return

        cur = conn.cursor()

        # ✅ Securely create the table using the psycopg2.sql module
        cur.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS {} (key VARCHAR(255) PRIMARY KEY, value JSONB);").format(
                sql.Identifier(table_name))
        )
        conn.commit()

        json_data = json.dumps(data)

        # ✅ Securely insert/update the data
        cur.execute(
            sql.SQL(
                "INSERT INTO {} (key, value) VALUES ('data', %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;").format(
                sql.Identifier(table_name)),
            (json_data,)
        )
        conn.commit()
        cur.close()
        logger.info(f"✅ Data saved to '{table_name}'.")

    except Exception as e:
        logger.error(f"❌ Error saving data to '{table_name}': {e}")
    finally:
        if conn:
            conn.close()