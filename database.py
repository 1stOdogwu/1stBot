
# --- New environment ---
import os
import json
import psycopg2
from psycopg2 import sql
import logging

# --- Database Setup ---
DATABASE_URL = os.environ.get("DATABASE_URL")

# --- DB Connection ---
def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to the database: {e}")
        return None

# --- Save & Load Data ---
def load_data(table_name, default_value=None):
    """Loads data from a specified database table."""
    conn = None
    data = default_value
    try:
        conn = get_db_connection()
        if not conn:
            return default_value

        cur = conn.cursor()
        cur.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS {} (key VARCHAR(255) PRIMARY KEY, value JSONB);").format(
                sql.Identifier(table_name))
        )
        conn.commit()

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
        cur.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS {} (key VARCHAR(255) PRIMARY KEY, value JSONB);").format(
                sql.Identifier(table_name))
        )
        conn.commit()

        json_data = json.dumps(data)

        cur.execute(
            sql.SQL(
                "INSERT INTO {} (key, value) VALUES ('data', %s) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;"
            ).format(sql.Identifier(table_name)),
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

# --- Logging Setup ---
class DBHandler(logging.Handler):
    """Custom logging handler to store logs in PostgreSQL."""
    def emit(self, record):
        try:
            log_entry = {
                "level": record.levelname,
                "message": self.format(record)
            }
            save_data("logs", log_entry)  # store in "logs" table
        except Exception as e:
            print(f"❌ Failed to log to DB: {e}")

# Configure logger
logger = logging.getLogger("bot_logger")
logger.setLevel(logging.INFO)

# Add DB logging
db_handler = DBHandler()
db_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(db_handler)

# Also log to console
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(console_handler)
