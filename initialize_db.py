import os
import json
import psycopg2
from psycopg2 import sql

# Assumes your DATABASE_URL environment variable is set.
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    """Establishes and returns a database connection."""
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def save_data(table_name, data):
    """Saves data to a specified database table."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY, data JSONB);").format(sql.Identifier(table_name)))
        cur.execute(
            sql.SQL("INSERT INTO {} (id, data) VALUES (1, %s) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data;").format(sql.Identifier(table_name)),
            (json.dumps(data),)
        )
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Error saving data to {table_name}: {e}")
    finally:
        if conn:
            conn.close()

# --- Initialize and save all data to the database ---

# Dictionaries
save_data("user_points_table", {})
save_data("submissions_table", {})
save_data("logs_table", {})
save_data("vip_posts_table", {})
save_data("user_xp_table", {})
save_data("weekly_quests_table", {"week": 0, "quests": []})
save_data("quest_submissions_table", {})
save_data("gm_log_table", {})
save_data("admin_points_table", {
    "total_supply": 10000000000.0,
    "balance": 10000000000.0,
    "claimed_points": 0.0,
    "burned_points": 0.0,
    "my_points": 0.0,
    "fees_earned": 0.0
})
save_data("referral_data_table", {})
save_data("pending_referrals_table", {})
save_data("active_tickets_table", {})
save_data("mysterybox_uses_table", {})

# Lists
save_data("approved_proofs_table", [])
save_data("points_history_table", [])
save_data("giveaway_logs_table", [])
save_data("all_time_giveaway_logs_table", [])

# Sets
save_data("referred_users_table", [])
save_data("processed_reactions_table", [])

print("Database initialization complete. All tables created and populated with default values.")