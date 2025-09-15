import os
import json
import psycopg2
from dotenv import load_dotenv
from datetime import datetime
from api_wrapper import ClashRoyaleAPI

PLAYER_TAGS_TO_TRACK = ["#2PPCJ0UUP", "#VP9GJYQ2", "#G9YV9GR8R", "#22GPGCVCV", "#C0RL8CVCR", "#2L8JG2GRJ"]
CLANG_TAGS_TO_TRACK = ["#2L80YUL", "#QCV8JQVR", "#2L8PCVP0", "#Q0U2PLGU"]

# load api key and check
load_dotenv()
API_KEY = os.getenv("CLASH_API_KEY_2")
if API_KEY is None:
    raise ValueError("did not found CLASH_API_KEY in .env")


wrapper = ClashRoyaleAPI(api_key=API_KEY) 
try:
    conn = psycopg2.connect(
        host="localhost",
        dbname="cr_db",
        user="cr_user",
        password="cr_pass",
        port=5432
    )

    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_players(
        tag TEXT PRIMARY KEY, 
        data JSONB, 
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_clans(
        tag TEXT PRIMARY KEY,
        data JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()

    print("Saving players info...")
    for player_tag in PLAYER_TAGS_TO_TRACK:
        player_info = wrapper.get_player_info(player_tag=player_tag)
        if player_info:
                now = datetime.now()
                cur.execute("""
                    INSERT INTO raw_players (tag, data, created_at, updated_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (tag) DO UPDATE SET
                        data = EXCLUDED.data,
                        updated_at = EXCLUDED.updated_at;
                """, (player_tag, json.dumps(player_info), now, now))
                conn.commit()
        else:
            print(f"Could not get player info of player_tag: {player_tag}")


    print("Saving clans info...")
    for clan_tag in CLANG_TAGS_TO_TRACK:
        clan_info = wrapper.get_clan_info(clan_tag=clan_tag)
        if clan_info:
                now = datetime.now()
                cur.execute("""
                    INSERT INTO raw_clans (tag, data, created_at, updated_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (tag) DO UPDATE SET
                        data = EXCLUDED.data,
                        updated_at = EXCLUDED.updated_at;
                """, (clan_tag, json.dumps(clan_info), now, now))
                conn.commit()
        else:
            print(f"Could not get clan info of clan_tag: {clan_tag}")

except psycopg2.Error as e:
    print(f"Error de PostgreSQL: {e}")
    if conn:
        conn.rollback()
except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
finally:
        if cur:
            cur.close()
        if conn:
            conn.close()