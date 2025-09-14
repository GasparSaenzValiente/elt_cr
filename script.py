import requests
import psycopg2
import os
from dotenv import load_dotenv

# cargar .env
load_dotenv()

# Config
API_KEY = os.getenv("CLASH_API_KEY")
player_tag = "#2PPCJ0UUP"
player_tag_encoded = player_tag.replace("#", "%23")

url = f"https://api.clashroyale.com/v1/players/{player_tag_encoded}"


# Headers
headers = {
    "Accept": "application/json",
    "Authorization": f"Bearer {API_KEY}"
}

# 1. Request a la API
resp = requests.get(url, headers=headers)
data = resp.json()
print(data)
# 2. Conexión a Postgres
conn = psycopg2.connect(
    host="localhost",
    dbname="cr_db",
    user="cr_user",
    password="cr_pass",
    port=5432
)
cur = conn.cursor()

# 3. Crear tabla
cur.execute("""
CREATE TABLE IF NOT EXISTS players (
    tag TEXT PRIMARY KEY,
    name TEXT,
    exp_level INT,
    trophies INT,
    best_trophies INT,
    wins INT,
    losses INT,
    battle_count INT
);
""")

# 4. Insertar jugador
cur.execute("""
INSERT INTO players (tag, name, exp_level, trophies, best_trophies, wins, losses, battle_count)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (tag) DO UPDATE
SET name=EXCLUDED.name,
    exp_level=EXCLUDED.exp_level,
    trophies=EXCLUDED.trophies,
    best_trophies=EXCLUDED.best_trophies,
    wins=EXCLUDED.wins,
    losses=EXCLUDED.losses,
    battle_count=EXCLUDED.battle_count;
""", (
    data["tag"],
    data["name"],
    data["expLevel"],
    data["trophies"],
    data["bestTrophies"],
    data["wins"],
    data["losses"],
    data["battleCount"]
))

conn.commit()
cur.close()
conn.close()

print(f"Jugador {data['name']} guardado en Postgres")
