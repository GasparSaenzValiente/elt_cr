import os
import json
from dotenv import load_dotenv
from api_wrapper import ClashRoyaleAPI

PLAYER_TAGS_TO_TRACK = ["#2PPCJ0UUP", "#VP9GJYQ2", "#G9YV9GR8R", "#22GPGCVCV", "#C0RL8CVCR", "#2L8JG2GRJ"]
CLANG_TAGS_TO_TRACK = ["#2L80YUL", "#QCV8JQVR", "#2L8PCVP0", "#Q0U2PLGU"]
PLAYERS_OUTPUT_DIR = "../data/raw/players/"
CLANS_OUTPUT_DIR = "../data/raw/clans/"


# load api key and check
load_dotenv()
API_KEY = os.getenv("CLASH_API_KEY")
if API_KEY is None:
    raise ValueError("did not found CLASH_API_KEY in .env")


wrapper = ClashRoyaleAPI(api_key=API_KEY) 

os.makedirs(PLAYERS_OUTPUT_DIR, exist_ok=True)
os.makedirs(CLANS_OUTPUT_DIR, exist_ok=True)


print("Saving players info...")
for player_tag in PLAYER_TAGS_TO_TRACK:
    player_info = wrapper.get_player_info(player_tag=player_tag)
    if player_info:
            filename = os.path.join(PLAYERS_OUTPUT_DIR, f"{player_tag.replace('#', '')}.json")
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(player_info, f, indent=4, ensure_ascii=False)
            print(f"Saved: {filename}")
    else:
        print(f"Could not get player info of player_tag: {player_tag}")


print("Saving clans info...")
for clan_tag in CLANG_TAGS_TO_TRACK:
    clan_info = wrapper.get_clan_info(clan_tag=clan_tag)
    if clan_info:
            filename = os.path.join(CLANS_OUTPUT_DIR, f"{clan_tag.replace('#', '')}.json")
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(clan_info, f, indent=4, ensure_ascii=False)
            print(f"Saved: {filename}")
    else:
        print(f"Could not get clan info of clan_tag: {clan_tag}")