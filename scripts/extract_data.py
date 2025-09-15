import os
import json
import psycopg2
import boto3
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

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000", 
    aws_access_key_id="admin",
    aws_secret_access_key="password",
    region_name="us-east-1"
)
bucket_name = "cr-raw-data"
try:
    s3.head_bucket(Bucket=bucket_name)
except:
    s3.create_bucket(Bucket=bucket_name)

try:
    print("Saving players info...")
    for player_tag in PLAYER_TAGS_TO_TRACK:
        player_info = wrapper.get_player_info(player_tag=player_tag)
        if player_info:
                now = datetime.now()
                object_key:str = f"raw/players/year={now:%Y}/month={now:%m}/day={now:%d}/{player_tag}.json" 
                s3.put_object(
                    Bucket=bucket_name,
                    Key=object_key,
                    Body=json.dumps(player_info),
                    ContentType="application/json"
                )
                print(f"Saved {player_tag} info")
        else:
            print(f"Could not get player info of player_tag: {player_tag}")


    print("Saving clans info...")
    for clan_tag in CLANG_TAGS_TO_TRACK:
        clan_info = wrapper.get_clan_info(clan_tag=clan_tag)
        if clan_info:
                now = datetime.now()
                object_key:str = f"raw/clans/year={now:%Y}/month={now:%m}/day={now:%d}/{clan_tag}.json" 
                s3.put_object(
                    Bucket=bucket_name,
                    Key=object_key,
                    Body=json.dumps(clan_info),
                    ContentType="application/json"
                )
                print(f"Saved {clan_tag} info")
        else:
            print(f"Could not get clan info of clan_tag: {clan_tag}")

except:
     pass