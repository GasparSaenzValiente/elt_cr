import os
import json
import psycopg2
import boto3
from dotenv import load_dotenv
from datetime import datetime
from scripts.api_wrapper import ClashRoyaleAPI
from botocore.exceptions import ClientError
from airflow.hooks.base import BaseHook
from airflow.models import Variable

def ingest_script():
    # load api key and check
    load_dotenv()
    API_KEY = os.getenv("CLASH_API_KEY")
    if API_KEY is None:
        raise ValueError("did not found CLASH_API_KEY in .env")


    wrapper = ClashRoyaleAPI(api_key=API_KEY) 
    minIO_conn = BaseHook.get_connection('minio_s3_conn')

    s3 = boto3.client(
        "s3",
        endpoint_url=minIO_conn.extra_dejson.get('endpoint_url'), 
        aws_access_key_id=minIO_conn.login,
        aws_secret_access_key=minIO_conn.password,
        region_name="us-east-1"
    )
    
    bucket_name = "cr-raw-data"
    


    try:
        s3.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'BucketAlreadyOwnedByYou':
            pass
        else:
            raise


    try:
        tracking_config = Variable.get("cr_tracking_config", deserialize_json=True)
        player_tags_to_track = tracking_config.get("players", [])
        clan_tags_to_track = tracking_config.get("clans", [])
        print(f"Loaded config: {len(player_tags_to_track)} players, {len(clan_tags_to_track)} clans.")
        
    except KeyError:
        print("Variable 'cr_tracking_config' not found. Starting with empty VIP lists.")
        player_tags_to_track = []
        clan_tags_to_track = []

    try:
        now = datetime.now()
        
        print("Fetching Top Global Clans (to find top players)...")
        
        top_clans_data = wrapper.get_top_clans_global(limit=1, locationId='global')
        
        dynamic_player_tags = []
        discovered_clan_tags = set(clan_tags_to_track) 

        if top_clans_data and 'items' in top_clans_data:
            for clan in top_clans_data['items']:
                clan_tag = clan['tag']
                discovered_clan_tags.add(clan_tag)
                
                print(f"extracting members from top clan {clan_tag}...")
                members_data = wrapper.get_clan_members(clan_tag)
                
                if members_data and 'items' in members_data:
                    for member in members_data['items']:
                        dynamic_player_tags.append(member['tag'])


        all_players_to_track = list(set(player_tags_to_track + dynamic_player_tags))
        
        print(f"Total players to process: {len(all_players_to_track)}")
        print(f"Total clans to process: {len(discovered_clan_tags)}")

        # players info
        print("Saving players info...")
        for player_tag in all_players_to_track:
            # Player
            player_info = wrapper.get_player_info(player_tag=player_tag)
            if player_info:
                object_key = f"raw/players/players_info/year={now:%Y}/month={now:%m}/day={now:%d}/{player_tag}.json" 
                s3.put_object(
                    Bucket=bucket_name,
                    Key=object_key,
                    Body=json.dumps(player_info),
                    ContentType="application/json"
                )
                print(f"Saved {player_tag} info")
            else:
                print(f"Could not get player info: {player_tag}")
            
            # battle logs
            player_battle_log = wrapper.get_player_battle_log(player_tag=player_tag)
            if player_battle_log:
                object_key = f"raw/players/battle_log/year={now:%Y}/month={now:%m}/day={now:%d}/{player_tag}.json" 
                s3.put_object(
                    Bucket=bucket_name,
                    Key=object_key,
                    Body=json.dumps(player_battle_log),
                    ContentType="application/json"
                )
                print(f"Saved {player_tag} battle log")         
            else:
                print(f"Could not get battle log: {player_tag}")

        
        # Clans
        print("Saving clans info...")
        for clan_tag in discovered_clan_tags:
            clan_info = wrapper.get_clan_info(clan_tag=clan_tag)
            if clan_info:
                object_key = f"raw/clans/year={now:%Y}/month={now:%m}/day={now:%d}/{clan_tag}.json" 
                s3.put_object(
                    Bucket=bucket_name,
                    Key=object_key,
                    Body=json.dumps(clan_info),
                    ContentType="application/json"
                )
                print(f"Saved {clan_tag} info")
            else:
                print(f"Could not get clan info: {clan_tag}")

        # Cartas
        print("Saving cards info...")
        cards_info = wrapper.get_cards()
        if cards_info:
            object_key = f"raw/latest_cards.json" 
            s3.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=json.dumps(cards_info),
                ContentType="application/json"
            )
            print(f"Saved cards info")        

    except Exception as e:
        raise e