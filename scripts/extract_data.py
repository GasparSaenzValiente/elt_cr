import json
import logging
import os
from datetime import datetime
from typing import Optional

import boto3
from botocore.client import Config
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from scripts.api_wrapper import ClashRoyaleAPI

logger = logging.getLogger(__name__)

BUCKET_NAME = "cr-raw-data"


# ------------------------------------------------------------------
# Helpers privados
# ------------------------------------------------------------------

def _get_s3_client() -> boto3.client:
    """Crea y retorna un cliente S3 configurado para MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )

def _ensure_bucket(s3_client: boto3.client, bucket_name: str) -> None:
    """Crea el bucket si no existe; ignora el error si ya existe."""
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        logger.info("Bucket '%s' created.", bucket_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            logger.debug("Bucket '%s' already exists.", bucket_name)
        else:
            raise


def _put_json(s3_client: boto3.client, key: str, data: dict) -> None:
    """Sube un objeto JSON al bucket de MinIO."""
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json",
    )


def _save_player(
    wrapper: ClashRoyaleAPI,
    s3: boto3.client,
    player_tag: str,
    now: datetime,
) -> bool:
    """
    Extrae y guarda el perfil de un jugador en MinIO.

    Returns:
        True si se guardó exitosamente, False si falló.
    """
    player_info = wrapper.get_player_info(player_tag=player_tag)
    if not player_info:
        logger.warning("Could not fetch player info: %s", player_tag)
        return False

    key = (
        f"raw/players/players_info/"
        f"year={now:%Y}/month={now:%m}/day={now:%d}/{player_tag}.json"
    )
    _put_json(s3, key, player_info)
    logger.info("Saved player info: %s", player_tag)
    return True


def _save_battle_log(
    wrapper: ClashRoyaleAPI,
    s3: boto3.client,
    player_tag: str,
    now: datetime,
) -> bool:
    """
    Extrae y guarda el battle log de un jugador en MinIO.

    Returns:
        True si se guardó exitosamente, False si falló.
    """
    battle_log = wrapper.get_player_battle_log(player_tag=player_tag)
    if not battle_log:
        logger.warning("Could not fetch battle log: %s", player_tag)
        return False

    key = (
        f"raw/players/battle_log/"
        f"year={now:%Y}/month={now:%m}/day={now:%d}/{player_tag}.json"
    )
    _put_json(s3, key, battle_log)
    logger.info("Saved battle log: %s", player_tag)
    return True


def _save_clan(
    wrapper: ClashRoyaleAPI,
    s3: boto3.client,
    clan_tag: str,
    now: datetime,
) -> bool:
    """
    Extrae y guarda la información de un clan en MinIO.

    Returns:
        True si se guardó exitosamente, False si falló.
    """
    clan_info = wrapper.get_clan_info(clan_tag=clan_tag)
    if not clan_info:
        logger.warning("Could not fetch clan info: %s", clan_tag)
        return False

    key = f"raw/clans/year={now:%Y}/month={now:%m}/day={now:%d}/{clan_tag}.json"
    _put_json(s3, key, clan_info)
    logger.info("Saved clan info: %s", clan_tag)
    return True


# ------------------------------------------------------------------
# Función principal (callable del PythonOperator)
# ------------------------------------------------------------------

def ingest_script() -> None:
    """
    Punto de entrada del PythonOperator de Airflow para la ingesta de datos.

    Estrategia de descubrimiento "clan-first":
      1. Obtiene los top clanes globales y sus miembros.
      2. Combina esos players con los de la VIP list (Airflow Variable).
      3. Extrae y guarda en MinIO: players, battle logs, clans y cards.

    Raises:
        ValueError: Si CLASH_API_KEY no está configurada.
    """
    load_dotenv()
    api_key = os.getenv("CLASH_API_KEY")
    if not api_key:
        raise ValueError("CLASH_API_KEY not found in environment.")

    wrapper = ClashRoyaleAPI(api_key=api_key)
    s3 = _get_s3_client()
    _ensure_bucket(s3, BUCKET_NAME)

    # --- Cargar VIP list (opcional) ---
    try:
        tracking_config = Variable.get("cr_tracking_config", deserialize_json=True)
        vip_players = tracking_config.get("players", [])
        vip_clans = tracking_config.get("clans", [])
        logger.info(
            "Loaded VIP config: %d players, %d clans.", len(vip_players), len(vip_clans)
        )
    except KeyError:
        logger.info("Variable 'cr_tracking_config' not set. Using empty VIP lists.")
        vip_players = []
        vip_clans = []

    now = datetime.now()

    # --- Descubrir clanes y jugadores top ---
    logger.info("Fetching top global clans...")
    discovered_clan_tags: set[str] = set(vip_clans)
    dynamic_player_tags: list[str] = []

    top_clans_data = wrapper.get_top_clans_global(limit=1, location_id="global")
    if top_clans_data and "items" in top_clans_data:
        for clan in top_clans_data["items"]:
            clan_tag = clan["tag"]
            discovered_clan_tags.add(clan_tag)
            logger.info("Fetching members of top clan %s...", clan_tag)
            members_data = wrapper.get_clan_members(clan_tag)
            if members_data and "items" in members_data:
                for member in members_data["items"]:
                    dynamic_player_tags.append(member["tag"])

    all_players = list(set(vip_players + dynamic_player_tags))
    logger.info(
        "Total to process: %d players, %d clans.",
        len(all_players),
        len(discovered_clan_tags),
    )

    # --- Extraer y guardar players ---
    player_ok, player_fail = 0, 0
    battle_ok, battle_fail = 0, 0

    logger.info("Saving player info and battle logs...")
    for player_tag in all_players:

        if _save_player(wrapper, s3, player_tag, now):
            player_ok += 1
        else:
            player_fail += 1

        if _save_battle_log(wrapper, s3, player_tag, now):
            battle_ok += 1
        else:
            battle_fail += 1

    logger.info(
        "Players: %d ok / %d failed. Battle logs: %d ok / %d failed.",
        player_ok, player_fail, battle_ok, battle_fail,
    )

    # --- Extraer y guardar clans ---
    clan_ok, clan_fail = 0, 0
    logger.info("Saving clan info...")
    for clan_tag in discovered_clan_tags:
        if _save_clan(wrapper, s3, clan_tag, now):
            clan_ok += 1
        else:
            clan_fail += 1

    logger.info("Clans: %d ok / %d failed.", clan_ok, clan_fail)

    # --- Extraer y guardar catálogo de cartas ---
    logger.info("Saving cards catalog...")
    cards_info = wrapper.get_cards()
    if cards_info:
        _put_json(s3, "raw/latest_cards.json", cards_info)
        logger.info("Saved cards catalog.")
    else:
        logger.error("Could not fetch cards catalog.")

    # --- Resumen final ---
    logger.info(
        "Ingestion complete. Players: %d/%d, Battles: %d/%d, Clans: %d/%d.",
        player_ok, player_ok + player_fail,
        battle_ok, battle_ok + battle_fail,
        clan_ok, clan_ok + clan_fail,
    )
