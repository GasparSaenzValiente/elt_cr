import logging
import os
from datetime import datetime

from airflow.hooks.base import BaseHook
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _build_spark_session() -> SparkSession:
    """
    Crea y configura la SparkSession con los JARs necesarios para
    conectarse a MinIO (S3A) y PostgreSQL (JDBC).

    Las credenciales de MinIO se leen desde variables de entorno para
    evitar depender del sistema de conexiones de Airflow.

    Returns:
        SparkSession configurada y lista para usar.
    """
    minio_endpoint = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
    minio_user = os.getenv("MINIO_ROOT_USER")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD")

    return (
        SparkSession.builder.appName("ClashRoyaleTransformTask")
        .config(
            "spark.jars",
            ",".join([
                "/opt/spark/jars/hadoop-aws-3.3.4.jar",
                "/opt/spark/jars/aws-java-sdk-bundle-1.12.446.jar",
                "/opt/spark/jars/postgresql-42.7.8.jar",
            ]),
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.8.jar")
        .getOrCreate()
    )


# ------------------------------------------------------------------
# Schemas
# ------------------------------------------------------------------

PLAYER_SCHEMA = StructType([
    StructField("tag", StringType(), True),
    StructField("name", StringType(), True),
    StructField("expLevel", IntegerType(), True),
    StructField("trophies", IntegerType(), True),
    StructField("bestTrophies", IntegerType(), True),
    StructField("wins", IntegerType(), True),
    StructField("losses", IntegerType(), True),
    StructField("battleCount", IntegerType(), True),
    StructField("threeCrownWins", IntegerType(), True),
    StructField("warDayWins", IntegerType(), True),
    StructField("currentDeck", ArrayType(MapType(StringType(), StringType())), True),
    StructField("currentDeckSupportCards", ArrayType(MapType(StringType(), StringType())), True),
    StructField("currentFavouriteCard", MapType(StringType(), StringType()), True),
    StructField("clan", MapType(StringType(), StringType()), True),
])

CLAN_SCHEMA = StructType([
    StructField("tag", StringType(), True),
    StructField("name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("location", MapType(StringType(), StringType())),
    StructField("clanScore", IntegerType(), True),
    StructField("clanWarTrophies", IntegerType(), True),
    StructField("requiredTrophies", IntegerType(), True),
    StructField("members", IntegerType(), True),
    StructField("memberList", ArrayType(MapType(StringType(), StringType())), True),
])

BATTLE_LOG_SCHEMA = StructType([
    StructField("battleTime", StringType(), True),
    StructField("gameMode", StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
    ])),
    StructField("isLadderTournament", BooleanType(), True),
    StructField("isHostedMatch", BooleanType(), True),
    StructField("team", ArrayType(StructType([
        StructField("tag", StringType()),
        StructField("cards", ArrayType(StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("level", IntegerType()),
        ]))),
        StructField("elixirLeaked", FloatType()),
        StructField("kingTowerHitPoints", IntegerType()),
        StructField("trophyChange", IntegerType()),
        StructField("crowns", IntegerType()),
        StructField("princessTowersHitPoints", ArrayType(IntegerType())),
    ]))),
    StructField("opponent", ArrayType(StructType([
        StructField("tag", StringType()),
        StructField("cards", ArrayType(StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("level", IntegerType()),
        ]))),
        StructField("elixirLeaked", FloatType()),
        StructField("kingTowerHitPoints", IntegerType()),
        StructField("trophyChange", IntegerType()),
        StructField("crowns", IntegerType()),
        StructField("princessTowersHitPoints", ArrayType(IntegerType())),
    ]))),
])

CARDS_SCHEMA = StructType([
    StructField("items", ArrayType(StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("rarity", StringType()),
        StructField("maxLevel", IntegerType()),
        StructField("elixirCost", IntegerType()),
        StructField("maxEvolutionLevel", IntegerType()),
    ]))),
    StructField("supportItems", ArrayType(StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("rarity", StringType()),
        StructField("maxLevel", IntegerType()),
        StructField("elixirCost", IntegerType()),
        StructField("maxEvolutionLevel", IntegerType()),
    ]))),
])


# ------------------------------------------------------------------
# Función principal (callable del PythonOperator)
# ------------------------------------------------------------------

def transform(execution_date: str, **kwargs) -> None:
    """
    Lee los datos raw de MinIO, los transforma con Spark y los carga
    en las tablas landing de PostgreSQL.

    Args:
        execution_date: Fecha de ejecución en formato 'YYYY-MM-DD'
                        (inyectada por Airflow via template '{{ ds }}').
    """
    spark = _build_spark_session()

    pg_conn = BaseHook.get_connection("postgres_cr_dw")


    db_url = (
    f"jdbc:postgresql://"
    f"{os.getenv('DBT_POSTGRES_HOST', 'db')}:"
    f"{os.getenv('DBT_POSTGRES_PORT', '5432')}/"
    f"{os.getenv('DBT_POSTGRES_DB')}"
    )
    db_properties = {
        "user": os.getenv("DBT_POSTGRES_USER"),
        "password": os.getenv("DBT_POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver",
    }
    logger.info("Connecting to PostgreSQL at: %s", db_url)

    # --- Leer datos raw desde MinIO ---
    logger.info("Reading raw data from MinIO for date: %s", execution_date)
    df_players = spark.read.schema(PLAYER_SCHEMA).json(
        "s3a://cr-raw-data/raw/players/players_info/*/*/*/"
    )
    df_clans = spark.read.schema(CLAN_SCHEMA).json(
        "s3a://cr-raw-data/raw/clans/*/*/*/"
    )
    df_player_battle_log = spark.read.schema(BATTLE_LOG_SCHEMA).json(
        "s3a://cr-raw-data/raw/players/battle_log/*/*/*/"
    )
    df_all_cards = spark.read.schema(CARDS_SCHEMA).json(
        "s3a://cr-raw-data/raw/latest_cards.json"
    )

    # --- Transformar catálogo de cartas ---
    df_cards = (
        df_all_cards
        .withColumn("card", F.explode(F.col("items")))
        .select(
            F.col("card.id").alias("card_id"),
            F.col("card.name").alias("card_name"),
            F.col("card.rarity").alias("rarity"),
            F.col("card.maxLevel").alias("max_level"),
            F.col("card.elixirCost").alias("elixir_cost"),
            F.col("card.maxEvolutionLevel").alias("max_evolution_level"),
        )
    )

    df_support_cards = (
        df_all_cards
        .withColumn("supp_card", F.explode(F.col("supportItems")))
        .select(
            F.col("supp_card.id").alias("card_id"),
            F.col("supp_card.name").alias("card_name"),
            F.col("supp_card.rarity").alias("rarity"),
            F.col("supp_card.maxLevel").alias("max_level"),
            F.col("supp_card.elixirCost").alias("elixir_cost"),
            F.col("supp_card.maxEvolutionLevel").alias("max_evolution_level"),
        )
    )

    # --- Transformar players ---
    df_players = df_players \
        .withColumn("clan", F.to_json(F.col("clan"))) \
        .withColumn("currentFavouriteCard", F.to_json(F.col("currentFavouriteCard")))

    df_player_cards = df_players \
        .withColumn("card", F.explode("currentDeck")) \
        .select(
            F.col("tag").alias("player_tag"),
            F.col("card.id").alias("card_id").cast(LongType()),
            F.col("card.name").alias("card_name"),
            F.col("card.level").alias("card_level").cast(IntegerType()),
            F.col("card.rarity").alias("card_rarity"),
            F.col("card.elixirCost").alias("card_elixir_cost").cast(IntegerType()),
            F.col("card.evolutionLevel").alias("card_evolution_level").cast(IntegerType()),
        )
    df_players = df_players.drop("currentDeck")

    df_support_card = df_players \
        .withColumn("support_card", F.explode("currentDeckSupportCards")) \
        .select(
            F.col("tag").alias("player_tag"),
            F.col("support_card.name").alias("spp_name"),
            F.col("support_card.id").alias("spp_id").cast(LongType()),
            F.col("support_card.level").alias("spp_level").cast(IntegerType()),
        )
    df_players = df_players.drop("currentDeckSupportCards")

    # --- Transformar clans ---
    df_clans = df_clans.withColumn("location", F.to_json(F.col("location")))

    df_clan_members = df_clans \
        .withColumn("member", F.explode(F.col("memberList"))) \
        .select(
            F.col("tag").alias("clan_tag"),
            F.col("member.tag").alias("member_tag"),
            F.col("member.name").alias("member_name"),
            F.col("member.role").alias("member_role"),
            F.col("member.clanRank").alias("member_clan_rank").cast(IntegerType()),
            F.col("member.expLevel").alias("member_exp_level").cast(IntegerType()),
            F.col("member.donations").alias("member_donations").cast(IntegerType()),
            F.col("member.donationsReceived").alias("donations_received").cast(IntegerType()),
        )
    df_clans = df_clans.drop("memberList")

    # --- Agregar snapshot_date y particiones ---
    snapshot_dt = datetime.strptime(execution_date, "%Y-%m-%d")
    year, month, day = snapshot_dt.year, snapshot_dt.month, snapshot_dt.day

    def _add_snapshot(df):
        return df \
            .withColumn("year", F.lit(year)) \
            .withColumn("month", F.lit(month)) \
            .withColumn("day", F.lit(day)) \
            .withColumn("snapshot_date", F.lit(execution_date))

    df_players = _add_snapshot(df_players)
    df_clans = _add_snapshot(df_clans)
    df_clan_members = _add_snapshot(df_clan_members)
    df_player_cards = _add_snapshot(df_player_cards)
    df_support_card = _add_snapshot(df_support_card)

    # --- Transformar battle logs ---
    df_player_battle_log = df_player_battle_log.withColumn(
        "battle_id",
        F.sha2(
            F.concat_ws(
                "_",
                F.col("battleTime"),
                F.least(F.col("team")[0]["tag"], F.col("opponent")[0]["tag"]),
                F.greatest(F.col("team")[0]["tag"], F.col("opponent")[0]["tag"]),
            ),
            256,
        ),
    )

    df_battle_info = df_player_battle_log.select(
        "battle_id",
        F.col("battleTime").alias("battle_time"),
        F.col("gameMode.id").alias("game_mode_id"),
        F.col("gameMode.name").alias("game_mode_name"),
        F.col("isLadderTournament").alias("is_ladder_tournament"),
        F.col("isHostedMatch").alias("is_hosted_match"),
        F.col("team")[0]["tag"].alias("player_tag"),
        F.col("team")[0]["elixirLeaked"].alias("player_elixir_leaked"),
        F.col("team")[0]["kingTowerHitPoints"].alias("player_king_tower_hit_points"),
        F.col("team")[0]["princessTowersHitPoints"].alias("player_princess_tower_hit_points"),
        F.col("team")[0]["trophyChange"].alias("player_trophy_change"),
        F.col("team")[0]["crowns"].alias("player_crowns"),
        F.col("opponent")[0]["tag"].alias("opp_tag"),
        F.col("opponent")[0]["elixirLeaked"].alias("opp_elixir_leaked"),
        F.col("opponent")[0]["kingTowerHitPoints"].alias("opp_king_tower_hit_points"),
        F.col("opponent")[0]["princessTowersHitPoints"].alias("opp_princess_tower_hit_points"),
        F.col("opponent")[0]["trophyChange"].alias("opp_trophy_change"),
        F.col("opponent")[0]["crowns"].alias("opp_crowns"),
    ).withColumn("snapshot_date", F.lit(execution_date))

    df_player_cards_battle_log = (
        df_player_battle_log
        .withColumn("team_player", F.explode("team"))
        .withColumn("team_card", F.explode("team_player.cards"))
        .select(
            "battle_id",
            F.col("team_player.tag").alias("player_tag"),
            F.col("team_card.id").alias("card_id"),
            F.col("team_card.name").alias("card_name"),
            F.col("team_card.level").alias("card_level"),
        )
        .withColumn("snapshot_date", F.lit(execution_date))
    )

    df_opp_cards_battle_log = (
        df_player_battle_log
        .withColumn("opponent_player", F.explode("opponent"))
        .withColumn("opponent_card", F.explode("opponent_player.cards"))
        .select(
            "battle_id",
            F.col("opponent_player.tag").alias("opp_tag"),
            F.col("opponent_card.id").alias("card_id"),
            F.col("opponent_card.name").alias("card_name"),
            F.col("opponent_card.level").alias("card_level"),
        )
        .withColumn("snapshot_date", F.lit(execution_date))
    )

    # --- Escribir en PostgreSQL ---
    logger.info("Writing transformed data to PostgreSQL...")

    # Tablas de eventos (append)
    append_tables = {
        "landing_players": df_players,
        "landing_clans": df_clans,
        "landing_members": df_clan_members,
        "landing_player_cards": df_player_cards,
        "landing_player_support_card": df_support_card,
        "landing_battle_info": df_battle_info,
        "landing_player_cards_battle_log": df_player_cards_battle_log,
        "landing_opp_cards_battle_log": df_opp_cards_battle_log,
    }
    for table_name, df in append_tables.items():
        logger.info("Writing to %s (append)...", table_name)
        df.write.mode("append").jdbc(
            url=db_url, table=table_name, properties=db_properties
        )

    # Tablas de lookup (overwrite con truncate para seguridad)
    overwrite_tables = {
        "landing_cards": df_cards,
        "landing_support_cards": df_support_cards,
    }
    for table_name, df in overwrite_tables.items():
        logger.info("Writing to %s (overwrite)...", table_name)
        df.write.mode("overwrite").option("truncate", "true").jdbc(
            url=db_url, table=table_name, properties=db_properties
        )

    logger.info("Transform task completed successfully.")