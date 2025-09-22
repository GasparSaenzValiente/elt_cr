from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from datetime import datetime

def transform():
    spark = SparkSession.builder \
    .appName("TransformTask") \
    .config(
        "spark.jars",
        "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
        "/opt/spark/jars/aws-java-sdk-bundle-1.12.446.jar"
    ) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.access.key", "admin")
    hadoop_conf.set("fs.s3a.secret.key", "password")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # schemas 
    player_schema = StructType([
        StructField("tag", StringType(), True),
        StructField("name", StringType(), True),
        StructField("expLevel", IntegerType(), True),
        StructField("bestTrophies", IntegerType(), True),
        StructField("currentDeck", ArrayType(MapType(StringType(), StringType())), True),
        StructField("currentDeckSupportCards", ArrayType(MapType(StringType(), StringType())), True),
        StructField("currentFavouriteCard", MapType(StringType(), StringType()), True),
        StructField("clan", MapType(StringType(), StringType()), True)
    ])

    clan_schema = StructType([
        StructField("tag", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("location", MapType(StringType(), StringType())),
        StructField("clanScore", IntegerType(), True),
        StructField("clanWarTrophies", IntegerType(), True),
        StructField("requiredTrophies", IntegerType(), True),
        StructField("members", IntegerType(), True),
        StructField("memberList", ArrayType(MapType(StringType(), StringType())), True)
    ])

    df_players = spark.read.schema(player_schema).json("s3a://cr-raw-data/raw/players/*/*/*/")
    df_clans   = spark.read.schema(clan_schema).json("s3a://cr-raw-data/raw/clans/*/*/*/")

    rename_players_dict = {
        "expLevel": "exp_level",
        "bestTrophies": "best_trophies",
        "currentDeck": "current_deck",
        "currentDeckSupportCards": "current_deck_support_cards",
        "currentFavouriteCard": "current_favourite_card"
    }

    rename_clans_dict = {
        "clanScore": "clan_score",
        "clanWarTrophies": "clan_war_trophies",
        "requiredTrophies": "required_trophies",
        "memberList": "member_list"
    }

    for oldname, newname in rename_players_dict.items():
        df_players = df_players.withColumnRenamed(oldname, newname)
    
    for oldname, newname in rename_clans_dict.items():
        df_clans = df_clans.withColumnRenamed(oldname, newname)

    # me quedo solo con el clan tag 
    df_players = df_players.withColumn("clan_tag", F.col("clan")["tag"]).drop("clan") 
    
    # hago otro df para el deck, luego sera otra tabla en la db
    df_player_cards = df_players \
        .withColumn("card", F.explode("current_deck")) \
        .select(
            F.col("tag").alias("player_tag"),
            F.col("card.id").alias("card_id"),
            F.col("card.name").alias("card_name"),
            F.col("card.level").alias("card_level"),
            F.col("card.rarity").alias("card_rarity"),
            F.col("card.elixirCost").alias("card_elixir_cost"),
            F.col("card.evolutionLevel").alias("card.evolution_level")
        )
    
    df_players = df_players.drop("current_deck")
    
    
    # clans
    df_clans = df_clans.withColumn("location_id", F.col("location")["id"]) \
                .withColumn("location_name", F.col("location")["name"]) \
                .drop("location")
    
    df_clan_members = df_clans.withColumn("member", F.explode(F.col("member_list"))) \
                .select(
                    F.col("tag").alias("clan_tag"),
                    F.col("member.tag").alias("member_tag"),
                    F.col("member.name").alias("member_name"),
                    F.col("member.role").alias("member_role"),
                    F.col("member.clanRank").alias("member_clan_rank"),
                    F.col("member.expLevel").alias("member_exp_level"),
                    F.col("member.donations").alias("member_donations"),
                    F.col("member.donationsReceived").alias("donations_received")
                )

    # df_clan_members.show(truncate=False)
    # df_player_cards.show(truncate=False)
    # df_players.show()
    # df_clans.show()
    today = datetime.today()
    year, month, day = today.year, today.month, today.day



    df_players = df_players.withColumn("year", F.lit(year)) \
                        .withColumn("month", F.lit(month)) \
                        .withColumn("day", F.lit(day))

    df_clans = df_clans.withColumn("year", F.lit(year)) \
                    .withColumn("month", F.lit(month)) \
                    .withColumn("day", F.lit(day))

    df_clan_members = df_clan_members.withColumn("year", F.lit(year)) \
                                    .withColumn("month", F.lit(month)) \
                                    .withColumn("day", F.lit(day))

    df_player_cards = df_player_cards.withColumn("year", F.lit(year)) \
                                    .withColumn("month", F.lit(month)) \
                                    .withColumn("day", F.lit(day))

    BUCKET_DIR = "s3a://cr-raw-data"

    df_players.write.mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(f"{BUCKET_DIR}/processed/players/")

    df_clans.write.mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(f"{BUCKET_DIR}/processed/clans/")

    df_clan_members.write.mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(f"{BUCKET_DIR}/processed/clan_members/")

    df_player_cards.write.mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(f"{BUCKET_DIR}/processed/player_cards/")

