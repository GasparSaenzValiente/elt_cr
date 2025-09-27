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
        "/opt/spark/jars/aws-java-sdk-bundle-1.12.446.jar,"
        "/opt/spark/jars/postgresql-42.7.8.jar"
    ) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.8.jar") \
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

    # PLAYER CLANTAG
    df_players = df_players.withColumn("clan_tag", F.col("clan")["tag"]).drop("clan") 

    # PLAYER CARDS
    df_player_cards = df_players \
        .withColumn("card", F.explode("current_deck")) \
        .select(
            F.col("tag").alias("player_tag"),
            F.col("card.id").alias("card_id").cast(LongType()),
            F.col("card.name").alias("card_name"),
            F.col("card.level").alias("card_level").cast(IntegerType()),
            F.col("card.rarity").alias("card_rarity"),
            F.col("card.elixirCost").alias("card_elixir_cost").cast(IntegerType()),
            F.col("card.evolutionLevel").alias("card_evolution_level").cast(IntegerType())
        )
    df_players = df_players.drop("current_deck")
    

    # SUPPORT CARD
    df_support_card = df_players \
        .withColumn("support_card", F.explode("current_deck_support_cards")) \
        .select(
            F.col("tag").alias("player_tag"),
            F.col("support_card.name").alias("spp_name"),
            F.col("support_card.id").alias("spp_id").cast(LongType()),
            F.col("support_card.level").alias("spp_level").cast(IntegerType())
        )
    df_players = df_players.drop("current_deck_support_cards")


    # FAV CARD
    df_players = df_players \
    .withColumn("fav_card_name", F.col("current_favourite_card")["name"]) \
    .withColumn("fav_card_elixir", F.col("current_favourite_card")["elixirCost"].cast(IntegerType())) \
    .withColumn("fav_card_rarity", F.col("current_favourite_card")["rarity"])
    df_players = df_players.drop("current_favourite_card") 


    # CLANS INFO
    df_clans = df_clans.withColumn("location_id", F.col("location")["id"]) \
                .withColumn("location_name", F.col("location")["name"]) \
                .drop("location")
    
    # CLANS MEMBERS
    df_clan_members = df_clans.withColumn("member", F.explode(F.col("member_list"))) \
                .select(
                    F.col("tag").alias("clan_tag"),
                    F.col("member.tag").alias("member_tag"),
                    F.col("member.name").alias("member_name"),
                    F.col("member.role").alias("member_role"),
                    F.col("member.clanRank").alias("member_clan_rank").cast(IntegerType()),
                    F.col("member.expLevel").alias("member_exp_level").cast(IntegerType()),
                    F.col("member.donations").alias("member_donations").cast(IntegerType()),
                    F.col("member.donationsReceived").alias("donations_received").cast(IntegerType())
                )
    df_clans = df_clans.drop("member_list")


    # ADDING DATETIME
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

    df_support_card = df_support_card.withColumn("year", F.lit(year)) \
                                    .withColumn("month", F.lit(month)) \
                                    .withColumn("day", F.lit(day))
    

    DB_URL = "jdbc:postgresql://db:5432/cr_db"

    DB_PROPERTIES = {
        "user": "cr_user",
        "password": "cr_pass",
        "driver": "org.postgresql.Driver"
    }

    df_players.write.mode("overwrite") \
    .jdbc(url=DB_URL, table='stg_players', properties=DB_PROPERTIES)

    df_clans.write.mode("overwrite") \
        .jdbc(url=DB_URL, table='stg_clans', properties=DB_PROPERTIES)

    df_clan_members.write.mode("overwrite") \
        .jdbc(url=DB_URL, table='stg_members', properties=DB_PROPERTIES)

    df_player_cards.write.mode("overwrite") \
        .jdbc(url=DB_URL, table='stg_cards', properties=DB_PROPERTIES)
    
    df_support_card.write.mode("overwrite") \
        .jdbc(url=DB_URL, table='stg_support_card', properties=DB_PROPERTIES)
