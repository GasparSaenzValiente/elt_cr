from pyspark.sql import SparkSession
from pyspark.sql.types import *

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

    df_players.show()
    df_clans.show()