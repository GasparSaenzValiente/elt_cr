from pyspark.sql import SparkSession

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

    
    df_players = spark.read.json("s3a://cr-raw-data/raw/players/*/*/*/")
    df_clans   = spark.read.json("s3a://cr-raw-data/raw/clans/*/*/*/")

    df_players_transformed = df_players.select("tag", "name", "expLevel", "bestTrophies", "currentDeck", "clan")
    df_clans_transformed = df_clans.select("tag", "name", "clanScore", "clanWarTrophies", "requiredTrophies", "members", "memberList")

    
    rename_players_dict = {
        "expLevel": "exp_level",
        "bestTrophies": "best_trophies",
        "currentDeck": "current_deck"
    }

    rename_clans_dict = {
        "clanScore": "clan_score",
        "clanWarTrophies": "clan_war_trophies",
        "requiredTrophies": "required_trophies",
        "memberList": "member_list"
    }

    for oldname, newname in rename_players_dict.items():
        df_players_transformed = df_players_transformed.withColumnRenamed(oldname, newname)
    
    for oldname, newname in rename_clans_dict.items():
        df_clans_transformed = df_clans_transformed.withColumnRenamed(oldname, newname)

    df_players_transformed.show()
    df_clans_transformed.show()