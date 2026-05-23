# -*- coding: utf-8 -*-
import argparse
import logging
import os
from datetime import date
from pyspark.sql import SparkSession, functions as F

# ARGUMENTS
parser = argparse.ArgumentParser(description="Feeder - Ingestion vers HDFS")
parser.add_argument("--source-dir", required=True, help="Dossier source des CSV")
parser.add_argument("--output-dir", required=True, help="Dossier de sortie HDFS")
parser.add_argument("--pg-url",     required=True, help="URL PostgreSQL JDBC")
parser.add_argument("--pg-user",    required=True, help="User PostgreSQL")
parser.add_argument("--pg-pass",    required=True, help="Password PostgreSQL")
parser.add_argument("--log-dir",    required=True, help="Dossier des logs")
args = parser.parse_args()

# LOGS
if not os.path.exists(args.log_dir):
    os.makedirs(args.log_dir)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(args.log_dir, "feeder.txt")),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("feeder")

# SPARK SESSION
spark = (
    SparkSession.builder
    .appName("feeder")
    .config("spark.jars", "/opt/jars/postgresql-42.2.18.jar")
    .getOrCreate()
)
log.info("SparkSession demarree")

# DATE D'INGESTION
today = date.today()
year  = today.year
month = today.month
day   = today.day
log.info("Date ingestion : {}/{}/{}".format(year, month, day))

# SOURCE 1 — PostgreSQL (application_train)
log.info("Lecture source 1 : application_train depuis PostgreSQL...")

df_pg = (
    spark.read
    .format("jdbc")
    .option("url", args.pg_url)
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "application_train")
    .option("user", args.pg_user)
    .option("password", args.pg_pass)
    .option("partitionColumn", "SK_ID_CURR")
    .option("lowerBound", "100002")
    .option("upperBound", "456255")
    .option("numPartitions", "8")
    .load()
)

today = date.today()
df_pg2 = (
    df_pg
    .withColumn("year",  F.lit(today.year))
    .withColumn("month", F.lit(today.month))
    .withColumn("day",   F.lit(today.day))
)

df_pg2.cache()
df_pg2.show(5)

r = df_pg2.count()
log.info("Nombre de lignes application_train (PostgreSQL) : {}".format(r))

output_path = "{}/application_train".format(args.output_dir)
(
    df_pg2.repartition(4)
    .write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(output_path)
)

log.info("application_train ecrit depuis PostgreSQL dans {}".format(output_path))
df_pg2.unpersist()

# SOURCE 2 — CSV (6 autres fichiers)
# Ecriture dans HDFS Bronze 
log.info("Lecture source 2 : fichiers CSV...")

csv_files = [
    "bureau",
    "bureau_balance",
    "credit_card_balance",
    "installments_payments",
    "POS_CASH_balance",
    "previous_application"
]

for filename in csv_files:
    input_path  = "file://{}/{}.csv".format(args.source_dir, filename)
    output_path = "{}/{}".format(args.output_dir, filename)

    try:
        log.info("Lecture de {}.csv ...".format(filename))

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(input_path)
        )

        today = date.today()
        df2 = (
            df.withColumn("year",  F.lit(today.year))
              .withColumn("month", F.lit(today.month))
              .withColumn("day",   F.lit(today.day))
        )

        df2.cache()
        df2.show(5)

        r = df2.count()
        log.info("Nombre de lignes {} : {}".format(filename, r))

        (
            df2.repartition(4)
            .write
            .mode("overwrite")
            .partitionBy("year", "month", "day")
            .parquet(output_path)
        )

        log.info("{} ecrit avec succes dans {}".format(filename, output_path))
        df2.unpersist()

    except Exception as e:
        log.error("Erreur sur {} : {}".format(filename, str(e)))

log.info("Feeder termine avec succes")
spark.stop()