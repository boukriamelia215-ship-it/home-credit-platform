# -*- coding: utf-8 -*-
import argparse
import logging
import os
from datetime import date
from pyspark.sql import SparkSession, functions as F

# ARGUMENTS
parser = argparse.ArgumentParser(description="Feeder - Ingestion CSV vers HDFS")
parser.add_argument("--source-dir", required=True, help="Dossier source des CSV")
parser.add_argument("--output-dir", required=True, help="Dossier de sortie HDFS")
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
    .getOrCreate()
)
log.info("SparkSession demarree")

# DATE D'INGESTION
today = date.today()
year  = today.year
month = today.month
day   = today.day
log.info("Date ingestion : {}/{}/{}".format(year, month, day))

# FICHIERS CSV
csv_files = [
    "application_train",
    "bureau",
    "bureau_balance",
    "credit_card_balance",
    "installments_payments",
    "POS_CASH_balance",
    "previous_application"
]

# INGESTION
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