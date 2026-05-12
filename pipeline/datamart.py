# -*- coding: utf-8 -*-
import argparse
import logging
import os
from pyspark.sql import SparkSession, functions as F

# ARGUMENTS
parser = argparse.ArgumentParser(description="Datamart - Silver vers MySQL")
parser.add_argument("--input-dir",   required=True, help="Dossier silver HDFS")
parser.add_argument("--mysql-url",   required=True, help="URL MySQL JDBC")
parser.add_argument("--mysql-user",  required=True, help="User MySQL")
parser.add_argument("--mysql-pass",  required=True, help="Password MySQL")
parser.add_argument("--log-dir",     required=True, help="Dossier des logs")
args = parser.parse_args()

# LOGS
if not os.path.exists(args.log_dir):
    os.makedirs(args.log_dir)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(args.log_dir, "datamart.txt")),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("datamart")

# SPARK SESSION
spark = (
    SparkSession.builder
    .appName("datamart")
    .config("spark.jars", "/opt/jars/mysql-connector-java-8.0.28.jar")
    .getOrCreate()
)
log.info("SparkSession demarree")

# LECTURE SILVER
log.info("Lecture silver...")
silver = spark.read.parquet("{}/silver_credits".format(args.input_dir))
silver.cache()
log.info("Silver charge : {} lignes".format(silver.count()))

# FONCTION ECRITURE MYSQL
def write_mysql(df, table_name):
    (
        df.write
        .format("jdbc")
        .option("url", args.mysql_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", table_name)
        .option("user", args.mysql_user)
        .option("password", args.mysql_pass)
        .mode("overwrite")
        .save()
    )
    log.info("Table {} ecrite dans MySQL".format(table_name))

# ============================================================
# DATAMART 1 - MARKETING
# Clients sans historique bancaire susceptibles de souscrire
# ============================================================
log.info("Construction datamart marketing...")

dm_marketing = (
    silver
    .filter(F.col("nb_credits_externes").isNull())
    .select(
        "SK_ID_CURR",
        "TARGET",
        "AMT_CREDIT",
        "AMT_INCOME_TOTAL",
        "NAME_CONTRACT_TYPE",
        "CODE_GENDER",
        "rank_credit_region"
    )
    .distinct()
)

write_mysql(dm_marketing, "dm_marketing")
log.info("Datamart marketing OK : {} lignes".format(dm_marketing.count()))

# ============================================================
# DATAMART 2 - RISQUE / ML
# Prediction du risque de defaut de paiement
# ============================================================
log.info("Construction datamart risque...")

dm_risque = (
    silver
    .select(
        "SK_ID_CURR",
        "TARGET",
        "AMT_CREDIT",
        "AMT_INCOME_TOTAL",
        "nb_credits_externes",
        "total_days_credit",
        "avg_payment",
        "nb_paiements",
        "rank_credit_region"
    )
    .distinct()
)

write_mysql(dm_risque, "dm_risque")
log.info("Datamart risque OK : {} lignes".format(dm_risque.count()))

# ============================================================
# DATAMART 3 - BI / DASHBOARD
# Sante du portefeuille de credit par region
# ============================================================
log.info("Construction datamart BI...")

dm_bi = (
    silver
    .groupBy("REGION_POPULATION_RELATIVE", "NAME_CONTRACT_TYPE")
    .agg(
        F.count("SK_ID_CURR").alias("nb_clients"),
        F.sum("AMT_CREDIT").alias("total_credit"),
        F.avg("AMT_CREDIT").alias("avg_credit"),
        F.sum("TARGET").alias("nb_defauts"),
        F.avg("TARGET").alias("taux_defaut")
    )
)

write_mysql(dm_bi, "dm_bi")
log.info("Datamart BI OK : {} lignes".format(dm_bi.count()))

silver.unpersist()
log.info("Datamart termine avec succes")
spark.stop()