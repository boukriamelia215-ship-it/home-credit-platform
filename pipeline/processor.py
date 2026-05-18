# -*- coding: utf-8 -*-
import argparse
import logging
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# ARGUMENTS
parser = argparse.ArgumentParser(description="Processor - Raw vers Silver")
parser.add_argument("--input-dir",  required=True, help="Dossier raw HDFS")
parser.add_argument("--output-dir", required=True, help="Dossier silver HDFS")
parser.add_argument("--log-dir",    required=True, help="Dossier des logs")
args = parser.parse_args()

# LOGS
if not os.path.exists(args.log_dir):
    os.makedirs(args.log_dir)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(args.log_dir, "processor.txt")),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("processor")

# SPARK SESSION
spark = (
    SparkSession.builder
    .appName("processor")
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()
)
log.info("SparkSession demarree")

# ============================================================
# LECTURE DEPUIS RAW
# ============================================================
log.info("Lecture des fichiers raw...")

app        = spark.read.parquet("{}/application_train".format(args.input_dir))
bureau     = spark.read.parquet("{}/bureau".format(args.input_dir))
bureau_bal = spark.read.parquet("{}/bureau_balance".format(args.input_dir))
prev_app   = spark.read.parquet("{}/previous_application".format(args.input_dir))
install    = spark.read.parquet("{}/installments_payments".format(args.input_dir))
cc_bal     = spark.read.parquet("{}/credit_card_balance".format(args.input_dir))
pos_cash   = spark.read.parquet("{}/POS_CASH_balance".format(args.input_dir))

# PERSIST sur les plus utilises
app.persist()
bureau.persist()
log.info("Persist applique sur app et bureau")

# ============================================================
# 5 REGLES DE VALIDATION
# ============================================================
log.info("Application des regles de validation...")

app = app.filter(F.col("SK_ID_CURR").isNotNull())
log.info("Regle 1 OK : SK_ID_CURR not null")

app = app.dropDuplicates(["SK_ID_CURR"])
log.info("Regle 2 OK : doublons supprimes")

app = app.filter(F.col("AMT_CREDIT") > 0)
log.info("Regle 3 OK : AMT_CREDIT > 0")

app = app.filter(F.col("TARGET").isin([0, 1]))
log.info("Regle 4 OK : TARGET in [0, 1]")

app = app.filter(F.col("AMT_INCOME_TOTAL") > 0)
log.info("Regle 5 OK : AMT_INCOME_TOTAL > 0")

log.info("Lignes apres validation : {}".format(app.count()))

# ============================================================
# JOINTURES
# ============================================================
log.info("Jointures en cours...")

app_bureau  = app.join(bureau,    on="SK_ID_CURR",   how="left")
bureau_full = bureau.join(bureau_bal, on="SK_ID_BUREAU", how="left")
app_prev    = app.join(prev_app,  on="SK_ID_CURR",   how="left")
app_install = app.join(install,   on="SK_ID_CURR",   how="left")
app_cc      = app.join(cc_bal,    on="SK_ID_CURR",   how="left")
app_pos     = app.join(pos_cash,  on="SK_ID_CURR",   how="left")

log.info("6 jointures OK")

# ============================================================
# AGREGATIONS
# ============================================================
log.info("Agregations en cours...")

agg_bureau = (
    bureau.groupBy("SK_ID_CURR")
    .agg(
        F.count("SK_ID_BUREAU").alias("nb_credits_externes"),
        F.sum("DAYS_CREDIT").alias("total_days_credit")
    )
)

agg_install = (
    install.groupBy("SK_ID_CURR")
    .agg(
        F.avg("AMT_PAYMENT").alias("avg_payment"),
        F.count("SK_ID_PREV").alias("nb_paiements")
    )
)

log.info("Agregations OK")

# ============================================================
# WINDOW FUNCTION
# ============================================================
log.info("Window function en cours...")

window_spec = Window.partitionBy("REGION_POPULATION_RELATIVE").orderBy(F.col("AMT_CREDIT").desc())
app = app.withColumn("rank_credit_region", F.rank().over(window_spec))
log.info("Window function OK")

# ============================================================
# TABLE SILVER FINALE
# ============================================================
silver = (
    app
    .join(agg_bureau,  on="SK_ID_CURR", how="left")
    .join(agg_install, on="SK_ID_CURR", how="left")
)

silver.cache()
log.info("Silver table : {} lignes".format(silver.count()))

# ============================================================
# ECRITURE SILVER HDFS
# ============================================================
log.info("Ecriture silver dans HDFS...")

silver_path = "hdfs://namenode:9000/data/silver/silver_credits"

(
    silver
    .repartition(4)
    .write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(silver_path)
)

log.info("Parquet silver ecrit : {}".format(silver_path))

# CREATION TABLE HIVE INTERNE via saveAsTable
log.info("Creation table Hive interne via saveAsTable...")

silver_hive = spark.read.parquet(silver_path)

(
    silver_hive
    .write
    .mode("overwrite")
    .format("parquet")
    .saveAsTable("default.silver_credits")
)

log.info("Table Hive default.silver_credits creee avec succes")