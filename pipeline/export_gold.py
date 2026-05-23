# -*- coding: utf-8 -*-
import logging
import os
from pyspark.sql import SparkSession

if not os.path.exists("/opt/logs"):
    os.makedirs("/opt/logs")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/opt/logs/export_gold.txt"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("export_gold")

spark = (
    SparkSession.builder
    .appName("export_gold")
    .config("spark.jars", "/opt/jars/mysql-connector-java-8.0.28.jar")
    .getOrCreate()
)
log.info("SparkSession demarree")

mysql_url = "jdbc:mysql://mysql:3306/datamarts"
mysql_props = {
    "user": "admin",
    "password": "admin2024",
    "driver": "com.mysql.cj.jdbc.Driver"
}

datamarts = ["dm_marketing", "dm_risque", "dm_bi"]

for table in datamarts:
    log.info("Export {} vers HDFS Gold...".format(table))
    df = spark.read.jdbc(url=mysql_url, table=table, properties=mysql_props)
    (
        df.write
        .mode("overwrite")
        .parquet("hdfs://namenode:9000/data/gold/{}".format(table))
    )
    log.info("{} exporte dans Gold : {} lignes".format(table, df.count()))

log.info("Export Gold termine avec succes")
spark.stop()# -*- coding: utf-8 -*-
import logging
import os
from pyspark.sql import SparkSession

if not os.path.exists("/opt/logs"):
    os.makedirs("/opt/logs")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/opt/logs/export_gold.txt"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("export_gold")

spark = (
    SparkSession.builder
    .appName("export_gold")
    .config("spark.jars", "/opt/jars/mysql-connector-java-8.0.28.jar")
    .getOrCreate()
)
log.info("SparkSession demarree")

mysql_url = "jdbc:mysql://mysql:3306/datamarts"
mysql_props = {
    "user": "admin",
    "password": "admin2024",
    "driver": "com.mysql.cj.jdbc.Driver"
}

datamarts = ["dm_marketing", "dm_risque", "dm_bi"]

for table in datamarts:
    log.info("Export {} vers HDFS Gold...".format(table))
    df = spark.read.jdbc(url=mysql_url, table=table, properties=mysql_props)
    (
        df.write
        .mode("overwrite")
        .parquet("hdfs://namenode:9000/data/gold/{}".format(table))
    )
    log.info("{} exporte dans Gold : {} lignes".format(table, df.count()))

log.info("Export Gold termine avec succes")
spark.stop()