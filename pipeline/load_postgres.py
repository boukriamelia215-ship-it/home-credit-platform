# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("load_postgres")
    .config("spark.jars", "/opt/jars/postgresql-42.2.18.jar")
    .getOrCreate()
)

pg_url = "jdbc:postgresql://hive-metastore-postgresql:5432/metastore"
pg_props = {
    "user": "hive",
    "password": "hive",
    "driver": "org.postgresql.Driver"
}

csv_files = [
    "application_train",
    "bureau",
    "bureau_balance",
    "credit_card_balance",
    "installments_payments",
    "POS_CASH_balance",
    "previous_application"
]

for filename in csv_files:
    print("Chargement de {}...".format(filename))
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("file:///source/{}.csv".format(filename))
    )
    (
        df.write
        .jdbc(url=pg_url, table=filename, mode="overwrite", properties=pg_props)
    )
    print("{} charge dans PostgreSQL : {} lignes".format(filename, df.count()))

print("Tous les fichiers charges dans PostgreSQL !")
spark.stop()