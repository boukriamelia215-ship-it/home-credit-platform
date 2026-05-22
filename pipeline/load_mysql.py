# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("load_mysql")
    .config("spark.jars", "/opt/jars/mysql-connector-java-8.0.28.jar")
    .getOrCreate()
)

# Lecture du CSV
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("file:///source/application_train.csv")
)

print("Nombre de lignes : {}".format(df.count()))

# Ecriture dans MySQL comme source BDD
(
    df.write
    .format("jdbc")
    .option("url", "jdbc:mysql://mysql:3306/datamarts")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "application_source")
    .option("user", "admin")
    .option("password", "admin2024")
    .mode("overwrite")
    .save()
)

print("application_train charge dans MySQL avec succes !")
spark.stop()