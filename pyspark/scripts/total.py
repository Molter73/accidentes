#!/usr/bin/env python3

import os

from pyspark.sql import SparkSession


MONGO_USER = os.environ['MONGO_USER']
MONGO_PASS = os.environ['MONGO_PASS']
MONGO_SERVICE = os.environ['MONGO_SERVICE']
MONGO_URI = f'mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_SERVICE}/admin'
MONGO_JAR = 'org.mongodb.spark:mongo-spark-connector_2.12:10.4.0'

# Creamos la sesión a spark con el conector a mongodb
spark = SparkSession \
    .builder \
    .appName('totals') \
    .master('spark://spark:7077') \
    .config('spark.jars.packages', MONGO_JAR) \
    .config('spark.mongodb.read.connection.uri', MONGO_URI) \
    .config('spark.mongodb.write.connection.uri', MONGO_URI) \
    .getOrCreate()

# Cargamos los datos desde mongo
df = spark.read \
    .format('mongodb') \
    .option('database', 'DMV') \
    .option('collection', 'accidentes') \
    .load()

# Contamos la cantidad de accidentes por año
location_df = df.rdd \
    .map(lambda x: (x['Start_Time'].year, 1)) \
    .reduceByKey(lambda x, y: x+y) \
    .toDF(['year', 'count'])

# Volcamos los resultados a mongo.
location_df.write \
    .format('mongodb') \
    .mode('append') \
    .option('database', 'DMV') \
    .option('collection', 'totals') \
    .save()
