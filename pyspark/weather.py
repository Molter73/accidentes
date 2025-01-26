#!/usr/bin/env python3

import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, sum


def weather_cleanup(w: str) -> str:
    w = w \
        .replace('Light', '') \
        .replace('Heavy', '') \
        .split('/')[0] \
        .strip()

    if w == 'T-Storm' or w == 'Thunderstorms' or w == 'Thunder':
        return 'Thunderstorm'
    return w


def weather_split_conj(w: str) -> list[str]:
    if ' and ' in w:
        return w.split(' and ')
    elif ' with ' in w:
        return w.split(' with ')
    else:
        return [w]


def weather_map(weather):
    condition = weather['Weather_Condition']
    d = weather['Start_Time']
    c = weather_split_conj(condition)
    return [(d.year, weather_cleanup(c)) for c in c]


def weather_filter(row) -> bool:
    w = row['Weather_Condition']
    return w is not None and w != ''


def weather_unwrap(row):
    t, count = row
    year, condition = t
    return [year, condition, count]


MONGO_USER = os.environ['MONGO_USER']
MONGO_PASS = os.environ['MONGO_PASS']
MONGO_SERVICE = os.environ['MONGO_SERVICE']
MONGO_URI = f'mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_SERVICE}/admin'
MONGO_JAR = 'org.mongodb.spark:mongo-spark-connector_2.12:10.4.0'

# Creamos la sesión a spark con el conector a mongodb
spark = SparkSession \
    .builder \
    .appName('weather') \
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

# Filtramos y limpiamos los climas, luego los agrupamos por año y
# contamos todas las ocurrencias. Finalmente, aplastamos el año y
# clima en un mismo array con las ocurrencias y lo convertimos en
# un dataframe.
weather_df = df.rdd \
    .filter(weather_filter) \
    .flatMap(weather_map) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(lambda x, y: x+y) \
    .map(weather_unwrap) \
    .toDF(['year', 'condition', 'count'])

# Agregamos el porcentaje de ocurrencias por año de accidentes en
# cada condición climática.
window = Window.partitionBy('year')
weather_df = weather_df.withColumn(
    'perc', weather_df['count'] / sum(col('count')).over(window))

# Volcamos los resultados a mongo.
weather_df.write \
    .format('mongodb') \
    .mode('overwrite') \
    .option('database', 'DMV') \
    .option('collection', 'weather') \
    .save()
