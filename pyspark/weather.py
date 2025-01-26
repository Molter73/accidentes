from operator import add
from datetime import datetime, date

import pandas as pd
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, Row


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


def weather_map(weather: str) -> list[str]:
    w = weather_split_conj(weather)
    return [weather_cleanup(w) for w in w]


def weather_filter(row) -> bool:
    w = row['Weather_Condition']
    return w is not None and w != ''


MONGO_URI = 'mongodb://mongo:mongo@mongo-db:27017/admin'

spark = SparkSession \
    .builder \
    .appName('test') \
    .master('spark://spark:7077') \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.4.0') \
    .config('spark.mongodb.read.connection.uri', MONGO_URI) \
    .config('spark.mongodb.write.connection.uri', MONGO_URI) \
    .getOrCreate()

df = spark.read \
    .format('mongodb') \
    .option('database', 'DMV') \
    .option('collection', 'accidentes') \
    .load()

weather_df = df.rdd \
    .filter(weather_filter) \
    .flatMap(lambda x: weather_map(x['Weather_Condition'])) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(lambda x, y: x+y) \
    .toDF()

weather_df.write \
    .format('mongodb') \
    .mode('overwrite') \
    .option('database', 'DMV') \
    .option('collection', 'weather') \
    .save()
