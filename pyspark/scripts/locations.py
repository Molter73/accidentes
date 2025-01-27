#!/usr/bin/env python3

import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, sum


def location_map(row):
    year = row['Start_Time'].year

    locations = []
    if row['Crossing']:
        locations.append('Crossing')
    if row['Give_Way']:
        locations.append('Give_Way')
    if row['Junction']:
        locations.append('Junction')
    if row['No_Exit']:
        locations.append('No_Exit')
    if row['Railway']:
        locations.append('Railway')
    if row['Roundabout']:
        locations.append('Roundabout')
    if row['Station']:
        locations.append('Station')
    if row['Stop']:
        locations.append('Stop')
    if row['Traffic_Calming']:
        locations.append('Traffic_Calming')
    if row['Traffic_Signal']:
        locations.append('Traffic_Signal')
    if row['Turning_Loop']:
        locations.append('Turning_Loop')

    return [((year, loc), 1) for loc in locations]


def location_unwrap(row):
    t, count = row
    year, location = t
    return [year, location, count]


MONGO_USER = os.environ['MONGO_USER']
MONGO_PASS = os.environ['MONGO_PASS']
MONGO_SERVICE = os.environ['MONGO_SERVICE']
MONGO_URI = f'mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_SERVICE}/admin'
MONGO_JAR = 'org.mongodb.spark:mongo-spark-connector_2.12:10.4.0'

# Creamos la sesión a spark con el conector a mongodb
spark = SparkSession \
    .builder \
    .appName('locations') \
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

# Contamos las ubicaciones en las que ocurrieron accidentes
# y las agrupamos por año
location_df = df.rdd \
    .flatMap(location_map) \
    .reduceByKey(lambda x, y: x+y) \
    .map(location_unwrap) \
    .toDF(['year', 'location', 'count'])

# Calculamos el porcentage de accidentes en cada año
window = Window.partitionBy('year')
location_df = location_df.withColumn(
    'perc', location_df['count'] / sum(col('count')).over(window))

# Volcamos los resultados a mongo.
location_df.write \
    .format('mongodb') \
    .mode('overwrite') \
    .option('database', 'DMV') \
    .option('collection', 'location') \
    .save()
