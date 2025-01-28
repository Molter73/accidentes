#!/usr/bin/env python3

import os

from pyspark.sql import SparkSession

# Taken from https://gist.githubusercontent.com/rozanecm/29926a7c8132a0a25e3b12a24abdff86/raw/d627bc780f6fcb82a22c2934f67c6fe831c07640/states.csv
# and https://www.faa.gov/air_traffic/publications/atpubs/cnt_html/appendix_a.html
STATE_COORDINATES = {
    'AL': (32.7794, -86.8287),
    'AK': (64.0685, -152.2782),
    'AZ': (34.2744, -111.6602),
    'AR': (34.8938, -92.4426),
    'CA': (37.1841, -119.4696),
    'CO': (38.9972, -105.5478),
    'CT': (41.6219, -72.7273),
    'DE': (38.9896, -75.5050),
    'DC': (38.9101, -77.0147),
    'FL': (28.6305, -82.4497),
    'GA': (32.6415, -83.4426),
    'HI': (20.2927, -156.3737),
    'ID': (44.3509, -114.6130),
    'IL': (40.0417, -89.1965),
    'IN': (39.8942, -86.2816),
    'IA': (42.0751, -93.4960),
    'KS': (38.4937, -98.3804),
    'KY': (37.5347, -85.3021),
    'LA': (31.0689, -91.9968),
    'ME': (45.3695, -69.2428),
    'MD': (39.0550, -76.7909),
    'MA': (42.2596, -71.8083),
    'MI': (44.3467, -85.4102),
    'MN': (46.2807, -94.3053),
    'MS': (32.7364, -89.6678),
    'MO': (38.3566, -92.4580),
    'MT': (47.0527, -109.6333),
    'NE': (41.5378, -99.7951),
    'NV': (39.3289, -116.6312),
    'NH': (43.6805, -71.5811),
    'NJ': (40.1907, -74.6728),
    'NM': (34.4071, -106.1126),
    'NY': (42.9538, -75.5268),
    'NC': (35.5557, -79.3877),
    'ND': (47.4501, -100.4659),
    'OH': (40.2862, -82.7937),
    'OK': (35.5889, -97.4943),
    'OR': (43.9336, -120.5583),
    'PA': (40.8781, -77.7996),
    'RI': (41.6762, -71.5562),
    'SC': (33.9169, -80.8964),
    'SD': (44.4443, -100.2263),
    'TN': (35.8580, -86.3505),
    'TX': (31.4757, -99.3312),
    'UT': (39.3055, -111.6703),
    'VT': (44.0687, -72.6658),
    'VA': (37.5215, -78.8537),
    'WA': (47.3826, -120.4472),
    'WV': (38.6409, -80.6227),
    'WI': (44.6243, -89.9941),
    'WY': (42.9957, -107.5512),
}


def state_to_coordinates(state):
    if state not in STATE_COORDINATES:
        return (0, 0)

    return STATE_COORDINATES[state]


def states_unwrap(row):
    (k, count) = row
    (year, t) = k
    (lat, lon) = t
    return [year, lat, lon, count]


MONGO_USER = os.environ['MONGO_USER']
MONGO_PASS = os.environ['MONGO_PASS']
MONGO_SERVICE = os.environ['MONGO_SERVICE']
MONGO_URI = f'mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_SERVICE}/admin'
MONGO_JAR = 'org.mongodb.spark:mongo-spark-connector_2.12:10.4.0'

# Creamos la sesión a spark con el conector a mongodb
spark = SparkSession \
    .builder \
    .appName('states') \
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

# Mapeamos cada accidente a las coordenadas centrales del estado en el
# que ocurrió, los agrupamos por año y los contamos.
states_df = df.rdd \
    .map(lambda x: ((x['Start_Time'].year, state_to_coordinates(x['State'])), 1)) \
    .reduceByKey(lambda x, y: x+y) \
    .map(states_unwrap) \
    .toDF(['year', 'lat', 'lon', 'count'])

# Volcamos los resultados a mongo.
states_df.write \
    .format('mongodb') \
    .mode('append') \
    .option('database', 'DMV') \
    .option('collection', 'states') \
    .save()
