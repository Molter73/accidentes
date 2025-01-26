#!/usr/bin/env python3

import os

from dash import Dash, dcc, html
import pandas as pd
import plotly.express as px
from pymongo import MongoClient

MONGO_USER = os.environ.get('MONGO_USER', 'mongo')
MONGO_PASS = os.environ.get('MONGO_PASS', 'mongo')
MONGO_SERVICE = os.environ.get('MONGO_SERVICE', 'localhost:27017')
MONGO_URI = f'mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_SERVICE}/admin'


class Collection:
    def __init__(self) -> None:
        self.client = MongoClient(MONGO_URI)

    def __enter__(self):
        return self.client['DMV']['weather']

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.close()


# Eliminamos condiciones comunes que no aportan al accidente y
# el año 2023 que está incompleto.
filt = {
    'condition': {
        '$not': {
            '$in': [
                'Clear',
                'Cloudy',
                'Fair',
                'N',
                'Overcast',
                'Mostly Cloudy',
                'Partly Cloudy',
                'Scattered Clouds',
            ]
        }
    },
    'year': {
        '$not': {
            '$in': [2023],
        }
    },
}

with Collection() as collection:
    data = pd.DataFrame([
        doc for doc in collection.find(filt)
    ])

app = Dash()
app.layout = [
    html.Div(children='Accidentes en EE.UU'),
    dcc.Graph(figure=px.line(data, x='year', y='count',
                             color='condition', markers=True)),
    dcc.Graph(figure=px.line(data, x='year', y='perc',
                             color='condition', markers=True)),
]

if __name__ == '__main__':
    app.run(host='0.0.0.0')
