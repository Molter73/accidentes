#!/usr/bin/env python3

import os

from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from pymongo import MongoClient

MONGO_USER = os.environ.get('MONGO_USER', 'mongo')
MONGO_PASS = os.environ.get('MONGO_PASS', 'mongo')
MONGO_SERVICE = os.environ.get('MONGO_SERVICE', 'localhost:27017')
MONGO_URI = f'mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_SERVICE}/admin'

EXCLUDE_2023 = {'year': {'$not': {'$in': [2023], }, }, }

app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])


class Collection:
    def __init__(self, collection) -> None:
        self.client = MongoClient(MONGO_URI)
        self.collection = collection

    def __enter__(self):
        return self.client['DMV'][self.collection]

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.close()


def load_weather():
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

    with Collection('weather') as collection:
        return pd.DataFrame(list(collection.find(filt))).sort_values('year')


@app.callback(Output('heatmap', 'figure'), [Input('dropdown', 'value')])
def update_heatmap(value):
    return px.density_map(states[states['year'] == value],
                          lat='lat', lon='lon', radius=50, z='count',
                          center=dict(lat=39, lon=-99), zoom=3,
                          map_style='open-street-map')


def load_locations():
    with Collection('location') as collection:
        return pd.DataFrame(list(collection.find(EXCLUDE_2023))).sort_values('year')


def load_totals():
    with Collection('totals') as collection:
        return pd.DataFrame(list(collection.find(EXCLUDE_2023))).sort_values('year')


def load_states():
    with Collection('states') as collection:
        return pd.DataFrame(list(collection.find({})))


weather = load_weather()
locations = load_locations()
totals = load_totals()
states = load_states()

years = states['year'].unique()
years.sort()

app.layout = dbc.Container([
    html.Div(children='Accidentes en EE.UU'),
    dcc.Graph(id='heatmap'),
    dcc.Dropdown(id='dropdown', options=years.tolist(),
                 value=2023),
    dcc.Graph(figure=px.line(totals, x='year', y='count', markers=True)),
    dbc.Row([
        dbc.Col(dcc.Graph(figure=px.line(weather, x='year', y='count',
                                         color='condition', markers=True))),
        dbc.Col(dcc.Graph(figure=px.line(weather, x='year', y='perc',
                                         color='condition', markers=True))),
    ]),
    dbc.Row([
        dbc.Col(dcc.Graph(figure=px.line(locations, x='year', y='count',
                                         color='location', markers=True))),
        dbc.Col(dcc.Graph(figure=px.line(locations, x='year', y='perc',
                                         color='location', markers=True))),
    ]),
])


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
