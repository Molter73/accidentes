db = new Mongo().getDB('DMV')

db.accidentes.createIndexes([
    {State: 1},
    {Weather_Condition: 1},
])

db.createCollection('totals', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            title: 'Total de accidentes',
            required: ['year', 'count'],
            properties: {
                year: {bsonType: 'long'},
                count: {bsonType: 'long'},
            }
        }
    }
})

db.totals.createIndex({ year: 1 })

db.createCollection('location', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            title: 'Validación de ubicaciones de trafico',
            required: ['year', 'location', 'count'],
            properties: {
                year: {bsonType: 'long'},
                location: {
                    bsonType: 'string',
                    enum: [
                        'Crossing',
                        'Give_Way',
                        'Junction',
                        'No_Exit',
                        'Railway',
                        'Roundabout',
                        'Station',
                        'Stop',
                        'Traffic_Calming',
                        'Traffic_Signal',
                        'Turning_Loop',
                    ],
                },
                count: {bsonType: 'long'},
            },
        }
    }
})

db.location.createIndex({ year: 1 })

db.createCollection('states', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            title: 'Validación de estados',
            required: ['year', 'lat', 'lon', 'count'],
            properties: {
                year: {bsonType: 'long'},
                lat: {bsonType: 'double'},
                lon: {bsonType: 'double'},
                count: {bsonType: 'long'},
            },
        }
    }
})

db.states.createIndexes([
    {lat: 1, lon: 1},
    {year: 1},
])

db.createCollection('weather', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            title: 'Accidentes por condicion climatica',
            required: ['year', 'condition', 'count', 'perc'],
            properties: {
                year: {bsonType: 'long'},
                condition: {bsonType: 'string'},
                count: {bsonType: 'long'},
                perc: {bsonType: 'double'},
            },
        }
    }
})

db.weather.createIndex({ year: 1 })
