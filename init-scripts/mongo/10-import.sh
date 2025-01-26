#!/usr/bin/env bash

set -euo pipefail

tail -n +2 /tmp/data/US_Accidents_March23.csv |
    mongoimport \
        --type=csv \
        --fieldFile=/init-scripts/mongo/field_file.txt \
        --columnsHaveTypes \
        --ignoreBlanks \
        --drop \
        -u "$MONGO_INITDB_ROOT_USERNAME" \
        -p "$MONGO_INITDB_ROOT_PASSWORD" \
        --db DMV \
        --collection=accidentes \
        --authenticationDatabase admin
