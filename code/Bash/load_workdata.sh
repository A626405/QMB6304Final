#!/bin/bash

DB_PATH="data/internal/databases.db"

OUTPUT_FILE="data/internal/temp/workdata.txt"

sqlite3 "$DB_PATH" <<EOF
.headers on
.mode quote
.output $OUTPUT_FILE
SELECT data FROM databases WHERE file_name = 'working_data.RDA';
.output stdout
EOF

echo "Data exported to $OUTPUT_FILE"



#!/bin/bash

DB_PATH="data/internal/databases.db"

OUTPUT_FILE="data/internal/temp/workdata"

sqlite3 "$DB_PATH" <<EOF
.headers on
.mode quote
.output $OUTPUT_FILE
SELECT data FROM databases WHERE file_name = 'working_data.RDA';
.output stdout
EOF

echo "Data exported to $OUTPUT_FILE"
