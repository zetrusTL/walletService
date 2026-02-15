#!/bin/sh
set -e

echo "Creating databases..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE DATABASE wallet;
  CREATE DATABASE exchanger;
EOSQL

echo "Databases created."
