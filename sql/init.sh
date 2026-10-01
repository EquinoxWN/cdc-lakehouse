#!/bin/sh
# Postgres entrypoint hook: runs init.sql with the Debezium password passed as a psql variable,
# so the secret never appears in the SQL file.
set -eu
psql -v ON_ERROR_STOP=1 -v debezium_password="$DEBEZIUM_PASSWORD" \
  --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /sql/init.sql
