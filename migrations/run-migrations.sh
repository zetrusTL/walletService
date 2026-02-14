#!/bin/sh
set -e

echo "Waiting for Postgres..."
until PGPASSWORD="${PGPASSWORD}" psql -h "${PGHOST}" -U "${PGUSER}" -d "${PGDATABASE}" -c "SELECT 1" >/dev/null 2>&1; do
  sleep 1
done
echo "Postgres is ready."

for f in /migrations/*.sql; do
  [ -f "$f" ] || continue
  echo "Applying $f"
  PGPASSWORD="${PGPASSWORD}" psql -h "${PGHOST}" -U "${PGUSER}" -d "${PGDATABASE}" -v ON_ERROR_STOP=1 -f "$f" || exit 1
done

echo "Migrations done."
exit 0
