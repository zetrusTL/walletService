#!/bin/sh
set -e

# Wait for Postgres to be ready
echo "Waiting for Postgres..."
i=0
while true; do
  if psql -c 'SELECT 1' >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  if [ $i -ge 30 ]; then
    echo "Postgres not ready in time"
    exit 1
  fi
  sleep 1
done
echo "Postgres is ready."

# Run migrations in order
for f in $(ls /migrations/*.sql 2>/dev/null | sort); do
  [ -f "$f" ] || continue
  echo "Applying $f"
  psql -v ON_ERROR_STOP=1 -f "$f"
done
echo "Migrations done."
