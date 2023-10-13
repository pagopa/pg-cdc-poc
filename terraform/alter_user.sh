#!/bin/bash

MAX_RETRIES=5
RETRY_DELAY=60

for ((i=1; i<=$MAX_RETRIES; i++)); do
  PGPASSWORD=$4 psql -h $1 -U $2 -d $3 -c "ALTER USER $2 WITH REPLICATION;" && exit 0
  echo "Attempt $i failed. Retrying in $RETRY_DELAY seconds..."
  sleep $RETRY_DELAY
done

echo "Failed to run psql after $MAX_RETRIES attempts."
exit 1
