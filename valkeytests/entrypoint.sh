#!/bin/bash

cd /code/ || exit 1
while /bin/true; do
  /root/.local/bin/poetry run python -m pyvalkey > pyvalkey.log 2>&1
done &

sleep 3

cd /valkey/ || exit 1
./runtest --host localhost --port 6379 --durable --quiet

kill %1