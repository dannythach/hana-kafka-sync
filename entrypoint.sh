#!/bin/bash
set -e

echo "Waiting for Kafka Connect..."

until curl -s http://kafka-connect:8083/connectors > /dev/null; do
  echo "Kafka Connect not ready yet..."
  sleep 5
done

echo "Deploying connectors..."
python deploy_connectors.py

echo "Starting main sync service..."
exec python main.py
