#!/bin/sh
set -e

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
CONNECTOR_CONFIG="/connectors/debezium-pg-connector.json"

echo "Waiting for Kafka Connect to be ready at ${CONNECT_URL} ..."
until curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; do
  echo "  Not ready yet, retrying in 5s..."
  sleep 5
done
echo "Kafka Connect is ready."

# Read connector name from config (simple grep, no jq dependency)
CONNECTOR_NAME=$(grep '"name"' "${CONNECTOR_CONFIG}" | head -1 | sed 's/.*"name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/')
echo "Connector name: ${CONNECTOR_NAME}"

# Check if already registered
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/connectors/${CONNECTOR_NAME}")
if [ "${STATUS}" = "200" ]; then
  echo "Connector '${CONNECTOR_NAME}' already registered, skipping."
  exit 0
fi

echo "Registering connector..."
HTTP_CODE=$(curl -s -o /tmp/resp.json -w "%{http_code}" \
  -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d @"${CONNECTOR_CONFIG}")

echo "Response (HTTP ${HTTP_CODE}):"
cat /tmp/resp.json
echo ""

if [ "${HTTP_CODE}" = "200" ] || [ "${HTTP_CODE}" = "201" ]; then
  echo "Connector registered successfully."
else
  echo "ERROR: Registration failed with HTTP ${HTTP_CODE}."
  exit 1
fi
