#!/usr/bin/env bash

SERVER="http://localhost:8080"

RESPONSE=$(curl -s -X POST "$SERVER/nodes/register" \
  -H "Content-Type: application/json" \
  -d '{
    "host": "localhost",
    "port": 9001,
    "totalCapacity": 1000000
  }')

echo "$RESPONSE"

NODE_ID=$(echo "$RESPONSE" | jq -r '.nodeId')

while true
do
  curl -s -X POST "$SERVER/nodes/heartbeat" \
    -H "Content-Type: application/json" \
    -d "{
      \"nodeId\": \"$NODE_ID\",
      \"availableCapacity\": 1000000,
      \"totalCapacity\": 1000000
    }"

  echo ""
  sleep 2
done