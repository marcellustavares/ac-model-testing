#!/bin/bash
set -e
TOKEN=$1
if [ "$TOKEN" = "" ]; then
    echo "Please enter a valid token"
    exit 1
fi
JSON=$(echo $TOKEN | base64 -d)
URL=$(echo $JSON | jq -r .url)
TOKEN=$(echo $JSON | jq -r .token)
curl -XPOST "$URL" -d "name=fake-dxp&portalURL=http://localhost:8090&token=$TOKEN"