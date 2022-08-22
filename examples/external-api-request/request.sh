#!/bin/sh
curl -X 'GET' 'https://api.coingecko.com/api/v3/simple/price?ids=golem&vs_currencies=usd'  -H 'accept: application/json' | jq .golem.usd > /golem/output/output.txt
