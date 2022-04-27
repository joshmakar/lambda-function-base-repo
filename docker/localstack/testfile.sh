#!/bin/bash

FUNCTION_NAME=toyotaRecallReports

# Source the ".env" file so the environment variables are available
if [ -f .env ]; then
  source .env
fi

AWS_LOCAL="aws --endpoint-url=http://localhost:4566"

$AWS_LOCAL lambda invoke \
  --function-name ${FUNCTION_NAME} \
  --payload '{
    "dealershipIntegralinkCodes": ["99999"],
    "startDate": "2020-03-23",
    "endDate": "2022-03-23",
    "replyTo": "${REPLY_TO}"
  }' \
  ${FUNCTION_NAME}_test.json
