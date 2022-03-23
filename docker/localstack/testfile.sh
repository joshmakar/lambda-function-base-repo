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
    "dealershipIds": ["e108cd88-bea5-f4af-11ac-574465d1fd2f"],
    "startDate": "2020-03-23",
    "endDate": "2022-03-23"
  }' \
  ${FUNCTION_NAME}_test.json