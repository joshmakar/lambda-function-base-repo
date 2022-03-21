#!/bin/bash

FUNCTION_NAME=toyotaRecallReports

# Source the ".env" file so the environment variables are available
if [ -f .env ]; then
    source .env
fi

# Define environment variables
export DB_HOST=${TEMP_DB_HOST}
export DB_USER=${TEMP_DB_USER}
export DB_PASS=${TEMP_DB_PASS}
export DB_NAME=${TEMP_DB_NAME}

AWS_LOCAL="aws --endpoint-url=http://localhost:4566"

$AWS_LOCAL lambda invoke \
    --function-name ${FUNCTION_NAME} \
    --payload '{
        "host":"'"$DB_HOST"'",
        "user":"'"$DB_USER"'",
        "password":"'"$DB_PASS"'",
        "database":"'"$DB_NAME"'"
    }' \
    ${FUNCTION_NAME}_test.json