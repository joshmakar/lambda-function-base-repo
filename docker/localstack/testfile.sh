#!/bin/sh

FUNCTION_NAME=toyotaRecallReports

# Source the ".env" file so Laravel's environment variables are available...
if [ -f ../../.env ]; then
    source ./.env
fi

# Define environment variables...
export TEMP_DB_HOST=${TEMP_DB_HOST}
export TEMP_DB_USER=${TEMP_DB_USER}
export TEMP_DB_PASS=${TEMP_DB_PASS}
export TEMP_DB_NAME=${TEMP_DB_NAME}

alias awslocal="aws --endpoint-url=http://localhost:4566"

awslocal lambda invoke \
    --function-name ${FUNCTION_NAME} \
    --payload '{
        "host": "$TEMP_DB_HOST",
        "user": "$TEMP_DB_USER",
        "password": "$TEMP_DB_PASS",
        "database": "$TEMP_DB_NAME"
    }' \
    ${FUNCTION_NAME}_test.json