#!/bin/sh

FUNCTION_NAME=toyotaRecallReports
alias awslocal="aws --endpoint-url=http://localhost:4566"

awslocal lambda invoke \
    --function-name ${FUNCTION_NAME} \
    --payload '{ "name": "Bob" }' \
    ${FUNCTION_NAME}_test.json