#!/bin/sh

# Set Working Directory Inside Container
# DIR=/docker-entrypoint-initaws.d/docker/localstack/shell

# shellcheck disable=SC1090
# for ext in "${DIR}"/ext-*.sh; do source "${ext}"; done

REGION=us-east-1
STAGE=test
FUNCTION_NAME=toyotaRecallReports

alias awslocal="aws --endpoint-url=http://localhost:4566"

# Remove previous zip file
rm -f ${FUNCTION_NAME}.zip

# Create zip file without adding parent folder to zip
(cd ${FUNCTION_NAME} && zip -r ../${FUNCTION_NAME}.zip .)

# Create update function
awslocal lambda update-function-code \
    --function-name ${FUNCTION_NAME} \
    --zip-file fileb://${FUNCTION_NAME}.zip