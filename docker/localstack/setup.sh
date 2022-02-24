#!/bin/sh

REGION=us-east-1
STAGE=test
FUNCTION_NAME=toyotaRecallReports

alias awslocal="aws --endpoint-url=http://localhost:4566"

# function fail() {
#     echo $2
#     exit $1
# }

# Create zip file without adding parent folder to zip
(cd ${FUNCTION_NAME} && zip -r ../${FUNCTION_NAME}.zip .)

awslocal lambda create-function \
    --region ${REGION} \
    --function-name ${FUNCTION_NAME} \
    --runtime nodejs8.10 \
    --handler index.${FUNCTION_NAME} \
    --memory-size 128 \
    --zip-file fileb://${FUNCTION_NAME}.zip \
    --role arn:aws:iam::123456:role/irrelevant

# [ $? == 0 ] || fail 1 "Failed: AWS / lambda / create-function"

LAMBDA_ARN=$(awslocal lambda list-functions --query "Functions[?FunctionName==\`${FUNCTION_NAME}\`].FunctionArn" --output text --region ${REGION})
