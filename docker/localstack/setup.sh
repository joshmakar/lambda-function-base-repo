#!/bin/sh

REGION=us-east-1
STAGE=test
FUNCTION_NAME=toyotaRecallReports

alias awslocal="aws --endpoint-url=http://localhost:4566"

# Create zip file without adding parent folder to zip
echo "Creating ${FUNCTION_NAME}.zip file"
(cd ${FUNCTION_NAME}/build && zip -r -q ../../${FUNCTION_NAME}.zip .)
(cd ${FUNCTION_NAME} && zip -r -q ../${FUNCTION_NAME}.zip node_modules)

awslocal lambda get-function --function-name ${FUNCTION_NAME} > /dev/null 2>&1
  if [ 0 -eq $? ]; then
    echo "Lambda '${FUNCTION_NAME}' exists. Updating it..."

    awslocal lambda update-function-code \
    --function-name ${FUNCTION_NAME} \
    --zip-file fileb://${FUNCTION_NAME}.zip
  else
    echo "Lambda '${FUNCTION_NAME}' does not exist. Creating it..."

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
  fi

