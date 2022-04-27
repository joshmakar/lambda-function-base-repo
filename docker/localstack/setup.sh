#!/bin/bash

REGION=us-east-1
STAGE=test
FUNCTION_NAME=toyotaRecallReports

# Source the ".env" file so the environment variables are available
if [ -f .env ]; then
  source .env
fi

fail () {
  echo $2
  exit $1
}

AWS_LOCAL="aws --endpoint-url=http://localhost:4566"

# Create zip file without adding parent folder to zip
echo "Creating ${FUNCTION_NAME}.zip file"
rm -f ${FUNCTION_NAME}.zip
(cd ${FUNCTION_NAME}/build && zip -r -q ../../${FUNCTION_NAME}.zip .)

# Check if lambda function exists
$AWS_LOCAL lambda get-function --function-name ${FUNCTION_NAME} > /dev/null 2>&1
  # If it does, update it
  if [ 0 -eq $? ]; then
    echo "Lambda '${FUNCTION_NAME}' exists. Updating it..."

    $AWS_LOCAL lambda update-function-code \
      --function-name ${FUNCTION_NAME} \
      --zip-file fileb://${FUNCTION_NAME}.zip

    [ $? -eq 1 ] && fail 1 "Failed: AWS / lambda / update-function-code"
  # Else, create it
  else
    echo "Lambda '${FUNCTION_NAME}' does not exist. Creating it..."

    $AWS_LOCAL lambda create-function \
      --region ${REGION} \
      --function-name ${FUNCTION_NAME} \
      --runtime nodejs14.x \
      --handler index.handler \
      --memory-size 128 \
      --timeout 60 \
      --zip-file fileb://${FUNCTION_NAME}.zip \
      --role arn:aws:iam::123456:role/irrelevant

    [ $? -eq 1 ] && fail 1 "Failed: AWS / lambda / create-function"

    LAMBDA_ARN=$($AWS_LOCAL lambda list-functions --query "Functions[?FunctionName==\`${FUNCTION_NAME}\`].FunctionArn" --output text --region ${REGION})
  fi

# Remove zip file after lambda function is created/updated
rm -f ${FUNCTION_NAME}.zip


# Create S3 bucket
createBucket() {
  $AWS_LOCAL s3 mb s3://"$1"
}

createBucket ${UNOTIFI_REPORTS_BUCKET}


# Add environment variables to the lambda function
echo "Adding environment variables to the lambda function"

$AWS_LOCAL lambda update-function-configuration --function-name ${FUNCTION_NAME} \
  --environment '{"Variables": {
    "UNOTIFI_API_TOKEN": "'${UNOTIFI_API_TOKEN}'"
  }}'
