# lambda-reports
This is the monorepo containing the codebases of next-gen reports for Unotifi (and maybe MSM down the road).

Code is written in Typescript, built/deployed via Github Actions, and executed in Node.js via an AWS Lambda function.

These reports can be executed via a scheduled job (using EventsBridge), a REST endpoint (using API Gateway), or any other supported AWS Lambda Trigger.

## Current Reports
### Dealer ROI Video Report
* Code: `./videoReport`
* CI configuration: `./.github/workflows/videoReport.yml`
* AWS Lambda Function: https://console.aws.amazon.com/lambda/home?region=us-east-1#/functions/DealerROIVideoReport
* Scheduled EventBridge Lambda triggers (using the `Production` alias of the Lambda)
    * [Every Month, to Audi/Fred, with CSV/Email](https://us-east-1.console.aws.amazon.com/events/home?region=us-east-1#/rules/VideoReportMonthlyToAudi)
    * [Test - Every minute, no CSV/Email](https://us-east-1.console.aws.amazon.com/events/home?region=us-east-1#/rules/VideoReportNoEmailTest)
    * [Test - Every day, with CSV/Email](https://us-east-1.console.aws.amazon.com/events/home?region=us-east-1#/rules/VideoReportWithEmailTest)

## Architecture
### Lambda Functions

## Reports

## Develop
Install deps (`npm i`), then run `ts-node ./videoReport/test/main` to run a test file, (which invokes the Lambda entrypoint)

## CI/CD
TODO

## "Deploying" to production

## Adding a new report