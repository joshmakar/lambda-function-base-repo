# lambda-reports
This is the monorepo containing the codebases of next-gen reports for Unotifi (and maybe MSM down the road).

Code is written in Typescript, built/deployed via Github Actions, and executed in Node.js via an AWS Lambda function.

These reports can be executed via a scheduled job (using EventsBridge), a REST endpoint (using API Gateway), or any other supported AWS Lambda Trigger.

## Current Reports
### Dealer ROI Video Report
* Code: `./videoReport`
* CI configuration: `./.github/workflows/videoReport.yml`
* [AWS Lambda Function](https://console.aws.amazon.com/lambda/home?region=us-east-1#/functions/DealerROIVideoReport)
* Scheduled EventBridge Lambda triggers (using the `Production` alias of the Lambda)
    * [Every Month, to Audi/Fred, with CSV/Email](https://us-east-1.console.aws.amazon.com/events/home?region=us-east-1#/rules/VideoReportMonthlyToAudi)
    * [Test - Every minute, no CSV/Email](https://us-east-1.console.aws.amazon.com/events/home?region=us-east-1#/rules/VideoReportNoEmailTest)
    * [Test - Every day, with CSV/Email](https://us-east-1.console.aws.amazon.com/events/home?region=us-east-1#/rules/VideoReportWithEmailTest)
* [S3 Bucket path for report uploads](https://s3.console.aws.amazon.com/s3/buckets/unotifi-reports?region=us-east-1&prefix=video-report-3KCe4kZqXCkpZdp4/&showversions=false)

## Architecture
### Lambda Functions
TODO

### Directory Structure
TODO

## Develop
1. Install `ts-node` globally (`npm i -g ts-node`)
1. Add environment variables (`cp .env.example .env` and update the values)
1. Install project deps (`cd videoReport; npm i`)
1. Run (for example) `cd videoReport; ts-node ./test/noEmail` to run a test file, (which invokes the Lambda entrypoint)


## CI/CD
TODO

### "Deploying" to production
TODO

## Adding a new report