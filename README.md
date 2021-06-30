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

## Directory Structure
`.github/workflows` - contains Github Actions config files for CI/Deployments. There should be one file per report  
`.env.example` - `dotenv` input example that should be copied to `.env` when developing locally  
`tsconfig.json` - Typescript base configuration with common settings reused by each report's `tsconfig-build.json`  
`{report name}/build` - build output from Typescript transpilation, and used as the Lambda entry point  
`{report name}/src` - Typescript files to be transpiled to `{report name}/build` during CI  
`{report name}/test` - files for local development that invoke the Lambda entry point in `src`  
`{report name}/tsconfig-build.json` - Typescript configuration specific to the report directory  

## Develop
1. Install Node.js 14+ (you can use nvm or the normal package installer)
1. Install `ts-node` globally (`npm i -g ts-node`)
1. Add environment variables (`cp .env.example .env` and update the values)
1. Install project deps (`cd {report name}; npm i`)
1. Run (for example) `cd {report name}; ts-node ./test/noEmail` to run a test file, (which invokes the Lambda entrypoint)

## CI/CD
When changes are merged into the `main` branch, a new CI build should automatically run in [Github Actions](https://github.com/PerfectDayLLC/lambda-reports/actions) for the changed report(s). The build process will typically compile the Typescript and upload the build artifact .zip file to Lambda. Configuration for build steps (aka workflows) are housed in the yaml files within the `.github/workflows` directory of this repo.

### Deploying changes
Unlike a traditional server environment, there is only one AWS resource for all "environments". Instead, we use [Lambda Aliases](https://docs.aws.amazon.com/lambda/latest/dg/configuration-aliases.html) with `Production` and `Staging` labels to route function invocations between production and development usage.

### Testing new changes
After CI runs and a new version of the Lambda is created, you can test the function's `$LATEST` version by selecting the Test tab on the Lambda page, and submitting a test with an example Event payload.

### Promoting to production
If tests pass, the latest version can be "promoted" to production:
1. From the Lambda details page, select the Aliases tab
1. Click Production, then click Edit button
1. Select the desired version (probably the most recent version with the highest number) and save. **DO NOT** select `$LATEST` for the `Production` alias, or you'll automatically deploy all changes from the main branch to production!

## Adding a new report
1. Copy code from `{report name}` to `{new report name}` and update code as needed
1. Copy `.github/workflows/{report name}` to `.github/workflows/{report name}` and update names
1. Develop MVP locally
1. Create a new Lambda Function (from scratch) and set the following non-default configurations:
    * Runtime `Node.js 14.x`
    * Execution Role `videoReportStaging-role-9lvmb7ht`
    * VPC `vpc-f368e696`
    * Subnets `subnet-02ab519a992cffd03` and `subnet-04ab02af5bfdcd515`
    * Security Group `sg-57729333`
    * Logs and Metrics enabled
    * Handler `build/index.handler`
    * Timeout `5` minutes (or something reasonable for running reports)
1. Set environment variables as needed from `.env`
1. Merge code to `main` and let the code be deployed to the new Lambda by Github Actions
1. [Test Lambda](#testing-new-changes)
1. Create a `Production` alias and [point it to the most recent version](#promoting-to-production)
1. Create a Trigger for the Lambda using CloudWatch Events (for scheduled runs) and/or API Gateway (for REST endpoint). Update the JSON payload of the trigger, and ensure that the trigger is pointed to the `Production` alias`