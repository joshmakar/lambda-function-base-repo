// Import dependencies
import mysql from 'promise-mysql';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { createObjectCsvStringifier } from 'csv-writer';
import axios from 'axios';

// Import query functions
import {
  getAppointments,
  getROICampaigns,
} from './queries/temp';

// Import modules
import { generateS3Key } from './modules/AwsS3Helpers';

// Import interfaces
import { FormattedResult } from './interfaces/FormattedResult';
import { ReturnedResult } from './interfaces/ReturnedResult';
import { DealershipDBInfo } from './classes/UnotifiApi/interfaces/DealershipDBInfo';

// Import classes
import { UnotifiApiClient } from './classes/UnotifiApi/UnotifiApiClient';

// Configure environment variables if not in production
if (process.env['NODE_ENV'] !== 'production') {
  require('dotenv').config();
}

/**
 * The main function that runs the entire process.
 */
export const handler = async (event: any, _context: any, callback: any) => {
  try {
    // Check required DB connection environment variables
    ['UNOTIFI_API_TOKEN', 'UNOTIFI_REPORTS_BUCKET'].forEach(envVar => {
      if (!process.env[envVar]) {
        callback(`Please set ${envVar} in your environment`);
        return;
      }
    });

    event = JSON.parse(event.Records[0].body);

    // Check that the event contains one or more dealershipIntegralinkCodes
    if (!event.dealershipIntegralinkCodes?.length) {
      callback('Please provide at least one dealershipIntegralinkCode');
      return;
    }

    // Check that the event contains a startDate and endDate
    if (!event.startDate || !event.endDate) {
      callback('Please provide a startDate and endDate');
      return;
    }

    // Check that the even has a replyTo field
    if (!event.replyTo) {
      callback('Please provide a replyTo field');
      return;
    }

    // Get dealerships db info
    const unotifiApiClient = new UnotifiApiClient(process.env['UNOTIFI_API_TOKEN']!);
    const dealershipsConnections: DealershipDBInfo[] = await unotifiApiClient.getDealershipsDBInfo(event.dealershipIntegralinkCodes);

    // Convert string date to Date object
    const startDate: Date = new Date(event.startDate);
    const endDate: Date = new Date(event.endDate);

    // Get report data for each dealership
    const dealershipsResults = await Promise.all(
      dealershipsConnections.map((connection) => {
        return getReportData(connection, startDate, endDate);
      })
    );

    const resultsFormatted: FormattedResult[] = [];

    // Format the results
    dealershipsResults.forEach((results: ReturnedResult[]) => {
      results.forEach((result: ReturnedResult) => {
        resultsFormatted.push({
          dealershipName: result.dealershipName, // Store
          campaignName: result.campaignName ?? '', // Campaign name
          campaignType: result.campaignType ?? '', // Campaign type
          textMessageNo: result.textMessageNo ?? 0, // # of text messages
          emailNo: result.emailNo ?? 0, // # of emails
          appointmentNo: result.appointmentNo ?? 0, // # of appointments
          arrivedAppointmentNo: result.arrivedAppointmentNo ?? 0, // # of arrived appointments
          roNo: result.roNo ?? 0, // # of ROs
          roTotal: result.roTotal ?? 0,
        });
      });
    });

    const csvWriterResults = createObjectCsvStringifier({
      header: [
        { id: 'dealershipName', title: 'Dealership Name' },
        { id: 'campaignName', title: 'Campaign Name' },
        { id: 'campaignType', title: 'Campaign Type' },
        { id: 'textMessageNo', title: 'No. of Texts' },
        { id: 'emailNo', title: 'No. of Emails' },
        { id: 'appointmentNo', title: 'No. of Appointments' },
        { id: 'arrivedAppointmentNo', title: 'No. of Arrived Appointments' },
        { id: 'roNo', title: 'No. of ROs' },
        { id: 'roTotal', title: 'Amount' },
      ]
    });

    const csvData = csvWriterResults.getHeaderString() + csvWriterResults.stringifyRecords(resultsFormatted);

    const s3Key = generateS3Key('recall-roi-report', 'csv', { prependToPath: 'Recall_ROI_Reports' });

    // This works when running via nodejs
    // const s3 = new S3Client({
    //   region: 'us-east-1', // The value here doesn't matter.
    //   endpoint: 'http://localhost:4566', // This is the localstack EDGE_PORT
    //   forcePathStyle: true
    // });

    // This works when running via lambda
    const s3 = new S3Client({
      region: 'us-east-1', // The value here doesn't matter.
      // endpoint: 'http://172.17.0.2:4566', // This is the localstack EDGE_PORT
      forcePathStyle: true
    });

    // Save the csv data to S3
    await s3.send(new PutObjectCommand({
      Bucket: process.env['UNOTIFI_REPORTS_BUCKET'],
      Key: s3Key,
      Body: csvData,
      ContentType: 'text/csv',
    }));

    await axios.put(event.replyTo, {
        status: 'completed',
        csv_s3_bucket: process.env['UNOTIFI_REPORTS_BUCKET'],
        csv_s3_key: s3Key,
      })
      .catch((error) => {
        throw new Error("Error updating report", error);
      });

    callback(null, {
      statusCode: 201,
      body: s3Key,
      headers: {
        'X-Custom-Header': 'ASDF'
      }
    });
  } catch (error) {
    callback(error);
  }
}

/**
 * Get the report data for a dealership
 * @param dealershipDBInfo The dealership db info
 * @param startDate The start date
 * @param endDate The end date
 * @returns The report data
 */
async function getReportData(dealershipDBInfo: DealershipDBInfo, startDate: Date, endDate: Date): Promise<ReturnedResult[]> {
  const dbConnection = await mysql.createConnection({
    host: dealershipDBInfo.connection.host,
    database: dealershipDBInfo.connection.database,
    user: dealershipDBInfo.connection.user,
    password: dealershipDBInfo.connection.password,
    timeout: 60000,
  });

  const consolidatedResults: ReturnedResult[] = [];

  try {
    const campaigns = dbConnection.query(getROICampaigns(dealershipDBInfo.internalCode, startDate, endDate));

    await Promise.all([campaigns])
      .then((queryResults) => {
        queryResults.forEach((queryResult: ReturnedResult[]) => {
          queryResult.forEach((result: ReturnedResult) => {
            // Find index in consolidatedResults by campaignId
            const index = consolidatedResults.findIndex((item: ReturnedResult) => item.campaignId === result.campaignId);

            // If not found, create new object
            if (index === -1) {
              consolidatedResults.push({
                ...result,
                dealershipName: dealershipDBInfo.dealerName,
              });
            } else {
              // If found, update existing object
              consolidatedResults[index] = {
                ...consolidatedResults[index],
                ...result,
                dealershipName: dealershipDBInfo.dealerName,
              };
            }
          });
        });
      });

    // Get unique campaign Ids
    const campaignIds = [...new Set(consolidatedResults.map((result: ReturnedResult) => result.campaignId))];

    if (campaignIds.length) {
      const appointments = dbConnection.query(getAppointments(campaignIds, startDate, endDate));

      await Promise.all([appointments])
        .then((queryResults) => {
          queryResults.forEach((queryResult: ReturnedResult[]) => {
            queryResult.forEach((result: ReturnedResult) => {
              // Find index in consolidatedResults by campaignId
              const index = consolidatedResults.findIndex((item: ReturnedResult) => item.campaignId === result.campaignId);

              // If not found, create new object
              if (index === -1) {
                consolidatedResults.push({
                  ...result,
                  dealershipName: dealershipDBInfo.dealerName,
                });
              } else {
                // If found, update existing object
                consolidatedResults[index] = {
                  ...consolidatedResults[index],
                  ...result,
                };
              }
            });
          });
        });
    }
  } catch (error) {
    console.error(error);
  } finally {
    // Close the db connection, even if there's an error. This avoids a hanging process.
    await dbConnection.end();
  }

  return consolidatedResults;
}
