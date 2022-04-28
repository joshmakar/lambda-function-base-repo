// Import dependencies
import mysql from 'promise-mysql';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { createObjectCsvStringifier } from 'csv-writer';
import axios from 'axios';

// Import query functions
import {
  getROICampaigns,
} from './queries/temp';

// Import modules
import { generateS3Key } from './modules/AwsS3Helpers';

// Import interfaces
import { FormattedResult } from './interfaces/FormattedResult';
import { ReturnedResult } from './interfaces/ReturnedResult';
import { Event } from './interfaces/Event';
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
    console.log('Received event:', event);
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
        console.log(JSON.stringify(connection));
        return getReportData(connection, startDate, endDate);
      })
    );

    console.log('Finished getting report data.');

    const resultsFormatted: FormattedResult[] = [];

    // Format the results
    dealershipsResults.forEach((results: ReturnedResult[]) => {
      results.forEach((result: ReturnedResult) => {
        resultsFormatted.push({
          autoCampaignName: result.autoCampaignName,
          dealershipName: result.dealershipName,
          revenue: result.revenue ?? 0,
          soldVehicles: result.soldVehicles ?? 0,
          totalAppointments: result.totalAppointments ?? 0,
          totalAppointmentsArrived: result.totalAppointmentsArrived ?? 0,
          totalOpportunities: result.totalOpportunities ?? 0,
          totalOpportunitiesCalled: result.totalOpportunitiesCalled ?? 0,
          totalOpportunitiesContacted: result.totalOpportunitiesContacted ?? 0,
          totalOpportunitiesTexted: result.totalOpportunitiesTexted ?? 0,
          totalRepairOrders: result.totalRepairOrders ?? 0,
        });
      });
    });

    const csvWriterResults = createObjectCsvStringifier({
      header: [
        { id: 'dealershipName', title: 'Dealership Name' },
        { id: 'autoCampaignName', title: 'Campaign Name' },
        { id: 'totalOpportunities', title: 'Total Opportunities' },
        { id: 'totalOpportunitiesContacted', title: 'Total Opportunities Contacted' },
        { id: 'totalOpportunitiesTexted', title: 'Total Opportunities Texted' },
        { id: 'totalOpportunitiesCalled', title: 'Total Opportunities Called' },
        { id: 'totalAppointments', title: 'Total Appointments' },
        { id: 'totalAppointmentsArrived', title: 'Total Appointments Arrived' },
        { id: 'totalRepairOrders', title: 'Total Repair Orders' },
        { id: 'revenue', title: 'Revenue' },
        { id: 'soldVehicles', title: 'Sold Vehicles' },
      ]
    });

    const csvData = csvWriterResults.getHeaderString() + csvWriterResults.stringifyRecords(resultsFormatted);

    const s3Key = generateS3Key('toyota-recall-reports', 'csv', { prependToPath: 'Toyota_Recall_Reports' });
    console.log(`S3 Key: ${s3Key}`);

    // This works when running via nodejs
    // const s3 = new S3Client({
    //   region: 'us-east-1', // The value here doesn't matter.
    //   endpoint: 'http://localhost:4566', // This is the localstack EDGE_PORT
    //   forcePathStyle: true
    // });

    // This works when running via lambda
    console.log('Connecting to S3...');
    const s3 = new S3Client({
      region: 'us-east-1', // The value here doesn't matter.
      // endpoint: 'http://172.17.0.2:4566', // This is the localstack EDGE_PORT
      forcePathStyle: true
    });
    console.log('Connected to S3.');

    // Save the csv data to S3
    console.log('Saving CSV data to S3...');
    await s3.send(new PutObjectCommand({
      Bucket: process.env['UNOTIFI_REPORTS_BUCKET'],
      Key: s3Key,
      Body: csvData,
      ContentType: 'text/csv',
    }));
    console.log('Saved CSV data to S3.');

    console.log('Updating report', event.replyTo);
    await axios.put(event.replyTo, {
        status: 'completed',
        csv_s3_bucket: process.env['UNOTIFI_REPORTS_BUCKET'],
        csv_s3_key: s3Key,
      })
      .then((response) => {
        console.log('Updated report', response.data);
      })
      .catch((error) => {
        console.log('Error updating report', error);
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
    console.log('Error:', error);
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
  console.log(`Getting report data for ${dealershipDBInfo.internalCode}`);
  const dbConnection = await mysql.createConnection({
    host: dealershipDBInfo.connection.host,
    database: dealershipDBInfo.connection.database,
    user: dealershipDBInfo.connection.user,
    password: dealershipDBInfo.connection.password,
    timeout: 60000,
  });
  console.log(`Connected to ${dealershipDBInfo.internalCode}`);

  let results: ReturnedResult[] = [];

  try {
    console.log(`Getting ROI campaigns for ${dealershipDBInfo.internalCode}`);
    const campaigns = dbConnection.query(getROICampaigns(dealershipDBInfo.internalCode, startDate, endDate));
    console.log('Campaigns: ' + JSON.stringify(campaigns));

    results = await Promise.all([campaigns])
      .then((queryResults) => {
        const consolidatedResults: ReturnedResult[] = [];
        console.log('Query results: ' + JSON.stringify(queryResults));

        queryResults.forEach((queryResult: ReturnedResult[]) => {
          queryResult.forEach((result: ReturnedResult) => {
            console.log('Result: ' + JSON.stringify(result));
            // Find index in consolidatedResults by autoCampaignName
            const index = consolidatedResults.findIndex((item: ReturnedResult) => item.autoCampaignName === result.autoCampaignName);

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

        return consolidatedResults;
      });
  } catch (error) {
    console.log(error);
  } finally {
    // Close the db connection, even if there's an error. This avoids a hanging process.
    await dbConnection.end();
  }

  return results;
}
