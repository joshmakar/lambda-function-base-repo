// Import dependencies
import mysql from 'promise-mysql';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { createObjectCsvStringifier } from 'csv-writer';

// Import query functions
import {
  getOpportunitiesTextedCalledQuery,
  getAppointmentsQuery,
  getRepairOrderRevenueQuery,
  getOpportunitiesContactedQuery,
  getDealershipsDBInfo,
} from './queries/temp';

// Import modules
import { generateS3Key } from './modules/AwsS3Helpers';

// Import interfaces
import { FormattedResult } from './interfaces/FormattedResult';
import { ReturnedResult } from './interfaces/ReturnedResult';
import { Event } from './interfaces/Event';
import { DealershipDBInfo } from './interfaces/DealershipDBInfo';

// Configure environment variables if not in production
if (process.env['NODE_ENV'] !== 'production') {
  require('dotenv').config();
}

/**
 * The main function that runs the entire process.
 */
const toyotaRecallReports = async (event: Event, _context: any, callback: any) => {
  // Check that the event contains one or more dealershipIds
  if (!event.dealershipIds?.length) {
    callback('Please provide at least one dealershipId');
    return;
  }

  // Check that the event contains a startDate and endDate
  if (!event.startDate || !event.endDate) {
    callback('Please provide a startDate and endDate');
    return;
  }

  // Check required DB connection environment variables
  ['UNOTIFI_COM_INDEX_DB_HOST', 'UNOTIFI_COM_INDEX_DB_USER', 'UNOTIFI_COM_INDEX_DB_PASS'].forEach(envVar => {
    if (!process.env[envVar]) {
      callback(`Please set ${envVar} in your environment`);
      return;
    }
  });

  // Escape the dealershipIds
  const safeDealershipIds: string[] = event.dealershipIds.map((id: string|number) => mysql.escape(id));

  // Connection for the Unotifi Index db to get the dealerships db info
  const indexDbConn: mysql.Connection = await mysql.createConnection({
    host: process.env['UNOTIFI_COM_INDEX_DB_HOST'],
    user: process.env['UNOTIFI_COM_INDEX_DB_USER'],
    password: process.env['UNOTIFI_COM_INDEX_DB_PASS'],
    database: 'unotifi_com_index',
    timeout: 60000,
  });

  // Get the dealerships db info
  const dealershipsConnections: DealershipDBInfo[] = await indexDbConn.query(getDealershipsDBInfo(safeDealershipIds))
    .finally(() => {
      indexDbConn.end();
    });

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

  const s3Key = generateS3Key('toyota-recall-reports', 'csv');

  // This works when running via nodejs
  const s3 = new S3Client({
    region: 'us-east-1', // The value here doesn't matter.
    endpoint: 'http://localhost:4566', // This is the localstack EDGE_PORT
    forcePathStyle: true
  });

  // This works when running via lambda
  // const s3 = new S3Client({
  //   region: 'us-east-1', // The value here doesn't matter.
  //   endpoint: 'http://172.17.0.2:4566', // This is the localstack EDGE_PORT
  //   forcePathStyle: true
  // });

  // Save the csv data to S3
  await s3.send(new PutObjectCommand({
    Bucket: 'test-bucket-123',
    Key: s3Key,
    Body: csvData,
    ContentType: 'text/csv',
  }));

  callback(null, {
    statusCode: 201,
    body: s3Key,
    headers: {
      'X-Custom-Header': 'ASDF'
    }
  });
}

module.exports = {
  toyotaRecallReports,
};

/**
 * Get the report data for a dealership
 * @param dealershipDBInfo The dealership db info
 * @param startDate The start date
 * @param endDate The end date
 * @returns The report data
 */
async function getReportData(dealershipDBInfo: DealershipDBInfo, startDate: Date, endDate: Date): Promise<ReturnedResult[]> {
  const dbConnection = await mysql.createConnection({
    host: dealershipDBInfo.IP,
    user: dealershipDBInfo.user,
    password: dealershipDBInfo.password,
    database: dealershipDBInfo.name,
    timeout: 60000,
  });

  let results: ReturnedResult[] = [];

  try {
    const opportunities = dbConnection.query(getOpportunitiesContactedQuery(dealershipDBInfo.internal_code, startDate, endDate));
    const opportunitiesContacted = dbConnection.query(getOpportunitiesTextedCalledQuery(dealershipDBInfo.internal_code, startDate, endDate));
    const opportunityAppointments = dbConnection.query(getAppointmentsQuery(dealershipDBInfo.internal_code, startDate, endDate));
    const opportunityROInfo = dbConnection.query(getRepairOrderRevenueQuery(dealershipDBInfo.internal_code, startDate, endDate));

    results = await Promise.all([opportunities, opportunitiesContacted, opportunityAppointments, opportunityROInfo])
      .then((queryResults) => {
        const consolidatedResults: ReturnedResult[] = [];

        queryResults.forEach((queryResult: ReturnedResult[]) => {
          queryResult.forEach((result: ReturnedResult) => {
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

/**
* Local testing
*/
const startDate = new Date('2021-12-01');
const endDate = new Date('2021-12-31');
// const startDate = new Date();
// startDate.setFullYear(startDate.getFullYear() - 2);
// const endDate = new Date();
// const startDate = new Date();
// startDate.setDate(startDate.getDate() - 90);
// const endDate = new Date();

const event: Event = {
  // dealershipIds: ['e108cd88-bea5-f4af-11ac-574465d1fd2f'],
  // dealershipIds: ['c5930e0c-72d6-4cd4-bfdf-d74db1d0ce38'],
  dealershipIds: ['e108cd88-bea5-f4af-11ac-574465d1fd2f', 'c5930e0c-72d6-4cd4-bfdf-d74db1d0ce38'],
  startDate: startDate,
  endDate: endDate,
};

toyotaRecallReports(event, {}, (error: any, response: any) => {
  return response ? console.log('Response:', response) : console.log('Error:', error);
});
