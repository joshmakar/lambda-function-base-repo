import mysql from 'promise-mysql';

// Import query functions
import {
  getOpportunitiesTextedCalledQuery,
  getAppointmentsQuery,
  getRepairOrderRevenueQuery,
  getOpportunitiesContactedQuery,
  getDealershipsDBInfo,
} from './queries/temp';

// Import interfaces
import { SelectDealerDBInfoResult } from './interfaces/SelectedDealerDBInfoResult';

// Configure environment variables if not in production
if (process.env['NODE_ENV'] !== 'production') {
  require('dotenv').config();
}

const toyotaRecallReports = async (event: any, context: any, callback: any) => {
  // Check that the event contains one or more dealershipIds
  if (!event.dealershipIds || !event.dealershipIds.length) {
    callback('Please provide at least one dealershipId');
    return;
  }

  // Check that the event contains a startDate and endDate
  if (!event.startDate || !event.endDate) {
    callback('Please provide a startDate and endDate');
    return;
  }

  // Check required environment variables
  ['UNOTIFI_COM_INDEX_DB_HOST', 'UNOTIFI_COM_INDEX_DB_USER', 'UNOTIFI_COM_INDEX_DB_PASS'].forEach(envVar => {
    if (!process.env[envVar]) {
      callback(`Please set ${envVar} in your environment`);
      return;
    }
  });

  // Connection for the Unotifi Index db to get the dealer db credentials
  const indexDbConn = await mysql.createConnection({
    host: process.env['UNOTIFI_COM_INDEX_DB_HOST'],
    user: process.env['UNOTIFI_COM_INDEX_DB_USER'],
    password: process.env['UNOTIFI_COM_INDEX_DB_PASS'],
    database: 'unotifi_com_index',
    timeout: 60000,
  });

  const safeDealerIds = event.dealershipIds.map((id: any) => mysql.escape(id));

  const dealershipsConnections: SelectDealerDBInfoResult[] = await indexDbConn.query(getDealershipsDBInfo(safeDealerIds));

  indexDbConn.end();

  // Convert string date to Date object
  const startDate = new Date(event.startDate);
  const endDate = new Date(event.endDate);

  // get the RO Level report data
  const results = await Promise.all(
    dealershipsConnections.map((connection) => {
      return getReportRowForDealerRoLevel(connection, startDate, endDate);
    })
  );
  const resultsSuccess: any[] = [];
  results[0]!.forEach((result: any) => {
    // console.log('result', result);
    const resultSuccess: any = {
      dealershipName: result.dealershipName,
      autoCampaignName: result.autoCampaignName,
      totalOpportunities: result.totalOpportunities ?? 0,
      totalOpportunitiesContacted: result.totalOpportunitiesContacted ?? 0,
      totalOpportunitiesTexted: result.totalOpportunitiesTexted ?? 0,
      totalOpportunitiesCalled: result.totalOpportunitiesCalled ?? 0,
      // percentageOfOpportunitiesContacted: '',
      // percentAppointments: '',
      totalAppointments: result.totalAppointments ?? 0,
      totalAppointmentsArrived: result.totalAppointmentsArrived ?? 0,
      soldVehicles: result.soldVehicles ?? 0,
      totalRepairOrders: result.roNo ?? 0,
      revenue: result.roAmount ?? 0,
      // averageROValue: '',
      // showRate: '',
    };
    resultsSuccess.push(resultSuccess);
  });

  const createCsvWriter = require('csv-writer').createObjectCsvWriter;
  const fileInfo = 'toyotaRecallReports';

  const csvWriterSuccessResults = createCsvWriter({
    path: `${fileInfo}-results_success-makar.csv`,
    header: [
      { id: 'dealershipName', title: 'Dealership Name' },
      { id: 'autoCampaignName', title: 'Campaign Name' },
      { id: 'totalOpportunities', title: 'Total Opportunities' },
      { id: 'totalOpportunitiesContacted', title: 'Total Opportunities Contacted' },
      { id: 'totalOpportunitiesTexted', title: 'Total Opportunities Texted' },
      { id: 'totalOpportunitiesCalled', title: 'Total Opportunities Called' },
      // { id: 'percentageOfOpportunitiesContacted', title: 'Percentage of Opportunities Contacted' },
      // { id: 'percentAppointments', title: 'Percent Appointments' },
      { id: 'totalAppointments', title: 'Total Appointments' },
      { id: 'totalAppointmentsArrived', title: 'Total Appointments Arrived' },
      { id: 'totalRepairOrders', title: 'Total Repair Orders' },
      { id: 'revenue', title: 'Revenue' },
      // { id: 'averageROValue', title: 'Average RO Value' },
      // { id: 'showRate', title: 'Show Rate' },
      { id: 'soldVehicles', title: 'Sold Vehicles' },
    ]
  });

  csvWriterSuccessResults.writeRecords(resultsSuccess)
    .then(() => {
        console.log('Saved results_success.csv');
    });

  callback(null, {
    statusCode: 201,
    // body: JSON.stringify(results, null, 2),
    body: '',
    headers: {
      'X-Custom-Header': 'ASDF'
    }
  });
}

module.exports = {
  toyotaRecallReports,
};

async function getReportRowForDealerRoLevel(dealerDbConnInfo: SelectDealerDBInfoResult, startDate: Date, endDate: Date) {
  const dbConnection = await mysql.createConnection({
    host: dealerDbConnInfo.IP || '',
    user: dealerDbConnInfo.user || '',
    password: dealerDbConnInfo.password || '',
    database: dealerDbConnInfo.name || '',
    timeout: 60000,
  });

  let results: any[] = [];

  try {
    const opportunities = dbConnection.query(getOpportunitiesContactedQuery(dealerDbConnInfo.internal_code, startDate, endDate));
    const opportunitiesContacted = dbConnection.query(getOpportunitiesTextedCalledQuery(dealerDbConnInfo.internal_code, startDate, endDate));
    const opportunityAppointments = dbConnection.query(getAppointmentsQuery(dealerDbConnInfo.internal_code, startDate, endDate));
    const opportunityROInfo = dbConnection.query(getRepairOrderRevenueQuery(dealerDbConnInfo.internal_code, startDate, endDate));
    results = await Promise.all([opportunities, opportunitiesContacted, opportunityAppointments, opportunityROInfo])
      .then((queryResults) => {
        const consolidatedResults: any[] = [];

        queryResults.forEach((queryResult: any) => {
          console.log('result', queryResult);
          queryResult.forEach((result: any) => {
            // Find index in consolidatedResults by autoCampaignName
            const index = consolidatedResults.findIndex((item: any) => item.autoCampaignName === result.autoCampaignName);
            // If not found, create new object
            if (index === -1) {
              consolidatedResults.push({
                dealershipName: dealerDbConnInfo.dealerName,
                ...result,
              });
            } else {
              // If found, update existing object
              consolidatedResults[index] = {
                dealershipName: dealerDbConnInfo.dealerName,
                ...consolidatedResults[index],
                ...result,
              };
            }
          });
        });

        console.log('consolidatedResults', consolidatedResults);

        return consolidatedResults;
      });
    
    // console.log(results[3]);
    // console.log(items);
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

const event = {
  // dealershipIds: ['e108cd88-bea5-f4af-11ac-574465d1fd2f'],
  dealershipIds: ['c5930e0c-72d6-4cd4-bfdf-d74db1d0ce38'],
  // dealershipIds: ['e108cd88-bea5-f4af-11ac-574465d1fd2f', 'c5930e0c-72d6-4cd4-bfdf-d74db1d0ce38'],
  startDate: startDate,
  endDate: endDate,
};

toyotaRecallReports(event, {}, (error: any, response: any) => {
  return console.log('fin');
  return response ? console.log('Response:', response) : console.log('Error:', error);
});
