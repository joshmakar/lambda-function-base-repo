// Require promise-mysql
import mysql from 'promise-mysql';
import {
  getOpportunitiesTextedCalledQuery,
  getAppointmentsQuery,
  getRepairOrderRevenueQuery,
  getOpportunitiesContactedQuery,
  getDealershipsDBInfo,
} from './queries/temp';
import { SelectDealerDBInfoResult } from './interfaces/SelectedDealerDBInfoResult';

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

  const dealershipsConnections: SelectDealerDBInfoResult[] = (
    await indexDbConn.query(getDealershipsDBInfo(safeDealerIds))
  ) as SelectDealerDBInfoResult[];

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

  callback(null, {
    statusCode: 201,
    body: JSON.stringify({
      message: results
    }),
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
    results.push(await dbConnection.query(getOpportunitiesContactedQuery(dealerDbConnInfo.internal_code, startDate, endDate)));
    results.push(await dbConnection.query(getOpportunitiesTextedCalledQuery(dealerDbConnInfo.internal_code, startDate, endDate)));
    results.push(await dbConnection.query(getAppointmentsQuery(dealerDbConnInfo.internal_code, startDate, endDate)));
    results.push(await dbConnection.query(getRepairOrderRevenueQuery(dealerDbConnInfo.internal_code, startDate, endDate)));
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
const startDate = new Date();
startDate.setFullYear(startDate.getFullYear() - 2);
const endDate = new Date();

const event = {
  // dealershipIds: ['e108cd88-bea5-f4af-11ac-574465d1fd2f'],
  startDate: startDate,
  endDate: endDate,
};

toyotaRecallReports(event, {}, (err: any, res: any) => {
  console.log('response', res);
});
