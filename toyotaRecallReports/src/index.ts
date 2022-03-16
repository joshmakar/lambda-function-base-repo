// Require promise-mysql
import promise from 'promise-mysql';
import {
    getOpportunitiesTextedCalledQuery,
    getAppointmentsQuery,
    getRepairOrderRevenueQuery,
    getOpportunitiesContactedQuery,
} from './queries/temp';

if (process.env['NODE_ENV'] !== 'production') {
    require('dotenv').config();
}

const toyotaRecallReports = async (payload: any, context: any, callback: any) => {
    console.log(`Function toyotaRecallReports called with payload ${JSON.stringify(payload)}`);

    const dbConnection = await promise.createConnection({
        host: payload.host,
        user: payload.user,
        port: payload.port ?? 3306,
        password: payload.password,
        database: payload.database,
        timeout: 6000,
    });

    let results: any[] = [];

    try {
        console.log('trying');
        const dealerIntegralinkCode = 99999;
        const startDate = new Date();
        startDate.setFullYear(startDate.getFullYear() - 2);
        const endDate = new Date();

        results.push(await dbConnection.query(getOpportunitiesContactedQuery(dealerIntegralinkCode, startDate, endDate)));
        results.push(await dbConnection.query(getOpportunitiesTextedCalledQuery(dealerIntegralinkCode, startDate, endDate)));
        results.push(await dbConnection.query(getAppointmentsQuery(dealerIntegralinkCode, startDate, endDate)));
        results.push(await dbConnection.query(getRepairOrderRevenueQuery(dealerIntegralinkCode, startDate, endDate)));
    } catch (error) {
        console.log(error);
    } finally {
        // Close the db connection, even if there's an error. This avoids a hanging process.
        await dbConnection.end();
    }

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
}

toyotaRecallReports({}, {}, (err: any, res: any) => {
    console.log('response', res);
});
