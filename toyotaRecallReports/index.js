'use strict'

// Require promise-mysql
const promise = require('promise-mysql');

const toyotaRecallReports = async (payload, context, callback) => {
    console.log(`Function toyotaRecallReports called with payload ${JSON.stringify(payload)}`);

    const result = await promise.createConnection({
        host: 'unotifi-web-application.chkwbdrblapr.us-east-1.rds.amazonaws.com',
        user: 'unotifi_web_stag',
        port: 3306,
        password: '8vFN^xY%#eeRV^87GbsW',
        database: 'unotifi_web_application',
        timeout: 60000,
    });

    var test = await result.query('SELECT * FROM users LIMIT 1');

    callback(null, {
        statusCode: 201,
        body: JSON.stringify({
            message: test
        }),
        headers: {
            'X-Custom-Header': 'ASDF'
        }
    });
}

module.exports = {
    toyotaRecallReports,
}
