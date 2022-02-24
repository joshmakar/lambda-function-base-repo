'use strict'

// Require promise-mysql
const promise = require('promise-mysql');

const toyotaRecallReports = (payload, context, callback) => {
    console.log(`Function toyotaRecallReports called with payload ${JSON.stringify(payload)}`);

    return promise.createConnection({
        host: 'unotifi-web-application.chkwbdrblapr.us-east-1.rds.amazonaws.com',
        user: 'unotifi_web_stag',
        port: 3306,
        password: '8vFN^xY%#eeRV^87GbsW',
        database: 'unotifi_web_application',
        timeout: 60000,
    }).then(result => {
        console.log('Connected to MySQL');
        // Perform query to show tables
        result.query('SELECT * FROM users LIMIT 10', function (error, results, fields) {
            callback(null, {
                statusCode: 201,
                body: JSON.stringify({
                    message: results, error, fields
                }),
                headers: {
                    'X-Custom-Header': 'ASDF'
                }
            });
          });
    });

    
}

module.exports = {
    toyotaRecallReports,
}
