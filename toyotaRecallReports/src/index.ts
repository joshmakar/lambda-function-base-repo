'use strict'

// Require promise-mysql
import promise from 'promise-mysql';

const toyotaRecallReports = async (payload: any, context: any, callback: any) => {
    console.log(`Function toyotaRecallReports called with payload ${JSON.stringify(payload)}`);

    const result = await promise.createConnection({
        host: 'unotifi-web-application.chkwbdrblapr.us-east-1.rds.amazonaws.com',
        user: 'unotifi_web_stag',
        port: 3306,
        password: '8vFN^xY%#eeRV^87GbsW',
        database: 'unotifi_web_application',
        timeout: 6000,
    });

    try {
        console.log('trying');
        var test: any = await result.query('SELECT * FROM users LIMIT 1');
    } catch (error) {
        var test: any = 'fail';
    }

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

toyotaRecallReports({}, {}, (err: any, res: any) => {
    console.log('response', res);
});
