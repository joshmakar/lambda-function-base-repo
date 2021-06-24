import * as mysql from 'promise-mysql';
import stringify from 'csv-stringify/lib/sync'
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { v4 } from 'uuid';

const s3Client = new S3Client({ region: 'us-east-1' });
const reportBucket = "unotifi-reports"

/**
 * This is the entry point for the Lambda function
 */
export async function handler(event?: VideoReportEvent) {
    // Fore setting defaults and file upload name
    const todayYMD = (new Date()).toISOString().split('T')[0]!

    // Set input defaults
    const startDateObj = new Date();
    startDateObj.setMonth(startDateObj.getMonth() - 1);
    let startDateYMD = startDateObj.toISOString().split('T')[0]!
    let endDateYMD = todayYMD

    // Sanitize date overrides
    if (event?.startDate) {
        startDateYMD = new Date(event.startDate).toISOString().split('T')[0] || startDateYMD
    }
    if (event?.endDate) {
        endDateYMD = new Date(event.endDate).toISOString().split('T')[0] || endDateYMD
    }

    // Check required input
    if (!event?.dealerIDs?.length) {
        throw new Error('Missing dealerIDs in event body')
    }

    // Check required environment variables
    ['UNOTIFI_COM_INDEX_DB_HOST', 'UNOTIFI_COM_INDEX_DB_USER', 'UNOTIFI_COM_INDEX_DB_PASS'].forEach(envVar => {
        if (!process.env[envVar]) {
            throw new Error('Missing env var: ' + envVar)
        }
    })

    // Connection for the Unotifi Index db to get the dealer db credentials
    const indexDbConn = await mysql.createConnection({
        host: process.env['UNOTIFI_COM_INDEX_DB_HOST'],
        user: process.env['UNOTIFI_COM_INDEX_DB_USER'],
        password: process.env['UNOTIFI_COM_INDEX_DB_PASS'],
        database: 'unotifi_com_index',
        timeout: 5000
    });

    // I couldn't figure out how to paramaterize a WHERE IN array, so manually escape the array values
    const safeDealerIds = event.dealerIDs.map(id => mysql.escape(id)).join(',')
    try {
        const dealerInfoResult = (await indexDbConn.query(`
            SELECT dealer.iddealer, dealer.name as dealerName, database.name, database.user, database.password, databaseserver.IP FROM dealer 
            INNER JOIN instance ON instance.idinstance = dealer.instance_idinstance
            INNER JOIN \`database\` ON database.iddatabase = instance.database_iddatabase
            INNER JOIN databaseserver ON databaseserver.iddatabaseserver = database.databaseServer_iddatabaseServer
            WHERE iddealer IN (${safeDealerIds})
        `)) as SelectDealerDbInfoResult[];

        const rows = await Promise.all(dealerInfoResult.map(res => getReportRowForDealer(res, startDateYMD, endDateYMD)))

        await indexDbConn.end()

        // Generate the CSV string (contents of a csv file) using csv-generate's sync API. If this data set ever gets huge, we'll need to use the callback or stream API.
        const csvString = stringify(rows, { header: true })
        if ('' == '') {
            return 'db was not the problem'
        }

        // Upload that bad boy to S3
        // I just appended a random string in the top level folder name for a bit more obfuscation
        const filePath = `video-report-3KCe4kZqXCkpZdp4/video-report_${todayYMD}_${v4()}.csv`;
        await s3Client.send(new PutObjectCommand({
            Bucket: reportBucket,
            Key: filePath,
            Body: csvString
        }));
        const reportURL = `https://${reportBucket}.s3.amazonaws.com/${filePath}`;
        return {
            reportURL,
            reportData: rows
        };
    } catch (err) {
        // Close the db connection, even if there's an error. This avoids a hanging process.
        await indexDbConn.end()
        throw err
    }
};

/**
 * Concurrently executes all aggregate queries for a dealer. This function should also be called concurrently for each dealer (e.g. using Promise.all).
 */
async function getReportRowForDealer(dealerDbConnInfo: SelectDealerDbInfoResult, startDate: string, endDate: string): Promise<ReportRow> {
    const reportRow: ReportRow = {
        'Dealer ID': dealerDbConnInfo.iddealer,
        'Dealer Name': dealerDbConnInfo.dealerName || ''
    };

    // Connection to the dealer database (aka sugarcrm database) for subsequent aggregate queries
    const dealerDbConn = await mysql.createConnection({
        host: dealerDbConnInfo.IP || '',
        user: dealerDbConnInfo.user || '',
        password: dealerDbConnInfo.password || '',
        database: dealerDbConnInfo.name || '',
        timeout: 5000
    });

    // This is just a placeholder function to run a simple aggregate query. In practice, we'll need one of these functions for each column of the report.
    const countROQuery = async (): Promise<number | undefined> => {
        const countResult = await dealerDbConn.query(
            `SELECT count(id) as count FROM auto_repair_order WHERE service_closed_date BETWEEN ? AND ?`,
            [startDate, endDate]
            // Typecasting here because mysql.query types don't allow you to pass in a type argument...
        ) as { count: number }[]
        return countResult && countResult[0] ? countResult[0].count : undefined;
    }
    // This is just a placeholder function to run a simple aggregate query. In practice, we'll need one of these functions for each column of the report.
    const countApptQuery = async (): Promise<number | undefined> => {
        const countResult = await dealerDbConn.query(
            `SELECT count(id) as count FROM auto_appointment WHERE appointment_date BETWEEN ? AND ?`,
            [startDate, endDate]
            // Typecasting here because mysql.query types don't allow you to pass in a type argument...
        ) as { count: number }[]
        return countResult && countResult[0] ? countResult[0].count : undefined;
    }

    try {
        // Run each aggregate column query concurrently to save time.
        // Technically, mysql queries still run serially for a single connection, but it should at least put the burden of handling that
        // on a db server instead of this Node.js app.
        const [
            roCountResult,
            apptCountResult
        ] = await Promise.all([
            countROQuery(),
            countApptQuery()
        ])

        // Assign all the db results to the CSV row
        reportRow['Repair Order Count'] = roCountResult
        reportRow['Appointment Count'] = apptCountResult

        // Don't forget to end the end the db connection for a single dealer!
        await dealerDbConn.end()

        return reportRow
    } catch (err) {
        // Close the db connection, even if there's an error. This avoids a hanging process.
        await dealerDbConn.end()
        throw err
    }
}

/**
 * Represents a row of the output CSV file
 */
interface ReportRow {
    'Dealer Name'?: string;
    'Dealer ID'?: string;
    'Repair Order Count'?: number;
    'Appointment Count'?: number
}

/**
 * JSON input of the lambda function
 */
interface VideoReportEvent {
    dealerIDs?: string[],
    emailRecipients?: string[],
    startDate?: string,
    endDate?: string
}

/**
 * Result of the query that fetches dealer db connection info
 */
interface SelectDealerDbInfoResult {
    iddealer: string;
    dealerName: string | null;
    name: string | null;
    user: string | null;
    password: string | null;
    IP: string | null;
}
