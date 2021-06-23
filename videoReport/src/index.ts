import * as mysql from 'promise-mysql';

/**
 * This is the entry point for the Lambda function
 */
export async function handler(event?: VideoReportEvent) {
    // Check required input
    if (!event?.dealerIDs?.length) {
        throw new Error('Missing dealerIDs in event body')
    }
    if (!event?.emailRecipients?.length) {
        throw new Error('Missing emailRecipients in event body')
    }
    if (event.startDate?.length != 10 || event.endDate?.length != 10) {
        throw new Error('startDate and endDate must be valid YYYY-MM-DD dates in event body')
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
        database: 'unotifi_com_index'
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
        const rows = await Promise.all(dealerInfoResult.map(getReportRowForDealer))
        console.table(rows)

        await indexDbConn.end()

        // TODO return csv link in s3
        return 'Done!';
    } catch (err) {
        // Close the db connection, even if there's an error. This avoids a hanging process.
        await indexDbConn.end()
        throw err
    }
};

/**
 * Concurrently executes all aggregate queries for a dealer. This function should also be called concurrently for each dealer (e.g. using Promise.all).
 */
export async function getReportRowForDealer(dealerDbConnInfo: SelectDealerDbInfoResult): Promise<ReportRow> {
    const reportRow: ReportRow = {
        'Dealer ID': dealerDbConnInfo.iddealer,
        'Dealer Name': dealerDbConnInfo.dealerName || ''
    };

    // Connection to the dealer database (aka sugarcrm database) for subsequent aggregate queries
    const dealerDbConn = await mysql.createConnection({
        host: dealerDbConnInfo.IP || '',
        user: dealerDbConnInfo.user || '',
        password: dealerDbConnInfo.password || '',
        database: dealerDbConnInfo.name || ''
    });

    // This is just a placeholder function to run a simple aggregate query. In practice, we'll need one of these functions for each column of the report.
    const countQuery = async (table: string): Promise<number | undefined> => {
        // Typecasting (`as` keyword) here because mysql.query types don't allow you to pass in a type argument...
        const countResult = await dealerDbConn.query(`SELECT count(id) as count FROM ${table}`) as { count: number }[]
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
            countQuery('auto_repair_order'),
            countQuery('auto_appointment')
        ])

        console.log(roCountResult)

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
export interface ReportRow {
    'Dealer Name'?: string;
    'Dealer ID'?: string;
    'Repair Order Count'?: number;
    'Appointment Count'?: number
}

/**
 * JSON input of the lambda function
 */
export interface VideoReportEvent {
    dealerIDs?: string[],
    emailRecipients?: string[],
    startDate?: string,
    endDate?: string
}

/**
 * Result of the query that fetches dealer db connection info
 */
export interface SelectDealerDbInfoResult {
    iddealer: string;
    dealerName: string | null;
    name: string | null;
    user: string | null;
    password: string | null;
    IP: string | null;
}
