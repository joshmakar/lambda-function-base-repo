import * as mysql from 'promise-mysql';
import * as Bluebird from 'bluebird';

/**
 * This is the entry point for the Lambda function
 */
export async function handler(event?: VideoReportEventUnchecked) {
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

    const indexDbConn = await mysql.createConnection({
        host: process.env['UNOTIFI_COM_INDEX_DB_HOST'],
        user: process.env['UNOTIFI_COM_INDEX_DB_USER'],
        password: process.env['UNOTIFI_COM_INDEX_DB_PASS'],
        database: 'unotifi_com_index'
    });

    const safeDealerIds = event.dealerIDs.map(id => mysql.escape(id)).join(',')
    try {
        const result = (await indexDbConn.query(`
            SELECT dealer.iddealer, database.name, database.user, database.password, databaseserver.IP FROM dealer 
            INNER JOIN instance ON instance.idinstance = dealer.instance_idinstance
            INNER JOIN \`database\` ON database.iddatabase = instance.database_iddatabase
            INNER JOIN databaseserver ON databaseserver.iddatabaseserver = database.databaseServer_iddatabaseServer
            WHERE iddealer IN (${safeDealerIds})
        `)) as SelectDealerDbInfoResult[];
        const rows = await Promise.all(result.map(getReportDataForDealer))
        console.table(rows)
    } catch (err) {
        await indexDbConn.end()
        throw err
    }

    await indexDbConn.end()

    return 'Done!';
};

export async function getReportDataForDealer(dealerDbConnInfo: SelectDealerDbInfoResult): Promise<ReportRow> {
    const reportRow: ReportRow = {};
    const dealerDbConn = await mysql.createConnection({
        host: dealerDbConnInfo.IP || '',
        user: dealerDbConnInfo.user || '',
        password: dealerDbConnInfo.password || '',
        database: dealerDbConnInfo.name || ''
    });
    const [
        roCountResult,
        apptCountResult
    ] = await Promise.all([
        (dealerDbConn.query(`SELECT count(id) as count FROM auto_repair_order`)) as (Bluebird<{ count: number }[]>),
        (dealerDbConn.query(`SELECT count(id) as count FROM auto_appointment`)) as (Bluebird<{ count: number }[]>)
    ])
    console.log(roCountResult)
    reportRow['Repair Order Count'] = roCountResult[0] ? roCountResult[0].count : undefined
    reportRow['Appointment Count'] = apptCountResult[0] ? apptCountResult[0].count : undefined

    await dealerDbConn.end()
    return reportRow
}

export interface ReportRow {
    'Repair Order Count'?: number;
    'Appointment Count'?: number
}

export type VideoReportEventUnchecked = Partial<VideoReportEvent>;
export interface VideoReportEvent {
    dealerIDs: string[],
    emailRecipients: string[],
    startDate: string,
    endDate: string
}

export interface SelectDealerDbInfoResult {
    iddealer: string | null;
    name: string | null;
    user: string | null;
    password: string | null;
    IP: string | null;
}
