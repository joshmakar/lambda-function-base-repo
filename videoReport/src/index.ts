import * as mysql from 'promise-mysql';
import { Dealer } from '../../types/db/unotifiComIndex'

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
        const result = (await indexDbConn.query(`SELECT * FROM dealer WHERE iddealer IN (${safeDealerIds})`)) as Dealer[];
        console.log(JSON.stringify(result[0]))
    } catch (err) {
        await indexDbConn.end()
        throw err
    }

    await indexDbConn.end()

    return 'Done!';
};

export type VideoReportEventUnchecked = Partial<VideoReportEvent>;
export interface VideoReportEvent {
    dealerIDs: string[],
    emailRecipients: string[],
    startDate: string,
    endDate: string
}
