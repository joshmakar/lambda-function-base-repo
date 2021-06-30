import * as mysql from 'promise-mysql';
import stringify from 'csv-stringify/lib/sync'
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import sendgrid from '@sendgrid/mail'
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
    ['UNOTIFI_COM_INDEX_DB_HOST', 'UNOTIFI_COM_INDEX_DB_USER', 'UNOTIFI_COM_INDEX_DB_PASS', 'SENDGRID_API_KEY'].forEach(envVar => {
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
        timeout: 60000
    });

    // I couldn't figure out how to paramaterize a WHERE IN array, so manually escape the array values
    const safeDealerIds = event.dealerIDs.map(id => mysql.escape(id)).join(',')
    try {
        const dealerInfoResult = (await indexDbConn.query(`
            SELECT dealer.iddealer, dealer.internal_code, dealer.name as dealerName, database.name, database.user, database.password, databaseserver.IP FROM dealer 
            INNER JOIN instance ON instance.idinstance = dealer.instance_idinstance
            INNER JOIN \`database\` ON database.iddatabase = instance.database_iddatabase
            INNER JOIN databaseserver ON databaseserver.iddatabaseserver = database.databaseServer_iddatabaseServer
            WHERE iddealer IN (${safeDealerIds})
        `)) as SelectDealerDbInfoResult[];

        const rows = await Promise.all(dealerInfoResult.map(res => getReportRowForDealer(res, startDateYMD, endDateYMD)))

        // String if results are uploaded as a csv, null otherwise
        let reportURL: string | null = null;

        // If email recipients are set, create a csv, upload it to s3, and email a link to the recipients
        if (event.emailRecipients?.length) {
            // Generate the CSV string (contents of a csv file) using csv-generate's sync API. If this data set ever gets huge, we'll need to use the callback or stream API.
            const csvString = stringify(rows, { header: true })

            // Upload that bad boy to S3
            // I just appended a random string in the top level folder name for a bit more obfuscation
            const filePath = `video-report-3KCe4kZqXCkpZdp4/video-report_${todayYMD}_${v4()}.csv`;
            await s3Client.send(new PutObjectCommand({
                Bucket: reportBucket,
                Key: filePath,
                Body: csvString
            }));
            reportURL = `https://${reportBucket}.s3.amazonaws.com/${filePath}`;

            sendgrid.setApiKey(process.env['SENDGRID_API_KEY'] + '')
            await sendgrid.send({
                to: event.emailRecipients,
                from: 'donotreply@unotifi.com',
                subject: 'Dealer ROI level Video Report from Unotifi',
                // If we ever need this content to be updated for non-Audi dealers, we should use sendgrid templates
                text: `Start Date: ${startDateYMD}\nEnd Date: ${endDateYMD}\nReport Download URL: ${reportURL}`,
                html: `
                    <p>
                        Please click <a href="${reportURL}">this link</a> to download the month end Audi Dealer level Video Report for dealers on Unotifi.
                        <br />Start Date: ${startDateYMD}
                        <br />End Date: ${endDateYMD}
                    </p>
                    <p>
                        <br />Thanks – Unotifi
                        <br /><a href="mailto:support@unotifi.com">support@unotifi.com</a>
                        <br /><a href="http://www.unotificrm.com">www.unotificrm.com</a>
                        <br />866-500-6161
                    </p>
                `,
            });
        }

        await indexDbConn.end()
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
        timeout: 60000
    });

    try {
        // Run each aggregate column query concurrently to save time.
        // Technically, mysql queries still run serially for a single connection, but it should at least put the burden of handling that
        // on a db server instead of this Node.js app.
        const [
            closedROCount,
            totalROsWithVideoCount,
            avgLabor,
            avgParts,
            avgROClosed,
            numberSMSSent,
            numberMediaSent,
        ] = await Promise.all([
            countROQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            countROWithVideosQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            avgLaborQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            avgPartsQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            avgROClosedQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            numberSMSSentQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            numberMediaSentQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
        ])

        // Assign all the db results to the CSV row
        reportRow['Total # of Closed ROs (CP + WP)'] = closedROCount
        reportRow['Number of ROs Containing AT LEAST one Tech Video'] = totalROsWithVideoCount
        reportRow['Average CP Labor $'] = avgLabor;
        reportRow['Average CP Parts $'] = avgParts;
        reportRow['Average RO Closed Value'] = avgROClosed;
        reportRow['Number of SMSs Sent to Customer'] = numberSMSSent;
        reportRow['Number of Media Sent to Customer'] = numberMediaSent;

        // Don't forget to end the end the db connection for a single dealer!
        await dealerDbConn.end()

        return reportRow
    } catch (err) {
        // Close the db connection, even if there's an error. This avoids a hanging process.
        await dealerDbConn.end()
        throw err
    }
}

async function countROQuery(conn: mysql.Connection, dealerID: string, startDate: string, endDate: string) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(`
        SELECT
            COUNT(*) AS total
        FROM
            (
                SELECT
                    auto_repair_order.id
                FROM
                    auto_dealer
                    INNER JOIN auto_custom_auto_dealer_c ON auto_custom_auto_dealer_c.auto_custo60bd_dealer_ida = auto_dealer.id
                    AND auto_custom_auto_dealer_c.deleted = 0
                    INNER JOIN auto_customer ON auto_custom_auto_dealer_c.auto_custo0932ustomer_idb = auto_customer.id
                    AND auto_customer.deleted = 0
                    INNER JOIN auto_vehicluto_customer_c ON auto_vehicluto_customer_c.auto_vehic9275ustomer_ida = auto_customer.id
                    AND auto_vehicluto_customer_c.deleted = 0
                    INNER JOIN auto_vehicle ON auto_vehicluto_customer_c.auto_vehic831dvehicle_idb = auto_vehicle.id
                    AND auto_vehicle.deleted = 0
                    INNER JOIN auto_repairauto_vehicle_c ON auto_repairauto_vehicle_c.auto_repai4169vehicle_ida = auto_vehicle.id
                    AND auto_repairauto_vehicle_c.deleted = 0
                    INNER JOIN auto_repair_order ON auto_repairauto_vehicle_c.auto_repai527cr_order_idb = auto_repair_order.id
                    AND auto_repair_order.deleted = 0
                    INNER JOIN auto_ro_labrepair_order_c AS aro_lab_pivot ON aro_lab_pivot.auto_ro_laada9r_order_ida = auto_repair_order.id
                    INNER JOIN auto_ro_labor AS labor ON aro_lab_pivot.auto_ro_la1301o_labor_idb = labor.id
                WHERE
                    1 = 1
                    AND COALESCE(auto_repair_order.technician_id, '') != ''
                    AND auto_repair_order.service_open_date BETWEEN ? AND ?
                    AND auto_dealer.integralink_code = ?
                GROUP BY
                    auto_repair_order.id
            ) as aros
        `,
        [startDate, endDate, dealerID]
    );
    return countResult && countResult[0] ? countResult[0].total : undefined;
}

async function countROWithVideosQuery(conn: mysql.Connection, dealerID: string, startDate: string, endDate: string) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(`
        SELECT
            COUNT(*) AS total
        FROM
            (
                SELECT
                    auto_repair_order.id
                FROM
                    auto_dealer
                    INNER JOIN auto_custom_auto_dealer_c ON auto_custom_auto_dealer_c.auto_custo60bd_dealer_ida = auto_dealer.id
                    AND auto_custom_auto_dealer_c.deleted = 0
                    INNER JOIN auto_customer ON auto_custom_auto_dealer_c.auto_custo0932ustomer_idb = auto_customer.id
                    AND auto_customer.deleted = 0
                    INNER JOIN auto_vehicluto_customer_c ON auto_vehicluto_customer_c.auto_vehic9275ustomer_ida = auto_customer.id
                    AND auto_vehicluto_customer_c.deleted = 0
                    INNER JOIN auto_vehicle ON auto_vehicluto_customer_c.auto_vehic831dvehicle_idb = auto_vehicle.id
                    AND auto_vehicle.deleted = 0
                    INNER JOIN auto_repairauto_vehicle_c ON auto_repairauto_vehicle_c.auto_repai4169vehicle_ida = auto_vehicle.id
                    AND auto_repairauto_vehicle_c.deleted = 0
                    INNER JOIN auto_repair_order ON auto_repairauto_vehicle_c.auto_repai527cr_order_idb = auto_repair_order.id
                    AND auto_repair_order.deleted = 0
                    INNER JOIN auto_ro_labrepair_order_c AS aro_lab_pivot ON aro_lab_pivot.auto_ro_laada9r_order_ida = auto_repair_order.id
                    INNER JOIN auto_ro_labor AS labor ON aro_lab_pivot.auto_ro_la1301o_labor_idb = labor.id
                WHERE
                    1 = 1
                    AND COALESCE(auto_repair_order.technician_id, '') != ''
                    AND auto_repair_order.service_open_date BETWEEN ? AND ?
                    AND auto_dealer.integralink_code = ?
                    AND auto_repair_order.has_videos = 1
                GROUP BY
                    auto_repair_order.id
            ) as aros
        `,
        [startDate, endDate, dealerID]
    );
    return countResult && countResult[0] ? countResult[0].total : undefined;
}

async function avgLaborQuery(conn: mysql.Connection, dealerID: string, startDate: string, endDate: string) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(
        `
            SELECT
                AVG(REPLACE(aro_sums.total_labor, ',', '')) AS total
            FROM
                (
                    SELECT
                        SUM(labor.labor_amount) AS total_labor
                    FROM
                        auto_dealer
                        INNER JOIN auto_custom_auto_dealer_c ON auto_custom_auto_dealer_c.auto_custo60bd_dealer_ida = auto_dealer.id
                        AND auto_custom_auto_dealer_c.deleted = 0
                        INNER JOIN auto_customer ON auto_custom_auto_dealer_c.auto_custo0932ustomer_idb = auto_customer.id
                        AND auto_customer.deleted = 0
                        INNER JOIN auto_vehicluto_customer_c ON auto_vehicluto_customer_c.auto_vehic9275ustomer_ida = auto_customer.id
                        AND auto_vehicluto_customer_c.deleted = 0
                        INNER JOIN auto_vehicle ON auto_vehicluto_customer_c.auto_vehic831dvehicle_idb = auto_vehicle.id
                        AND auto_vehicle.deleted = 0
                        INNER JOIN auto_repairauto_vehicle_c ON auto_repairauto_vehicle_c.auto_repai4169vehicle_ida = auto_vehicle.id
                        AND auto_repairauto_vehicle_c.deleted = 0
                        INNER JOIN auto_repair_order ON auto_repairauto_vehicle_c.auto_repai527cr_order_idb = auto_repair_order.id
                        AND auto_repair_order.deleted = 0
                        INNER JOIN auto_ro_labrepair_order_c AS aro_lab_pivot ON aro_lab_pivot.auto_ro_laada9r_order_ida = auto_repair_order.id
                        INNER JOIN auto_ro_labor AS labor ON aro_lab_pivot.auto_ro_la1301o_labor_idb = labor.id
                    WHERE
                        COALESCE(auto_repair_order.technician_id, '') != ''
                        AND auto_repair_order.service_open_date BETWEEN ? AND ?
                        AND auto_dealer.integralink_code = ?
                    GROUP BY
                        auto_repair_order.name
                ) as aro_sums;
        `,
        [startDate, endDate, dealerID]
    );
    return countResult && countResult[0] ? countResult[0].total : undefined;
}

async function avgPartsQuery(conn: mysql.Connection, dealerID: string, startDate: string, endDate: string) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(
        `
            SELECT
                AVG(REPLACE(aro_sums.total_parts, ',', '')) AS total
            FROM
                (
                    SELECT SUM(labor.parts_amount) AS total_parts
                    FROM auto_dealer
                    
                    INNER JOIN auto_custom_auto_dealer_c
                    ON auto_custom_auto_dealer_c.auto_custo60bd_dealer_ida = auto_dealer.id
                    AND auto_custom_auto_dealer_c.deleted = 0
                    
                    INNER JOIN auto_customer
                    ON auto_custom_auto_dealer_c.auto_custo0932ustomer_idb = auto_customer.id
                    AND auto_customer.deleted = 0
                    
                    INNER JOIN auto_vehicluto_customer_c
                    ON auto_vehicluto_customer_c.auto_vehic9275ustomer_ida = auto_customer.id
                    AND auto_vehicluto_customer_c.deleted = 0
                    
                    INNER JOIN auto_vehicle
                    ON auto_vehicluto_customer_c.auto_vehic831dvehicle_idb = auto_vehicle.id
                    AND auto_vehicle.deleted = 0
                    
                    INNER JOIN auto_repairauto_vehicle_c
                    ON auto_repairauto_vehicle_c.auto_repai4169vehicle_ida = auto_vehicle.id
                    AND auto_repairauto_vehicle_c.deleted = 0
                    
                    INNER JOIN auto_repair_order
                    ON auto_repairauto_vehicle_c.auto_repai527cr_order_idb = auto_repair_order.id
                    AND auto_repair_order.deleted = 0
                    
                    INNER JOIN auto_ro_labrepair_order_c AS aro_lab_pivot ON aro_lab_pivot.auto_ro_laada9r_order_ida = auto_repair_order.id
                    INNER JOIN auto_ro_labor AS labor ON aro_lab_pivot.auto_ro_la1301o_labor_idb = labor.id
                    
                    WHERE COALESCE(auto_repair_order.technician_id, '') != ''
                    AND auto_repair_order.service_open_date BETWEEN ? AND ?
                    AND auto_dealer.integralink_code = ?
                    
                    GROUP BY auto_repair_order.name
                ) as aro_sums;
        `,
        [startDate, endDate, dealerID]
    );
    return countResult && countResult[0] ? countResult[0].total : undefined;
}

async function avgROClosedQuery(conn: mysql.Connection, dealerID: string, startDate: string, endDate: string) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(
        `
            SELECT
                AVG(REPLACE(totals.repair_order_amount_total, ',', '')) AS total
            FROM
                (
                    SELECT
                        auto_repair_order.id,
                        auto_repair_order.repair_order_amount_total
                    FROM
                        auto_dealer
                        INNER JOIN auto_custom_auto_dealer_c ON auto_custom_auto_dealer_c.auto_custo60bd_dealer_ida = auto_dealer.id
                        AND auto_custom_auto_dealer_c.deleted = 0
                        INNER JOIN auto_customer ON auto_custom_auto_dealer_c.auto_custo0932ustomer_idb = auto_customer.id
                        AND auto_customer.deleted = 0
                        INNER JOIN auto_vehicluto_customer_c ON auto_vehicluto_customer_c.auto_vehic9275ustomer_ida = auto_customer.id
                        AND auto_vehicluto_customer_c.deleted = 0
                        INNER JOIN auto_vehicle ON auto_vehicluto_customer_c.auto_vehic831dvehicle_idb = auto_vehicle.id
                        AND auto_vehicle.deleted = 0
                        INNER JOIN auto_repairauto_vehicle_c ON auto_repairauto_vehicle_c.auto_repai4169vehicle_ida = auto_vehicle.id
                        AND auto_repairauto_vehicle_c.deleted = 0
                        INNER JOIN auto_repair_order ON auto_repairauto_vehicle_c.auto_repai527cr_order_idb = auto_repair_order.id
                        AND auto_repair_order.deleted = 0
                        INNER JOIN auto_ro_labrepair_order_c AS aro_lab_pivot ON aro_lab_pivot.auto_ro_laada9r_order_ida = auto_repair_order.id
                        INNER JOIN auto_ro_labor AS labor ON aro_lab_pivot.auto_ro_la1301o_labor_idb = labor.id
                    WHERE
                        1 = 1
                        AND COALESCE(auto_repair_order.technician_id, '') != ''
                        AND auto_repair_order.service_open_date BETWEEN ? AND ?
                        AND auto_dealer.integralink_code = ?
                    GROUP BY
                        auto_repair_order.id
                ) as totals
        `,
        [startDate, endDate, dealerID]
    );
    return countResult && countResult[0] ? countResult[0].total : undefined;
}

async function numberSMSSentQuery(conn: mysql.Connection, dealerID: string, startDate: string, endDate: string) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(
        `
            SELECT
                count(auto_event.id) as total
            FROM
                auto_event
                INNER JOIN users ON auto_event.modified_user_id = users.id
                INNER JOIN auto_contact_person ON auto_contact_person.user_id_c = users.id
                    AND auto_contact_person.deleted = 0
                LEFT JOIN auto_event_to_recipient_c ON auto_event_to_recipient_c.auto_eventfa83o_event_idb = auto_event.id
                    AND auto_event_to_recipient_c.deleted = 0
                INNER JOIN auto_contac_auto_dealer_c ON auto_contac_auto_dealer_c.auto_contaff8f_person_idb = auto_contact_person.id
                    AND auto_contac_auto_dealer_c.deleted = 0
                INNER JOIN auto_dealer ON auto_contac_auto_dealer_c.auto_contafb84_dealer_ida = auto_dealer.id
                    AND auto_dealer.deleted = 0
            WHERE
                auto_event.type = 'Not-Pending'
                AND auto_event.date_entered BETWEEN ? AND ?
                AND integralink_code = ?
        `,
        [startDate, endDate, dealerID]
    );
    return countResult && countResult[0] ? countResult[0].total : undefined;
}

async function numberMediaSentQuery(conn: mysql.Connection, dealerID: string, startDate: string, endDate: string) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(
        `
            SELECT
                count(auto_media_file.id) AS total
            FROM
                auto_media_file
                INNER JOIN auto_event ON auto_event.id = auto_media_file.event_id
                INNER JOIN users ON auto_event.modified_user_id = users.id
                INNER JOIN auto_contact_person ON auto_contact_person.user_id_c = users.id
                    AND auto_contact_person.deleted = 0
                LEFT JOIN auto_event_to_recipient_c ON auto_event_to_recipient_c.auto_eventfa83o_event_idb = auto_event.id
                    AND auto_event_to_recipient_c.deleted = 0
                INNER JOIN auto_contac_auto_dealer_c ON auto_contac_auto_dealer_c.auto_contaff8f_person_idb = auto_contact_person.id
                    AND auto_contac_auto_dealer_c.deleted = 0
                INNER JOIN auto_dealer ON auto_contac_auto_dealer_c.auto_contafb84_dealer_ida = auto_dealer.id
                    AND auto_dealer.deleted = 0
            WHERE
                auto_event.type = 'Not-Pending'
                AND auto_media_file.date_entered BETWEEN ? AND ?
                AND integralink_code = ?
        `,
        [startDate, endDate, dealerID]
    );
    return countResult && countResult[0] ? countResult[0].total : undefined;
}

type AggregateQueryResult = { total: number }[]

/**
 * Represents a row of the output CSV file
 */
interface ReportRow {
    'Dealer Name'?: string;
    'Dealer ID'?: string;
    'Total # of Closed ROs (CP + WP)'?: number;
    'Number of ROs Containing AT LEAST one Tech Video'?: number
    'Average CP Labor $'?: number;
    'Average CP Parts $'?: number;
    'Average RO Closed Value'?: number;
    'Number of SMSs Sent to Customer'?: number;
    'Number of Media Sent to Customer'?: number
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
    internal_code?: string;
    dealerName: string | null;
    name: string | null;
    user: string | null;
    password: string | null;
    IP: string | null;
}
