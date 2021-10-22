import * as mysql from 'promise-mysql';
import stringify from 'csv-stringify/lib/sync';
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import sendgrid from '@sendgrid/mail';
import { v4 } from 'uuid';
import moment from 'moment';

const s3Client = new S3Client({ region: 'us-east-1' });
const reportBucket = "unotifi-reports";

/**
 * This is the entry point for the Lambda function
 */
export async function handler(event?: VideoReportEvent) {
    // Fore setting defaults and file upload name
    const todayYMD = (new Date()).toISOString().split('T')[0]!;

    // Set input defaults
    const startDateObj = new Date();
    startDateObj.setMonth(startDateObj.getMonth() - 1);
    let startDateYMD = startDateObj.toISOString().split('T')[0]!;
    let endDateYMD = todayYMD;

    // Sanitize date overrides
    if (event?.startDate) {
        startDateYMD = new Date(event.startDate).toISOString().split('T')[0] || startDateYMD;
    }
    if (event?.endDate) {
        endDateYMD = new Date(event.endDate).toISOString().split('T')[0] || endDateYMD;
    } else {
        // set end date to yesterday
        let date = new Date();
        endDateYMD = new Date(date.setDate(date.getDate()-1)).toISOString().split('T')[0] || endDateYMD;
    } 

    // Check required input
    if (!event?.dealerIDs?.length) {
        throw new Error('Missing dealerIDs in event body');
    }

    // Check required environment variables
    ['UNOTIFI_COM_INDEX_DB_HOST', 'UNOTIFI_COM_INDEX_DB_USER', 'UNOTIFI_COM_INDEX_DB_PASS', 'SENDGRID_API_KEY'].forEach(envVar => {
        if (!process.env[envVar]) {
            throw new Error('Missing env var: ' + envVar);
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

    // I couldn't figure out how to paramaterize a WHERE IN array, so manually escape the array values
    const safeDealerIds = event.dealerIDs.map(id => mysql.escape(id)).join(',');

    try {
        const dealerInfoResult = (await indexDbConn.query(`
            SELECT dealer.iddealer, dealer.internal_code, dealer.name as dealerName, database.name, database.user, database.password, databaseserver.IP FROM dealer 
            INNER JOIN instance ON instance.idinstance = dealer.instance_idinstance
            INNER JOIN \`database\` ON database.iddatabase = instance.database_iddatabase
            INNER JOIN databaseserver ON databaseserver.iddatabaseserver = database.databaseServer_iddatabaseServer
            WHERE iddealer IN (${safeDealerIds})
        `)) as SelectDealerDbInfoResult[];

        const rows = await Promise.all(dealerInfoResult.map(res => getReportRowForDealer(res, startDateYMD, endDateYMD)));

        // String if results are uploaded as a csv, null otherwise
        let reportURL: string | null = null;

        // If email recipients are set, create a csv, upload it to s3, and email a link to the recipients
        if (event.emailRecipients?.length) {
            // Generate the CSV string (contents of a csv file) using csv-generate's sync API. If this data set ever gets huge, we'll need to use the callback or stream API.
            const csvString = stringify(rows, { header: true });

            // Upload that bad boy to S3
            // I just appended a random string in the top level folder name for a bit more obfuscation
            const filePath = `video-report-3KCe4kZqXCkpZdp4/video-report_${todayYMD}_${v4()}.csv`;
            await s3Client.send(new PutObjectCommand({
                Bucket: reportBucket,
                Key: filePath,
                Body: csvString,
            }));
            reportURL = `https://${reportBucket}.s3.amazonaws.com/${filePath}`;

            sendgrid.setApiKey(process.env['SENDGRID_API_KEY'] + '');
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

        return {
            reportURL,
            reportData: rows,
        };
    } catch (err) {
        throw err;
    } finally {
        // Close the db connection, even if there's an error. This avoids a hanging process.
        await indexDbConn.end();
    }
};

/**
 * Concurrently executes all aggregate queries for a dealer. This function should also be called concurrently for each dealer (e.g. using Promise.all).
 */
async function getReportRowForDealer(dealerDbConnInfo: SelectDealerDbInfoResult, startDate: string, endDate: string): Promise<ReportRow> {
    const reportRow: ReportRow = {
        'Vendor Name': 'Unotifi',
        'Dealer Name': dealerDbConnInfo.dealerName || '',
        'Dealer Code': dealerDbConnInfo.internal_code
    };

    // Connection to the dealer database (aka sugarcrm database) for subsequent aggregate queries
    const dealerDbConn = await mysql.createConnection({
        host: dealerDbConnInfo.IP || '',
        user: dealerDbConnInfo.user || '',
        password: dealerDbConnInfo.password || '',
        database: dealerDbConnInfo.name || '',
        timeout: 60000,
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
            averageROOpenValue,
            avgROClosed,
            averageUpsellAmount,
            averageSmsResponseTimeInSeconds,
            averageVideoLength,
            numberOptedInROs,
            numberOptedOutROs,
            averageSMSSent,
            averagePhotoSent,
            averageVideoViews
        ] = await Promise.all([
            countROQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            countROWithVideosQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            avgLaborQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            avgPartsQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            averageROOpenValueQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            avgROClosedQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            averageUpsellAmountQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            getAverageSmsResponseTimeInSeconds(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            averageVideoLengthQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            numberOptedInROsQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            numberOptedOutROsQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            averageSMSSentQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            averagePhotoSentQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate),
            averageVideoViewsQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate)
        ])

        // Assign all the db results to the CSV row
        reportRow['Total # of Closed RO\'s (CP + WP)'] = closedROCount;
        reportRow['# of RO\'s Containing AT LEAST one Tech Video'] = totalROsWithVideoCount;
        reportRow['Average CP Labor $'] = avgLabor;
        reportRow['Average CP Parts $'] = avgParts;
        reportRow['Average RO Open Value'] = averageROOpenValue;
        reportRow['Average RO Closed Value'] = avgROClosed;
        reportRow['Average Upsell Amount'] = averageUpsellAmount;
        reportRow['Average Response Time'] = averageSmsResponseTimeInSeconds ? 
            moment.utc(averageSmsResponseTimeInSeconds * 1000).format("HH:mm:ss") :
            null;
        reportRow["Average Video Length"] = averageVideoLength ?
          moment.utc(averageVideoLength * 1000).format("HH:mm:ss") :
          null;
        reportRow['# of Opted In RO\'s'] = numberOptedInROs;
        reportRow['# of Opted Out RO\'s'] = numberOptedOutROs;
        reportRow['Average # of SMS Sent to Customer'] = averageSMSSent;
        reportRow['Average # of Photo\'s Sent to Customer'] = averagePhotoSent;
        reportRow['Average # of Email Opened/Microsite Clicked'] = averageVideoViews;

        return reportRow;
    } catch (err) {
        throw err;
    } finally {
        // Close the db connection, even if there's an error. This avoids a hanging process.
        await dealerDbConn.end();
    }
}

async function countROQuery(conn: mysql.Connection, dealerID: string, startDate: string, endDate: string) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(`
        SELECT
            count(DISTINCT auto_repair_order.id) AS total
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
            AND auto_repair_order.service_closed_date BETWEEN ? AND ?
            AND auto_dealer.integralink_code = ?
            AND labor.event_repair_labor_pay_type in ('C', 'W')
        `,
        [startDate, endDate, dealerID]
    );

    return countResult && countResult[0] ? countResult[0].total : undefined;
}

async function countROWithVideosQuery(conn: mysql.Connection, dealerID: string, startDate: string, endDate: string) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(`
        SELECT
            COUNT(1) AS total
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
        WHERE
            COALESCE(auto_repair_order.technician_id, '') != ''
            AND auto_repair_order.service_closed_date BETWEEN ? AND ?
            AND auto_dealer.integralink_code = ?
            AND auto_repair_order.has_videos = 1
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
                CAST(AVG(REPLACE(aro_sums.total_labor, ',', '')) AS DECIMAL(10,2)) AS total
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
                        AND auto_repair_order.service_closed_date BETWEEN ? AND ?
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
                CAST(AVG(REPLACE(aro_sums.total_parts, ',', '')) AS DECIMAL(10,2)) AS total
            FROM
                (SELECT 
                    SUM(labor.parts_amount) AS total_parts
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
                        AND auto_repair_order.service_closed_date BETWEEN ? AND ?
                        AND auto_dealer.integralink_code = ?
                GROUP BY auto_repair_order.name) AS aro_sums
        `,
        [startDate, endDate, dealerID]
    );

    return countResult && countResult[0] ? countResult[0].total : undefined;
}

async function averageROOpenValueQuery(
    conn: mysql.Connection,
    dealerID: string,
    startDate: string,
    endDate: string
  ) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(
      `
        SELECT 
            CAST(
                AVG(
                    IFNULL(
                        auto_repair_order.repair_order_amount_total_original, 
                        REPLACE(IFNULL(auto_repair_order.repair_order_amount_total, 0), ',', '')
                )
            ) AS DECIMAL(10,2)) as total
        FROM
            auto_dealer
                INNER JOIN
            auto_custom_auto_dealer_c ON auto_custom_auto_dealer_c.auto_custo60bd_dealer_ida = auto_dealer.id
                AND auto_custom_auto_dealer_c.deleted = 0
                INNER JOIN
            auto_customer ON auto_custom_auto_dealer_c.auto_custo0932ustomer_idb = auto_customer.id
                AND auto_customer.deleted = 0
                INNER JOIN
            auto_vehicluto_customer_c ON auto_vehicluto_customer_c.auto_vehic9275ustomer_ida = auto_customer.id
                AND auto_vehicluto_customer_c.deleted = 0
                INNER JOIN
            auto_vehicle ON auto_vehicluto_customer_c.auto_vehic831dvehicle_idb = auto_vehicle.id
                AND auto_vehicle.deleted = 0
                INNER JOIN
            auto_repairauto_vehicle_c ON auto_repairauto_vehicle_c.auto_repai4169vehicle_ida = auto_vehicle.id
                AND auto_repairauto_vehicle_c.deleted = 0
                INNER JOIN
            auto_repair_order ON auto_repairauto_vehicle_c.auto_repai527cr_order_idb = auto_repair_order.id
                AND auto_repair_order.deleted = 0
        WHERE
            COALESCE(auto_repair_order.technician_id, '') != ''
                AND auto_repair_order.service_closed_date BETWEEN ? AND ?
                AND auto_dealer.integralink_code = ?
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
                CAST(AVG(REPLACE(IFNULL(auto_repair_order.repair_order_amount_total, 0), ',', '')) AS DECIMAL(10,2)) AS total
            FROM
                auto_dealer
                    INNER JOIN
                auto_custom_auto_dealer_c ON auto_custom_auto_dealer_c.auto_custo60bd_dealer_ida = auto_dealer.id
                    AND auto_custom_auto_dealer_c.deleted = 0
                    INNER JOIN
                auto_customer ON auto_custom_auto_dealer_c.auto_custo0932ustomer_idb = auto_customer.id
                    AND auto_customer.deleted = 0
                    INNER JOIN
                auto_vehicluto_customer_c ON auto_vehicluto_customer_c.auto_vehic9275ustomer_ida = auto_customer.id
                    AND auto_vehicluto_customer_c.deleted = 0
                    INNER JOIN
                auto_vehicle ON auto_vehicluto_customer_c.auto_vehic831dvehicle_idb = auto_vehicle.id
                    AND auto_vehicle.deleted = 0
                    INNER JOIN
                auto_repairauto_vehicle_c ON auto_repairauto_vehicle_c.auto_repai4169vehicle_ida = auto_vehicle.id
                    AND auto_repairauto_vehicle_c.deleted = 0
                    INNER JOIN
                auto_repair_order ON auto_repairauto_vehicle_c.auto_repai527cr_order_idb = auto_repair_order.id
                    AND auto_repair_order.deleted = 0
            WHERE
                COALESCE(auto_repair_order.technician_id, '') != ''
                AND auto_repair_order.service_closed_date BETWEEN ? AND ?
                AND auto_dealer.integralink_code = ?
        `,
        [startDate, endDate, dealerID]
    );

    return countResult && countResult[0] ? countResult[0].total : undefined;
}


async function averageUpsellAmountQuery(
    conn: mysql.Connection,
    dealerID: string,
    startDate: string,
    endDate: string
  ) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(
      `
          SELECT 
            CAST(
                AVG(
                    REPLACE(IFNULL(auto_repair_order.repair_order_amount_total, 0), ',', '') -
                    (IFNULL(auto_repair_order.repair_order_amount_total_original, REPLACE(IFNULL(auto_repair_order.repair_order_amount_total, 0), ',', ''))
                )
            ) AS DECIMAL(10,2)) as total
          FROM
              auto_dealer
                  INNER JOIN
              auto_custom_auto_dealer_c ON auto_custom_auto_dealer_c.auto_custo60bd_dealer_ida = auto_dealer.id
                  AND auto_custom_auto_dealer_c.deleted = 0
                  INNER JOIN
              auto_customer ON auto_custom_auto_dealer_c.auto_custo0932ustomer_idb = auto_customer.id
                  AND auto_customer.deleted = 0
                  INNER JOIN
              auto_vehicluto_customer_c ON auto_vehicluto_customer_c.auto_vehic9275ustomer_ida = auto_customer.id
                  AND auto_vehicluto_customer_c.deleted = 0
                  INNER JOIN
              auto_vehicle ON auto_vehicluto_customer_c.auto_vehic831dvehicle_idb = auto_vehicle.id
                  AND auto_vehicle.deleted = 0
                  INNER JOIN
              auto_repairauto_vehicle_c ON auto_repairauto_vehicle_c.auto_repai4169vehicle_ida = auto_vehicle.id
                  AND auto_repairauto_vehicle_c.deleted = 0
                  INNER JOIN
              auto_repair_order ON auto_repairauto_vehicle_c.auto_repai527cr_order_idb = auto_repair_order.id
                  AND auto_repair_order.deleted = 0
          WHERE
              COALESCE(auto_repair_order.technician_id, '') != ''
                  AND auto_repair_order.service_closed_date BETWEEN ? AND ?
                  AND auto_dealer.integralink_code = ?
        `,
      [startDate, endDate, dealerID]
    );
    return countResult && countResult[0] ? countResult[0].total : undefined;
  }

async function numberOptedInROsQuery(conn: mysql.Connection, dealerID: string, startDate: string, endDate: string) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(
        `
        SELECT 
            COUNT(DISTINCT auto_repair_order.id) as total
        FROM
            auto_dealer
                INNER JOIN
            auto_custom_auto_dealer_c ON auto_custom_auto_dealer_c.auto_custo60bd_dealer_ida = auto_dealer.id
                AND auto_custom_auto_dealer_c.deleted = 0
                INNER JOIN
            auto_customer ON auto_custom_auto_dealer_c.auto_custo0932ustomer_idb = auto_customer.id
                AND auto_customer.deleted = 0
                INNER JOIN
            auto_vehicluto_customer_c ON auto_vehicluto_customer_c.auto_vehic9275ustomer_ida = auto_customer.id
                AND auto_vehicluto_customer_c.deleted = 0
                INNER JOIN
            auto_vehicle ON auto_vehicluto_customer_c.auto_vehic831dvehicle_idb = auto_vehicle.id
                AND auto_vehicle.deleted = 0
                INNER JOIN
            auto_repairauto_vehicle_c ON auto_repairauto_vehicle_c.auto_repai4169vehicle_ida = auto_vehicle.id
                AND auto_repairauto_vehicle_c.deleted = 0
                INNER JOIN
            auto_repair_order ON auto_repairauto_vehicle_c.auto_repai527cr_order_idb = auto_repair_order.id
                AND auto_repair_order.deleted = 0
        WHERE
            auto_repair_order.service_closed_date BETWEEN ? AND ?
                AND auto_dealer.integralink_code = ?
                AND COALESCE(auto_repair_order.technician_id, '') != ''
                AND auto_customer.do_not_text_flag = 0
        `,
        [startDate, endDate, dealerID]
    );
    return countResult && countResult[0] ? countResult[0].total : undefined;
}

async function numberOptedOutROsQuery(conn: mysql.Connection, dealerID: string, startDate: string, endDate: string) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(
        `
        SELECT
            count(DISTINCT auto_repair_order.id) as total
        FROM auto_repair_order
            INNER JOIN auto_repairauto_vehicle_c ON auto_repairauto_vehicle_c.auto_repai527cr_order_idb = auto_repair_order.id
                AND auto_repair_order.deleted = 0
            INNER JOIN auto_vehicle ON auto_vehicle.id = auto_repairauto_vehicle_c.auto_repai4169vehicle_ida
                AND auto_repairauto_vehicle_c.deleted = 0
            INNER JOIN auto_vehicluto_customer_c ON auto_vehicluto_customer_c.auto_vehic831dvehicle_idb = auto_vehicle.id
                AND auto_vehicle.deleted = 0
            INNER JOIN auto_customer ON auto_customer.id = auto_vehicluto_customer_c.auto_vehic9275ustomer_ida
                AND auto_vehicluto_customer_c.deleted = 0		
            INNER JOIN auto_custom_auto_dealer_c ON auto_custom_auto_dealer_c.auto_custo0932ustomer_idb = auto_customer.id
                AND auto_customer.deleted = 0
            INNER JOIN auto_dealer ON auto_dealer.id = auto_custom_auto_dealer_c.auto_custo60bd_dealer_ida
                AND auto_custom_auto_dealer_c.deleted = 0
        WHERE
            auto_repair_order.service_closed_date BETWEEN ? AND ?
            AND auto_dealer.integralink_code = ?
            AND auto_customer.do_not_text_flag = 1
        `,
        [startDate, endDate, dealerID]
    );
    return countResult && countResult[0] ? countResult[0].total : undefined;
}

async function averageSMSSentQuery(conn: mysql.Connection, dealerID: string, startDate: string, endDate: string) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(
        `
            SELECT
                CAST(COUNT(DISTINCT auto_event.id) / COUNT(DISTINCT repairOrders.roId) AS DECIMAL (10 , 2 )) AS total
            FROM
                (SELECT DISTINCT
                     auto_vehicle.id AS vehicleId,
                     auto_repair_order.id AS roId,
                     auto_repair_order.service_open_date AS roOpenDate,
                     ADDTIME(auto_repair_order.service_closed_date, '23:59:59') AS roClosedDate
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
                 WHERE
                     COALESCE(auto_repair_order.technician_id, '') != ''
            AND auto_repair_order.service_closed_date BETWEEN ? AND ?
            AND auto_repair_order.deleted = 0
            AND auto_dealer.integralink_code = ?) AS repairOrders
                    INNER JOIN
                auto_recipient ON auto_recipient.auto_vehicle_id_c = repairOrders.vehicleId
                    AND auto_recipient.deleted = 0
                    INNER JOIN
                auto_event_to_recipient_c ON auto_recipient.id = auto_event_to_recipient_c.auto_eventa735cipient_ida
                    AND auto_event_to_recipient_c.deleted = 0
                    INNER JOIN
                auto_event ON auto_event.id = auto_event_to_recipient_c.auto_eventfa83o_event_idb
                    AND auto_event.deleted = 0
                    INNER JOIN
                users ON auto_event.modified_user_id = users.id
                    INNER JOIN
                auto_contact_person ON auto_contact_person.user_id_c = users.id
                    AND auto_contact_person.deleted = 0
            WHERE
                auto_event.body_type = 'Text'
              AND auto_event.generated_from = 'Comunicator'
              AND auto_event.type = 'Not-Pending'
              AND auto_event.date_entered BETWEEN repairOrders.roOpenDate AND ADDTIME(repairOrders.roClosedDate, '23:59:59')
        `,
        [startDate, endDate, dealerID]
    );

    return countResult && countResult[0] ? countResult[0].total : undefined;
}

async function averagePhotoSentQuery(conn: mysql.Connection, dealerID: string, startDate: string, endDate: string) {
  // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
  const countResult: AggregateQueryResult = await conn.query(
    `
        SELECT
            CAST(COUNT(DISTINCT auto_media_file.id) / COUNT(DISTINCT repairOrders.roId) AS DECIMAL (10 , 2 )) AS total
        FROM
            (SELECT DISTINCT
                 auto_vehicle.id AS vehicleId,
                 auto_repair_order.id AS roId,
                 auto_repair_order.service_open_date AS roOpenDate,
                 ADDTIME(auto_repair_order.service_closed_date, '23:59:59') AS roClosedDate
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
             WHERE
                 COALESCE(auto_repair_order.technician_id, '') != ''
            AND auto_repair_order.service_closed_date BETWEEN ? AND ?
            AND auto_repair_order.deleted = 0
            AND auto_dealer.integralink_code = ?) AS repairOrders
                INNER JOIN
            auto_recipient ON auto_recipient.auto_vehicle_id_c = repairOrders.vehicleId
                AND auto_recipient.deleted = 0
                INNER JOIN
            auto_event_to_recipient_c ON auto_recipient.id = auto_event_to_recipient_c.auto_eventa735cipient_ida
                AND auto_event_to_recipient_c.deleted = 0
                INNER JOIN
            auto_event ON auto_event.id = auto_event_to_recipient_c.auto_eventfa83o_event_idb
                AND auto_event.deleted = 0
                INNER JOIN
            users ON auto_event.modified_user_id = users.id
                INNER JOIN
            auto_contact_person ON auto_contact_person.user_id_c = users.id
                AND auto_contact_person.deleted = 0
                INNER JOIN
            auto_media_file ON auto_event.id = auto_media_file.event_id
        WHERE
            auto_media_file.date_entered BETWEEN repairOrders.roOpenDate AND repairOrders.roClosedDate
          AND auto_media_file.file_guid IS NULL
    `,
    [startDate, endDate, dealerID]
  );
  return countResult && countResult[0] ? countResult[0].total : undefined;
}

async function getAverageSmsResponseTimeInSeconds(conn: mysql.Connection, dealerID: string, startDate: string, endDate: string) {

    const textEvents: TextEvents = await conn.query(
        `
            SELECT DISTINCT
                auto_recipient.id AS recipientId,
                auto_event.id AS eventId,
                auto_event.sent_date AS eventSentDate,
                auto_event.type AS eventType,
                auto_event.generated_from AS eventGeneratedFrom,
                auto_media_file.id AS mediaFileId
            FROM
                (SELECT DISTINCT
                     auto_vehicle.id AS vehicleId,
                     auto_repair_order.service_open_date AS roOpenDate,
                     ADDTIME(auto_repair_order.service_closed_date, '23:59:59') AS roClosedDate
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
                 WHERE
                     COALESCE(auto_repair_order.technician_id, '') != ''
            AND auto_repair_order.service_closed_date BETWEEN ? AND ?
            AND auto_repair_order.deleted = 0
            AND auto_dealer.integralink_code = ?) AS repairOrders
                    INNER JOIN
                auto_recipient ON auto_recipient.auto_vehicle_id_c = repairOrders.vehicleId
                    AND auto_recipient.deleted = 0
                    INNER JOIN
                auto_event_to_recipient_c ON auto_recipient.id = auto_event_to_recipient_c.auto_eventa735cipient_ida
                    AND auto_event_to_recipient_c.deleted = 0
                    INNER JOIN
                auto_event ON auto_event.id = auto_event_to_recipient_c.auto_eventfa83o_event_idb
                    AND auto_event.deleted = 0
                    LEFT JOIN
                auto_media_file ON auto_event.id = auto_media_file.event_id
            WHERE
                auto_event.sent_date BETWEEN repairOrders.roOpenDate AND repairOrders.roClosedDate
              AND auto_event.body_type = 'Text'
              AND ((auto_event.generated_from = 'Comunicator'
                AND auto_event.type = 'Not-Pending')
                OR (auto_event.generated_from = 'Reply'
                    AND auto_event.type = 'Reply'))
            ORDER BY auto_recipient.id , auto_event.sent_date
        `,
        [startDate, endDate, dealerID]
    );

    const outboundTextEvents: { [key: string]: string } = {};
    const inboundTextEvents: { [key: string]: string } = {};
    const responseTimesInSeconds: number[] = [];

    // Process the text events in order to find the average response time
    textEvents.forEach((textEvent, index) => {
        const recipientId = textEvent.recipientId ?? '';

        // Find outbound text events
        if (textEvent.eventGeneratedFrom == 'Comunicator' && textEvent.eventType == 'Not-Pending') {
            outboundTextEvents[recipientId] = textEvent.eventSentDate;
        }

        // Find inbound text events
        if (textEvent.eventGeneratedFrom == 'Reply' && textEvent.eventType == 'Reply') {
            inboundTextEvents[recipientId] = textEvent.eventSentDate;
        }

        // Calculate response times
        if (outboundTextEvents[recipientId] && inboundTextEvents[recipientId]) {
            const outboundTextEventDate = new Date(outboundTextEvents[recipientId]!);
            const inboundTextEventDate = new Date(inboundTextEvents[recipientId]!);

            // Check diff only for consecutive outbound/inbound pair
            if (
                index > 0 &&
                textEvents[index - 1]!.eventGeneratedFrom == 'Comunicator' &&
                textEvents[index - 1]!.mediaFileId &&
                textEvent.eventGeneratedFrom == 'Reply'
            ) {
                responseTimesInSeconds.push(getDateDifferenceInSeconds(outboundTextEventDate, inboundTextEventDate));
            }
        }
    });

    return responseTimesInSeconds.length ? average(responseTimesInSeconds) : null;
}

async function averageVideoLengthQuery(
  conn: mysql.Connection,
  dealerID: string,
  startDate: string,
  endDate: string
) {
  // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
  const countResult: AggregateQueryResult = await conn.query(
    `
        SELECT 
            AVG(IF(auto_media_file.file_length IS NOT NULL, auto_media_file.file_length, 0)) as total
        FROM
            auto_media_file
                INNER JOIN
            auto_event ON auto_event.id = auto_media_file.event_id
                INNER JOIN
            users ON auto_event.modified_user_id = users.id
                INNER JOIN
            auto_contact_person ON auto_contact_person.user_id_c = users.id
                AND auto_contact_person.deleted = 0
                LEFT JOIN
            auto_event_to_recipient_c ON auto_event_to_recipient_c.auto_eventfa83o_event_idb = auto_event.id
                AND auto_event_to_recipient_c.deleted = 0
                INNER JOIN
            auto_contac_auto_dealer_c ON auto_contac_auto_dealer_c.auto_contaff8f_person_idb = auto_contact_person.id
                AND auto_contac_auto_dealer_c.deleted = 0
                INNER JOIN
            auto_dealer ON auto_contac_auto_dealer_c.auto_contafb84_dealer_ida = auto_dealer.id
                AND auto_dealer.deleted = 0
        WHERE
            auto_event.type = 'Not-Pending'
                AND auto_media_file.date_entered BETWEEN ? AND ?
                AND auto_media_file.file_guid IS NOT NULL
                AND auto_media_file.file_length IS NOT NULL
                AND integralink_code = ?
       `,
    [startDate, endDate, dealerID]
  );
  return countResult && countResult[0] ? countResult[0].total : undefined;
}

async function averageVideoViewsQuery(
  conn: mysql.Connection,
  dealerID: string,
  startDate: string,
  endDate: string
) {
  // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
  const countResult: AggregateQueryResult = await conn.query(
    `
        SELECT 
            CAST(COUNT(DISTINCT auto_media_file_view.id) / COUNT(DISTINCT auto_media_file.id) AS DECIMAL(10, 2)) AS total
        FROM
            auto_media_file
                INNER JOIN
            auto_event ON auto_event.id = auto_media_file.event_id
                INNER JOIN
            users ON auto_event.modified_user_id = users.id
                INNER JOIN
            auto_contact_person ON auto_contact_person.user_id_c = users.id
                AND auto_contact_person.deleted = 0
                LEFT JOIN
            auto_event_to_recipient_c ON auto_event_to_recipient_c.auto_eventfa83o_event_idb = auto_event.id
                AND auto_event_to_recipient_c.deleted = 0
                INNER JOIN
            auto_contac_auto_dealer_c ON auto_contac_auto_dealer_c.auto_contaff8f_person_idb = auto_contact_person.id
                AND auto_contac_auto_dealer_c.deleted = 0
                INNER JOIN
            auto_dealer ON auto_contac_auto_dealer_c.auto_contafb84_dealer_ida = auto_dealer.id
                AND auto_dealer.deleted = 0
                LEFT JOIN
            auto_media_file_view ON auto_media_file.id = auto_media_file_view.auto_media_file_id_c
        WHERE
            auto_event.type = 'Not-Pending'
                AND auto_media_file.date_entered BETWEEN ? AND ?
                AND auto_media_file.file_guid IS NOT NULL
                AND integralink_code = ?
      `,
    [startDate, endDate, dealerID]
  );
  return countResult && countResult[0] ? countResult[0].total : undefined;
}

/////////////////////////////////////////////////
// Types
/////////////////////////////////////////////////

// Aggregate Query Result
type AggregateQueryResult = { total: number }[];

// Average Response Time Result
type TextEvents = {
    eventId: string
    eventSentDate: string
    eventType: string
    eventGeneratedFrom: string
    recipientId: string
    mediaFileId: string
}[];


/////////////////////////////////////////////////
// Interfaces
/////////////////////////////////////////////////

/**
 * Represents a row of the output CSV file
 */
interface ReportRow {
    'Vendor Name'?: string;
    'Dealer Name'?: string;
    'Dealer Code'?: string;
    'Total # of Closed RO\'s (CP + WP)'?: number | null;
    '# of RO\'s Containing AT LEAST one Tech Video'?: number | null;
    'Average CP Labor $'?: number | null;
    'Average CP Parts $'?: number | null;
    'Average RO Open Value'?: number | null;
    'Average RO Closed Value'?: number | null;
    'Average Upsell Amount'?: number | null;
    'Average Response Time'?: string | null;
    'Average Video Length'?: string | null;
    '# of Opted In RO\'s'?: number | null;
    '# of Opted Out RO\'s'?: number | null;
    'Average # of SMS Sent to Customer'?: number | null;
    'Average # of Photo\'s Sent to Customer'?: number | null;
    'Average # of Email Opened/Microsite Clicked'?: number | null;
}

/**
 * JSON input of the lambda function
 */

interface VideoReportEvent {
    dealerIDs?: string[];
    emailRecipients?: string[];
    startDate?: string;
    endDate?: string;
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


/////////////////////////////////////////////////
// Helper Functions
/////////////////////////////////////////////////

/**
 * Average
 * @param array An array of numbers to calculate the average
 * @returns The average value of the numbers provided
 */
const average = (array: number[]): number => array.reduce((a, b) => a + b) / array.length;

/**
 * Get Date Difference in Seconds
 * @param startDate 
 * @param endDate 
 * @returns Returns difference between two dates in seconds rounded to the nearest whole number
 */
const getDateDifferenceInSeconds = (startDate: any, endDate: any): number => {
    const diffInMs = Math.round(endDate - startDate);
    return diffInMs / 1000;
}
