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

    // Set input defaults and sanitize date overrides
    // if start date is not provided set start date to a month ago
    let startDate = new Date();
    startDate = event?.startDate ? new Date(event.startDate) : new Date(startDate.setMonth(startDate.getMonth() - 1));

    // if end date is not provided set end date to a day ago
    let endDate = new Date();
    endDate = event?.endDate ? new Date(event.endDate) : new Date(endDate.setDate(endDate.getDate() - 1));

    // convert dates to string
    const startDateYMD = startDate.toISOString().split('T')[0]!;
    const endDateYMD = endDate.toISOString().split('T')[0]!;
    const endDateYMDHMS = new Date(endDate.setHours(23, 59, 59, 999)).toISOString()!;

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

        const rows = await Promise.all(
            dealerInfoResult.map((res) =>
                getReportRowForDealer(
                    res,
                    startDateYMD,
                    endDateYMDHMS
                )
            )
        );

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
        [ReportColumns.VENDOR_NAME]: 'Unotifi',
        [ReportColumns.DEALER_NAME]: dealerDbConnInfo.dealerName || '',
        [ReportColumns.DEALER_CODE]: dealerDbConnInfo.internal_code
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
        // get all distinct ROs before running other queries
        const rows = await closedROQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate);

        // return if no repair order found
        if (!rows.length) {
            reportRow[ReportColumns.CLOSED_ROS] = 0;
            reportRow[ReportColumns.ROS_CONTAINING_VIDEO] = 0;
            reportRow[ReportColumns.AVERAGE_CP_LABOR] = 0;
            reportRow[ReportColumns.AVERAGE_CP_PARTS] = 0;
            reportRow[ReportColumns.AVERAGE_RO_OPEN_VALUE] = 0;
            reportRow[ReportColumns.AVERAGE_RO_CLOSED_VALUE] = 0;
            reportRow[ReportColumns.AVERAGE_UPSELL_AMOUNT] = 0;
            reportRow[ReportColumns.AVERAGE_RESPONSE_TIME] = 0;
            reportRow[ReportColumns.AVERAGE_VIDEO_LENGTH] = 0;
            reportRow[ReportColumns.OPTED_IN_ROS] = 0;
            reportRow[ReportColumns.OPTED_OUT_ROS] = 0;
            reportRow[ReportColumns.AVERAGE_SMS_SENT_TO_CUSTOMER] = 0;
            reportRow[ReportColumns.AVERAGE_PHOTOS_SENT_TO_CUSTOMER] = 0;
            reportRow[ReportColumns.AVERAGE_EMAIL_OPENED] = 0;

            return reportRow;
        }

        // extract the repair order Ids
        const roIds = rows!.map(a => a.id);

        // join these ids to a string to be used in the "IN" mysql clause
        const strROIds  = "('" + roIds.join("', '") + "')";

        // Run each aggregate column query concurrently to save time.
        // Technically, mysql queries still run serially for a single connection, but it should at least put the burden of handling that
        // on a db server instead of this Node.js app.
        const [
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
            countROWithVideosQuery(dealerDbConn, strROIds),
            avgLaborQuery(dealerDbConn, strROIds),
            avgPartsQuery(dealerDbConn, strROIds),
            averageROOpenValueQuery(dealerDbConn, strROIds),
            avgROClosedQuery(dealerDbConn, strROIds),
            averageUpsellAmountQuery(dealerDbConn, strROIds),
            getAverageSmsResponseTimeInSeconds(dealerDbConn, strROIds),
            averageVideoLengthQuery(dealerDbConn, strROIds),
            numberOptedInROsQuery(dealerDbConn, strROIds),
            numberOptedOutROsQuery(dealerDbConn, strROIds),
            averageSMSSentQuery(dealerDbConn, strROIds),
            averagePhotoSentQuery(dealerDbConn, strROIds),
            averageVideoViewsQuery(dealerDbConn, strROIds)
        ])

        // Assign all the db results to the CSV row
        reportRow[ReportColumns.CLOSED_ROS] = roIds.length;
        reportRow[ReportColumns.ROS_CONTAINING_VIDEO] = totalROsWithVideoCount;
        reportRow[ReportColumns.AVERAGE_CP_LABOR] = avgLabor;
        reportRow[ReportColumns.AVERAGE_CP_PARTS] = avgParts;
        reportRow[ReportColumns.AVERAGE_RO_OPEN_VALUE] = averageROOpenValue;
        reportRow[ReportColumns.AVERAGE_RO_CLOSED_VALUE] = avgROClosed;
        reportRow[ReportColumns.AVERAGE_UPSELL_AMOUNT] = averageUpsellAmount;
        reportRow[ReportColumns.AVERAGE_RESPONSE_TIME] = averageSmsResponseTimeInSeconds ?
            moment.utc(averageSmsResponseTimeInSeconds * 1000).format("HH:mm:ss") :
            0;
        reportRow[ReportColumns.AVERAGE_VIDEO_LENGTH] = averageVideoLength ?
            moment.utc(averageVideoLength * 1000).format("HH:mm:ss") :
            0;
        reportRow[ReportColumns.OPTED_IN_ROS] = numberOptedInROs;
        reportRow[ReportColumns.OPTED_OUT_ROS] = numberOptedOutROs;
        reportRow[ReportColumns.AVERAGE_SMS_SENT_TO_CUSTOMER] = averageSMSSent;
        reportRow[ReportColumns.AVERAGE_PHOTOS_SENT_TO_CUSTOMER] = averagePhotoSent;
        reportRow[ReportColumns.AVERAGE_EMAIL_OPENED] = averageVideoViews;

        return reportRow;
    } catch (err) {
        throw err;
    } finally {
        // Close the db connection, even if there's an error. This avoids a hanging process.
        await dealerDbConn.end();
    }
}

async function closedROQuery(conn: mysql.Connection, dealerID: string, startDate: string, endDate: string) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const result: QueryResult = await conn.query(
        `
            SELECT
                DISTINCT auto_repair_order.id
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
                    AND is_customer_warranty_pay_type = 1
        `,
        [startDate, endDate, dealerID]
    );

    return result ? result : [];
}

async function countROWithVideosQuery(
    conn: mysql.Connection,
    roIds: string
) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(
        `
            SELECT
                COUNT(DISTINCT auto_repair_order.id) AS total
            FROM
                auto_repair_order
            WHERE
                auto_repair_order.has_videos = 1 
                AND auto_repair_order.id IN ` + roIds
    );

    return countResult && countResult[0] && countResult[0].total ? countResult[0].total : 0;
}

async function avgLaborQuery(
    conn: mysql.Connection,
    roIds: string
) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    // because the apostrophe parameter is escaped in a query parameter, put directly the value in the query
    const countResult: AggregateQueryResult = await conn.query(
        `
            SELECT
                CAST(SUM(REPLACE(auto_ro_labor.labor_amount, ',', '')) / COUNT(DISTINCT auto_repair_order.id) AS DECIMAL (10 , 2 )) AS total
            FROM
                auto_repair_order
                    INNER JOIN
                auto_ro_labrepair_order_c ON auto_ro_labrepair_order_c.auto_ro_laada9r_order_ida = auto_repair_order.id
                    AND auto_ro_labrepair_order_c.deleted = 0
                    INNER JOIN
                auto_ro_labor ON auto_ro_labrepair_order_c.auto_ro_la1301o_labor_idb = auto_ro_labor.id
            WHERE auto_repair_order.id IN ` + roIds + `
                AND auto_ro_labor.event_repair_labor_pay_type in ('C', 'W')
        `
    );

    return countResult && countResult[0] && countResult[0].total ? countResult[0].total : 0;
}

async function avgPartsQuery(
    conn: mysql.Connection,
    roIds: string
) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(
        `
            SELECT 
                CAST(SUM(REPLACE(auto_ro_labor.parts_amount, ',', '')) / COUNT(DISTINCT auto_repair_order.id) AS DECIMAL (10 , 2 )) AS total
            FROM 
                 auto_repair_order
                     INNER JOIN
                 auto_ro_labrepair_order_c ON auto_ro_labrepair_order_c.auto_ro_laada9r_order_ida = auto_repair_order.id
                     AND auto_ro_labrepair_order_c.deleted = 0
                     INNER JOIN
                 auto_ro_labor ON auto_ro_labrepair_order_c.auto_ro_la1301o_labor_idb = auto_ro_labor.id
            WHERE auto_repair_order.id IN ` + roIds + `
                AND auto_ro_labor.event_repair_labor_pay_type in ('C', 'W')
        `
    );

    return countResult && countResult[0] && countResult[0].total ? countResult[0].total : 0;
}

async function averageROOpenValueQuery(
    conn: mysql.Connection,
    roIds: string
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
            auto_repair_order
        WHERE 
            auto_repair_order.id IN ` + roIds
    );

    return countResult && countResult[0] && countResult[0].total ? countResult[0].total : 0;
  }

async function avgROClosedQuery(
    conn: mysql.Connection,
        roIds: string
) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(
        `
            SELECT
                CAST(AVG(REPLACE(IFNULL(auto_repair_order.repair_order_amount_total, 0), ',', '')) AS DECIMAL(10,2)) AS total
            FROM 
                 auto_repair_order
            WHERE 
                  auto_repair_order.id IN ` + roIds
    );

    return countResult && countResult[0] && countResult[0].total ? countResult[0].total : 0;
}


async function averageUpsellAmountQuery(
    conn: mysql.Connection,
    roIds: string
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
               auto_repair_order
          WHERE 
                auto_repair_order.id IN ` + roIds
    );
    return countResult && countResult[0] && countResult[0].total ? countResult[0].total : 0;
  }

async function numberOptedInROsQuery(
    conn: mysql.Connection,
    roIds: string
) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(
        `
            SELECT
                COUNT(DISTINCT auto_customer.id) as total
            FROM
                auto_customer
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
                auto_customer.do_not_text_flag = 0
              AND auto_repair_order.id IN ` + roIds
    );

    return countResult && countResult[0] && countResult[0].total ? countResult[0].total : 0;
}

async function numberOptedOutROsQuery(
    conn: mysql.Connection,
    roIds: string
) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(
        `
            SELECT 
                   COUNT(DISTINCT auto_customer.id) as total
            FROM 
                auto_customer
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
                auto_customer.do_not_text_flag = 1
                AND auto_repair_order.id IN ` + roIds
    );

    return countResult && countResult[0] && countResult[0].total ? countResult[0].total : 0;
}

async function averageSMSSentQuery(
    conn: mysql.Connection,
    roIds: string
) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const countResult: AggregateQueryResult = await conn.query(
        `
            SELECT
                CAST(COUNT(DISTINCT auto_event.id) / COUNT(DISTINCT auto_repair_order.id) AS DECIMAL (10 , 2 )) AS total
            FROM
                auto_vehicle
                    INNER JOIN
                auto_repairauto_vehicle_c ON auto_repairauto_vehicle_c.auto_repai4169vehicle_ida = auto_vehicle.id
                    AND auto_repairauto_vehicle_c.deleted = 0
                    INNER JOIN
                auto_repair_order ON auto_repairauto_vehicle_c.auto_repai527cr_order_idb = auto_repair_order.id
                    INNER JOIN
                auto_recipient ON auto_recipient.auto_vehicle_id_c = auto_vehicle.id
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
                  auto_repair_order.id IN ` + roIds + `
                  AND auto_event.body_type = 'Text'
                  AND auto_event.generated_from = 'Comunicator'
                  AND auto_event.type = 'Not-Pending'
                  AND auto_event.date_entered BETWEEN auto_repair_order.service_open_date AND ADDTIME(auto_repair_order.service_closed_date, '23:59:59')
        `
    );

    return countResult && countResult[0] && countResult[0].total ? countResult[0].total : 0;
}

async function averagePhotoSentQuery(
    conn: mysql.Connection,
    roIds: string
) {
  // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
  const countResult: AggregateQueryResult = await conn.query(
    `
        SELECT
            CAST(COUNT(DISTINCT auto_media_file.id) / COUNT(DISTINCT auto_repair_order.id) AS DECIMAL (10 , 2 )) AS total
        FROM
            auto_vehicle
                INNER JOIN
            auto_repairauto_vehicle_c ON auto_repairauto_vehicle_c.auto_repai4169vehicle_ida = auto_vehicle.id
                AND auto_repairauto_vehicle_c.deleted = 0
                INNER JOIN
            auto_repair_order ON auto_repairauto_vehicle_c.auto_repai527cr_order_idb = auto_repair_order.id
                INNER JOIN
            auto_recipient ON auto_recipient.auto_vehicle_id_c = auto_vehicle.id
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
            auto_repair_order.id IN ` + roIds + `
            AND auto_media_file.date_entered BETWEEN auto_repair_order.service_open_date AND ADDTIME(auto_repair_order.service_open_date, '23:59:59')
            AND auto_media_file.file_guid IS NULL
    `
  );
  return countResult && countResult[0] && countResult[0].total ? countResult[0].total : 0;
}

async function getAverageSmsResponseTimeInSeconds(
    conn: mysql.Connection,
        roIds: string
) {
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
                auto_vehicle
                    INNER JOIN
                auto_repairauto_vehicle_c ON auto_repairauto_vehicle_c.auto_repai4169vehicle_ida = auto_vehicle.id
                    AND auto_repairauto_vehicle_c.deleted = 0
                    INNER JOIN
                auto_repair_order ON auto_repairauto_vehicle_c.auto_repai527cr_order_idb = auto_repair_order.id
                    INNER JOIN
                auto_recipient ON auto_recipient.auto_vehicle_id_c = auto_vehicle.id
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
                    auto_repair_order.id IN ` + roIds + `
              AND auto_event.sent_date BETWEEN auto_repair_order.service_open_date AND ADDTIME(auto_repair_order.service_open_date, '23:59:59')
              AND auto_event.body_type = 'Text'
              AND ((auto_event.generated_from = 'Comunicator'
                AND auto_event.type = 'Not-Pending'
                AND auto_media_file.file_guid IS NOT NULL)
                OR (auto_event.generated_from = 'Reply'
                    AND auto_event.type = 'Reply'))
            ORDER BY auto_recipient.id, auto_event.sent_date
        `
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
                // ignore the responses greater than one day
                const diff = getDateDifferenceInSeconds(outboundTextEventDate, inboundTextEventDate);
                if (diff < 86400) {
                    responseTimesInSeconds.push(diff);
                }
            }
        }
    });

    return responseTimesInSeconds.length ? average(responseTimesInSeconds) : 0;
}

async function averageVideoLengthQuery(
    conn: mysql.Connection,
    roIds: string
) {
  // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
  const countResult: AggregateQueryResult = await conn.query(
    `
        SELECT 
            AVG(IF(auto_media_file.file_length IS NOT NULL, auto_media_file.file_length, 0)) as total
        FROM
            auto_vehicle
                INNER JOIN
            auto_repairauto_vehicle_c ON auto_repairauto_vehicle_c.auto_repai4169vehicle_ida = auto_vehicle.id
                AND auto_repairauto_vehicle_c.deleted = 0
                INNER JOIN
            auto_repair_order ON auto_repairauto_vehicle_c.auto_repai527cr_order_idb = auto_repair_order.id
                INNER JOIN
            auto_recipient ON auto_recipient.auto_vehicle_id_c = auto_vehicle.id
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
            auto_repair_order.id IN ` + roIds + `
            AND auto_event.body_type = 'Text'
            AND auto_event.generated_from = 'Comunicator'
            AND auto_event.type = 'Not-Pending'
            AND auto_media_file.date_entered BETWEEN auto_repair_order.service_open_date AND ADDTIME(auto_repair_order.service_open_date, '23:59:59')  
            AND auto_media_file.file_guid IS NOT NULL
            AND auto_media_file.file_length IS NOT NULL
       `
  );
  return countResult && countResult[0] && countResult[0].total ? countResult[0].total : 0;
}

async function averageVideoViewsQuery(
    conn: mysql.Connection,
    roIds: string
) {
  // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
  const countResult: AggregateQueryResult = await conn.query(
    `
        SELECT 
            CAST(COUNT(DISTINCT auto_media_file_view.id) / COUNT(DISTINCT auto_media_file.id) AS DECIMAL(10, 2)) AS total
        FROM
            auto_vehicle
                INNER JOIN
            auto_repairauto_vehicle_c ON auto_repairauto_vehicle_c.auto_repai4169vehicle_ida = auto_vehicle.id
                AND auto_repairauto_vehicle_c.deleted = 0
                INNER JOIN
            auto_repair_order ON auto_repairauto_vehicle_c.auto_repai527cr_order_idb = auto_repair_order.id
                INNER JOIN
            auto_recipient ON auto_recipient.auto_vehicle_id_c = auto_vehicle.id
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
                LEFT JOIN
            auto_media_file_view ON auto_media_file.id = auto_media_file_view.auto_media_file_id_c
        WHERE 
            auto_repair_order.id IN ` + roIds + `
            AND auto_event.body_type = 'Text'
            AND auto_event.generated_from = 'Comunicator'
            AND auto_event.type = 'Not-Pending'  
            AND auto_media_file.date_entered BETWEEN auto_repair_order.service_open_date AND ADDTIME(auto_repair_order.service_open_date, '23:59:59')
            AND auto_media_file.file_guid IS NOT NULL
      `
  );
  return countResult && countResult[0] && countResult[0].total ? countResult[0].total : 0;
}

/////////////////////////////////////////////////
// Constants
/////////////////////////////////////////////////

/**
 * Represents the column names of the output CSV file
 */

enum ReportColumns {
    VENDOR_NAME = 'Vendor Name',
    DEALER_NAME = 'Dealer Name',
    DEALER_CODE = 'Dealer Code',
    CLOSED_ROS = 'Total # of Closed RO\'s (CP + WP)',
    ROS_CONTAINING_VIDEO = '# of RO\'s Containing AT LEAST one Tech Video',
    AVERAGE_CP_LABOR = 'Average CP Labor $',
    AVERAGE_CP_PARTS = 'Average CP Parts $',
    AVERAGE_RO_OPEN_VALUE = 'Average RO Open Value',
    AVERAGE_RO_CLOSED_VALUE = 'Average RO Closed Value',
    AVERAGE_UPSELL_AMOUNT = 'Average Upsell Amount',
    AVERAGE_RESPONSE_TIME = 'Average Response Time',
    AVERAGE_VIDEO_LENGTH = 'Average Video Length',
    OPTED_IN_ROS = '# of Opted In RO\'s',
    OPTED_OUT_ROS = '# of Opted Out RO\'s',
    AVERAGE_SMS_SENT_TO_CUSTOMER = 'Average # of SMS Sent to Customer',
    AVERAGE_PHOTOS_SENT_TO_CUSTOMER = 'Average # of Photo\'s Sent to Customer',
    AVERAGE_EMAIL_OPENED = 'Average # of Email Opened/Microsite Clicked'
}

/////////////////////////////////////////////////
// Types
/////////////////////////////////////////////////

// Query Result
type QueryResult = { id: string }[];

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
    [ReportColumns.VENDOR_NAME]?: string;
    [ReportColumns.DEALER_NAME]?: string;
    [ReportColumns.DEALER_CODE]?: string;
    [ReportColumns.CLOSED_ROS]?: number;
    [ReportColumns.ROS_CONTAINING_VIDEO]?: number;
    [ReportColumns.AVERAGE_CP_LABOR]?: number;
    [ReportColumns.AVERAGE_CP_PARTS]?: number;
    [ReportColumns.AVERAGE_RO_OPEN_VALUE]?: number;
    [ReportColumns.AVERAGE_RO_CLOSED_VALUE]?: number;
    [ReportColumns.AVERAGE_UPSELL_AMOUNT]?: number;
    [ReportColumns.AVERAGE_RESPONSE_TIME]?: string | number;
    [ReportColumns.AVERAGE_VIDEO_LENGTH]?: string | number;
    [ReportColumns.OPTED_IN_ROS]?: number;
    [ReportColumns.OPTED_OUT_ROS]?: number;
    [ReportColumns.AVERAGE_SMS_SENT_TO_CUSTOMER]?: number;
    [ReportColumns.AVERAGE_PHOTOS_SENT_TO_CUSTOMER]?: number;
    [ReportColumns.AVERAGE_EMAIL_OPENED]?: number;
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
