import * as mysql from 'promise-mysql';
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import sendgrid from '@sendgrid/mail';
import { v4 } from 'uuid';
import moment from 'moment';
const ExcelJS = require('exceljs');

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

    // get the dealer group name (used in the email body)
    const dealerGroupName = event.dealerGroupName ?? '';

    try {
        const dealerInfoResult = (await indexDbConn.query(`
            SELECT dealer.iddealer, dealer.internal_code, dealer.name as dealerName, database.name, database.user, database.password, databaseserver.IP FROM dealer 
            INNER JOIN instance ON instance.idinstance = dealer.instance_idinstance
            INNER JOIN \`database\` ON database.iddatabase = instance.database_iddatabase
            INNER JOIN databaseserver ON databaseserver.iddatabaseserver = database.databaseServer_iddatabaseServer
            WHERE iddealer IN (${safeDealerIds})
        `)) as SelectDealerDbInfoResult[];

        // get the RO Level report data
        const resultRoLevel = await Promise.all(
            dealerInfoResult.map((res) =>
                getReportRowForDealerRoLevel(
                    res,
                    startDateYMD,
                    endDateYMDHMS
                )
            )
        );

        // concatenate all sub-array elements
        const rowsRoLevel = resultRoLevel.flat();

        // get the RollUp report data
        const resultRollUp = await Promise.all(
            dealerInfoResult.map((res) =>
                getReportRowForDealerRollUp(
                    res,
                    startDateYMD,
                    endDateYMDHMS
                )
            )
        );

        // concatenate all sub-array elements
        const rowsRollUp = resultRollUp.flat();

        // String if results are uploaded as a excel, null otherwise
        let reportURL: string | null = null;

        // If email recipients are set, create a excel, upload it to s3, and email a link to the recipients
        if (event.emailRecipients?.length) {
            // create a excel file
            const workbook = new ExcelJS.Workbook();
            let worksheet = workbook.addWorksheet('Exhibit A - RO Level Monthly');

            // add the columns sheet
            worksheet.columns = Object.keys(rowsRoLevel[0]!).map((k) => ({header: k, key: k}));

            // add the rows
            for (let rowItem in rowsRoLevel) {
                worksheet.addRow(rowsRoLevel[rowItem]);
            }

            // add a new sheet
            worksheet = workbook.addWorksheet('Exhibit B - RO Roll-Up Monthly ');

            // add the columns sheet
            worksheet.columns = Object.keys(rowsRollUp[0]!).map((k) => ({header: k, key: k}));

            // add the rows
            for (let rowItem in rowsRollUp) {
                worksheet.addRow(rowsRollUp[rowItem]);
            }

            // write to a new buffer
            // Generate the excel string (contents of a excel file). If this data set ever gets huge, we'll need to use the callback or stream API.
            const buffer = await workbook.xlsx.writeBuffer();

            // Upload that bad boy to S3
            // I just appended a random string in the top level folder name for a bit more obfuscation
            const filePath = `video-report-3KCe4kZqXCkpZdp4/video-report_${todayYMD}_${v4()}.xlsx`;
            await s3Client.send(new PutObjectCommand({
                Bucket: reportBucket,
                Key: filePath,
                Body: buffer,
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
                        Please click <a href="${reportURL}">this link</a> to download the month end ${dealerGroupName} Dealer level Video Report for dealers on Unotifi.
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
            reportData: [ ...rowsRoLevel, ...rowsRollUp]
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
async function getReportRowForDealerRoLevel(dealerDbConnInfo: SelectDealerDbInfoResult, startDate: string, endDate: string): Promise<ReportRowROLevel[]> {
    const reportRow: ReportRowROLevel[] = [];

    // Connection to the dealer database (aka sugarcrm database) for subsequent aggregate queries
    const dealerDbConn = await mysql.createConnection({
        host: dealerDbConnInfo.IP || '',
        user: dealerDbConnInfo.user || '',
        password: dealerDbConnInfo.password || '',
        database: dealerDbConnInfo.name || '',
        timeout: 60000
    });


    try {
         // get all distinct ROs before running other queries
         const contactPersonsRows = await contactPersonsQuery(dealerDbConn, dealerDbConnInfo.internal_code + '');

        // create an associative object
        let contactPersons: any = {};
        contactPersonsRows.forEach(element => {
            contactPersons[element.id] = element.fullName;
        });

        // get all distinct ROs before running other queries
        const rows = await closedRORoLevelQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate);

        // return if no repair order found
        if (!rows.length) {
            reportRow.push({
                [ReportColumnsROLevel.VENDOR_NAME]: 'Unotifi',
                [ReportColumnsROLevel.DEALER_NAME]: dealerDbConnInfo.dealerName || '',
                [ReportColumnsROLevel.DEALER_CODE]: dealerDbConnInfo.internal_code,
                [ReportColumnsROLevel.RO_NUMBER]: '',
                [ReportColumnsROLevel.MAKE]: '',
                [ReportColumnsROLevel.MODEL]: '',
                [ReportColumnsROLevel.MODEL_YEAR]: '',
                [ReportColumnsROLevel.PAY_TYPE]: '',
                [ReportColumnsROLevel.CP_LABOR]: 0,
                [ReportColumnsROLevel.CP_PARTS]: 0,
                [ReportColumnsROLevel.SERVICE_ADVISOR]: '',
                [ReportColumnsROLevel.TECHNICIAN]: '',
                [ReportColumnsROLevel.RO_OPEN_DATE]: '',
                [ReportColumnsROLevel.RO_CLOSED_DATE]: '',
                [ReportColumnsROLevel.RO_OPEN_VALUE]: 0,
                [ReportColumnsROLevel.RO_CLOSED_VALUE]: 0,
                [ReportColumnsROLevel.RO_UPSELL_AMOUNT]: 0,
                [ReportColumnsROLevel.OPTED_IN]: '',
                [ReportColumnsROLevel.VIDEO_NO]: 0,
                [ReportColumnsROLevel.HAS_VIDEO]: 0,
                [ReportColumnsROLevel.VIEW_NO]: 0,
                [ReportColumnsROLevel.SMS_SENT_NO]: 0,
                [ReportColumnsROLevel.SMS_RECEIVED_NO]: 0,
                [ReportColumnsROLevel.PHOTO_NO]: 0,
                [ReportColumnsROLevel.RESPONSE_TIME]: 0,
                [ReportColumnsROLevel.EMAIL_SENT_NO]: 0,
                [ReportColumnsROLevel.EMAIL_RECEIVED_NO]: 'N/A',
                [ReportColumnsROLevel.EMAIL_OPENED_NO]: 0,
                [ReportColumnsROLevel.VIDEO_URLS]: '',
            });

            return reportRow;
        }

        // extract the repair order Ids
        const roIds = rows!.map(a => a.roId);

        // join these ids to a string to be used in the "IN" mysql clause
        const strROIds  = "('" + roIds.join("', '") + "')";

        // Run each aggregate column query concurrently to save time.
        // Technically, mysql queries still run serially for a single connection, but it should at least put the burden of handling that
        // on a db server instead of this Node.js app.
        const [
            laborDataRows,
            mediaDataRows,
            communicationDataRows,
            averageSmsResponseTimeInSeconds,
        ] = await Promise.all([
            laborDataQuery(dealerDbConn, strROIds),
            mediaDataQuery(dealerDbConn, strROIds),
            communicationDataQuery(dealerDbConn, strROIds),
            getAverageSmsResponseTimeInSeconds(dealerDbConn, strROIds, true)
        ])

        // extract repair orders data
        rows!.forEach((record) => {
            // get labor data for each repair order
            const laborData = laborDataRows!.filter(item => item.roId == record.roId);

            // get video data for each repair order
            const mediaData = mediaDataRows!.filter(item => item.roId == record.roId);

            // get communication data for each repair order
            const communicationData = communicationDataRows!.filter(item => item.roId == record.roId);

            reportRow.push({
                [ReportColumnsROLevel.VENDOR_NAME]: 'Unotifi',
                [ReportColumnsROLevel.DEALER_NAME]: dealerDbConnInfo.dealerName || '',
                [ReportColumnsROLevel.DEALER_CODE]: dealerDbConnInfo.internal_code,
                [ReportColumnsROLevel.RO_NUMBER]: record.roNumber,
                [ReportColumnsROLevel.MAKE]: record.makeName,
                [ReportColumnsROLevel.MODEL]: record.modelName,
                [ReportColumnsROLevel.MODEL_YEAR]: record.modelYear,
                [ReportColumnsROLevel.PAY_TYPE]: laborData && laborData.length ? laborData[0]!.payType : '',
                [ReportColumnsROLevel.CP_LABOR]: laborData && laborData.length ? laborData[0]!.laborAmount : 0,
                [ReportColumnsROLevel.CP_PARTS]: laborData && laborData.length ? laborData[0]!.partsAmount : 0,
                [ReportColumnsROLevel.SERVICE_ADVISOR]: record.serviceAdvisor && contactPersons[record.serviceAdvisor] ? contactPersons[record.serviceAdvisor] : record.serviceAdvisor,
                [ReportColumnsROLevel.TECHNICIAN]: record.technician && contactPersons[record.technician] ? contactPersons[record.technician] : record.technician,
                [ReportColumnsROLevel.RO_OPEN_DATE]: record.roOpenDate,
                [ReportColumnsROLevel.RO_CLOSED_DATE]: record.roClosedDate,
                [ReportColumnsROLevel.RO_OPEN_VALUE]: +record.roOpenValue,
                [ReportColumnsROLevel.RO_CLOSED_VALUE]: +record.roClosedValue,
                [ReportColumnsROLevel.RO_UPSELL_AMOUNT]: +record.roUpsellAmount,
                [ReportColumnsROLevel.OPTED_IN]: record.optedIn,
                [ReportColumnsROLevel.VIDEO_NO]: mediaData && mediaData.length ? mediaData[0]!.videoNo : 0,
                [ReportColumnsROLevel.HAS_VIDEO]: mediaData && mediaData.length ? mediaData[0]!.hasVideo : 0,
                [ReportColumnsROLevel.VIEW_NO]: mediaData && mediaData.length ? mediaData[0]!.viewNo : 0,
                [ReportColumnsROLevel.SMS_SENT_NO]: communicationData && communicationData.length ? communicationData[0]!.smsSentNo : 0,
                [ReportColumnsROLevel.SMS_RECEIVED_NO]: communicationData && communicationData.length ? communicationData[0]!.smsReceivedNo : 0,
                [ReportColumnsROLevel.PHOTO_NO]: mediaData && mediaData.length ? mediaData[0]!.photoNo : 0,
                [ReportColumnsROLevel.RESPONSE_TIME]: Object.keys(averageSmsResponseTimeInSeconds).length && averageSmsResponseTimeInSeconds[record.roId] ?
                    moment.utc(averageSmsResponseTimeInSeconds[record.roId]! * 1000).format("HH:mm:ss") :
                    0,
                [ReportColumnsROLevel.EMAIL_SENT_NO]: communicationData && communicationData.length ? communicationData[0]!.emailSentNo : 0,
                [ReportColumnsROLevel.EMAIL_RECEIVED_NO]: 'N/A',
                [ReportColumnsROLevel.EMAIL_OPENED_NO]: communicationData && communicationData.length ? communicationData[0]!.emailOpenedNo : 0,
                [ReportColumnsROLevel.VIDEO_URLS]: mediaData && mediaData.length ? mediaData[0]!.videoURLs : ''
            });
        });


        return reportRow;
    } catch (err) {
        throw err;
    } finally {
        // Close the db connection, even if there's an error. This avoids a hanging process.
        await dealerDbConn.end();
    }
}

/**
 * Concurrently executes all aggregate queries for a dealer. This function should also be called concurrently for each dealer (e.g. using Promise.all).
 */
async function getReportRowForDealerRollUp(dealerDbConnInfo: SelectDealerDbInfoResult, startDate: string, endDate: string): Promise<ReportRowRollUp> {
    const reportRow: ReportRowRollUp = {
        [ReportColumnsRollUp.VENDOR_NAME]: 'Unotifi',
        [ReportColumnsRollUp.DEALER_NAME]: dealerDbConnInfo.dealerName || '',
        [ReportColumnsRollUp.DEALER_CODE]: dealerDbConnInfo.internal_code
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
        const rows = await closedRORollUpQuery(dealerDbConn, dealerDbConnInfo.internal_code + '', startDate, endDate);

        // return if no repair order found
        if (!rows.length) {
            reportRow[ReportColumnsRollUp.CLOSED_ROS] = 0;
            reportRow[ReportColumnsRollUp.ROS_CONTAINING_VIDEO] = 0;
            reportRow[ReportColumnsRollUp.AVERAGE_CP_LABOR] = 0;
            reportRow[ReportColumnsRollUp.AVERAGE_CP_PARTS] = 0;
            reportRow[ReportColumnsRollUp.AVERAGE_RO_OPEN_VALUE] = 0;
            reportRow[ReportColumnsRollUp.AVERAGE_RO_CLOSED_VALUE] = 0;
            reportRow[ReportColumnsRollUp.AVERAGE_UPSELL_AMOUNT] = 0;
            reportRow[ReportColumnsRollUp.AVERAGE_RESPONSE_TIME] = 0;
            reportRow[ReportColumnsRollUp.AVERAGE_VIDEO_LENGTH] = 0;
            reportRow[ReportColumnsRollUp.OPTED_IN_ROS] = 0;
            reportRow[ReportColumnsRollUp.OPTED_OUT_ROS] = 0;
            reportRow[ReportColumnsRollUp.AVERAGE_SMS_SENT_TO_CUSTOMER] = 0;
            reportRow[ReportColumnsRollUp.AVERAGE_PHOTOS_SENT_TO_CUSTOMER] = 0;
            reportRow[ReportColumnsRollUp.AVERAGE_EMAIL_OPENED] = 0;

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
            averageVideoLengthQuery(dealerDbConn, strROIds),
            numberOptedInROsQuery(dealerDbConn, strROIds),
            numberOptedOutROsQuery(dealerDbConn, strROIds),
            averageSMSSentQuery(dealerDbConn, strROIds),
            averagePhotoSentQuery(dealerDbConn, strROIds),
            averageVideoViewsQuery(dealerDbConn, strROIds)
        ])

        // using destructuring with Promise.all requires to receive the same result type for each method ({ [key: string]: number; })
        // since getAverageSmsResponseTimeInSeconds returns other type, use a separate call
        const averageSmsResponseTimeInSeconds = await getAverageSmsResponseTimeInSeconds(dealerDbConn, strROIds, false);

        // Assign all the db results to the CSV row
        reportRow[ReportColumnsRollUp.CLOSED_ROS] = roIds.length;
        reportRow[ReportColumnsRollUp.ROS_CONTAINING_VIDEO] = totalROsWithVideoCount;
        reportRow[ReportColumnsRollUp.AVERAGE_CP_LABOR] = avgLabor;
        reportRow[ReportColumnsRollUp.AVERAGE_CP_PARTS] = avgParts;
        reportRow[ReportColumnsRollUp.AVERAGE_RO_OPEN_VALUE] = averageROOpenValue;
        reportRow[ReportColumnsRollUp.AVERAGE_RO_CLOSED_VALUE] = avgROClosed;
        reportRow[ReportColumnsRollUp.AVERAGE_UPSELL_AMOUNT] = averageUpsellAmount;
        reportRow[ReportColumnsRollUp.AVERAGE_RESPONSE_TIME] = averageSmsResponseTimeInSeconds && averageSmsResponseTimeInSeconds['0'] ?
            moment.utc(averageSmsResponseTimeInSeconds['0'] * 1000).format("HH:mm:ss") :
            0,
        reportRow[ReportColumnsRollUp.AVERAGE_VIDEO_LENGTH] = averageVideoLength ?
            moment.utc(averageVideoLength * 1000).format("HH:mm:ss") :
            0;
        reportRow[ReportColumnsRollUp.OPTED_IN_ROS] = numberOptedInROs;
        reportRow[ReportColumnsRollUp.OPTED_OUT_ROS] = numberOptedOutROs;
        reportRow[ReportColumnsRollUp.AVERAGE_SMS_SENT_TO_CUSTOMER] = averageSMSSent;
        reportRow[ReportColumnsRollUp.AVERAGE_PHOTOS_SENT_TO_CUSTOMER] = averagePhotoSent;
        reportRow[ReportColumnsRollUp.AVERAGE_EMAIL_OPENED] = averageVideoViews;

        return reportRow;
    } catch (err) {
        throw err;
    } finally {
        // Close the db connection, even if there's an error. This avoids a hanging process.
        await dealerDbConn.end();
    }
}

async function closedRORoLevelQuery(conn: mysql.Connection, dealerID: string, startDate: string, endDate: string) {
    // Type asserting as ROQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const result: ROQueryResult = await conn.query(
        `
            SELECT DISTINCT
                auto_repair_order.id AS roId,
                auto_repair_order.name AS roNumber,
                auto_v_make.name AS makeName,
                auto_v_model.name AS modelName,
                auto_vehicle.year AS modelYear,
                DATE_FORMAT(auto_repair_order.service_open_date, '%m/%d/%y') AS roOpenDate,
                DATE_FORMAT(auto_repair_order.service_closed_date, '%m/%d/%y') AS roClosedDate,
                CAST(IFNULL(auto_repair_order.repair_order_amount_total_original, REPLACE(IFNULL(auto_repair_order.repair_order_amount_total, 0), ',', '')) AS DECIMAL (10 , 2 )) AS roOpenValue,
                CAST(REPLACE(IFNULL(auto_repair_order.repair_order_amount_total, 0), ',', '') AS DECIMAL (10 , 2 )) AS roClosedValue,
                CAST(REPLACE(IFNULL(auto_repair_order.repair_order_amount_total, 0), ',', '') - (IFNULL(auto_repair_order.repair_order_amount_total_original, REPLACE(IFNULL(auto_repair_order.repair_order_amount_total, 0), ',', ''))) AS DECIMAL (10 , 2 )) AS roUpsellAmount,
                auto_repair_order.service_advisor_id AS serviceAdvisor,
                auto_repair_order.technician_id AS technician,
                NOT auto_customer.do_not_text_flag as optedIn
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
                    LEFT JOIN
                auto_vehiclauto_v_model_c ON auto_vehiclauto_v_model_c.auto_vehicc751vehicle_idb = auto_vehicle.id
                    AND auto_vehiclauto_v_model_c.deleted = 0
                    LEFT JOIN
                auto_v_model ON auto_v_model.id = auto_vehiclauto_v_model_c.auto_vehiccbc1v_model_ida
                    AND auto_v_model.deleted = 0
                    LEFT JOIN
                auto_v_mode_auto_v_make_c ON auto_v_mode_auto_v_make_c.auto_v_mod4fdfv_model_idb = auto_v_model.id
                    AND auto_v_mode_auto_v_make_c.deleted = 0
                    LEFT JOIN
                auto_v_make ON auto_v_mode_auto_v_make_c.auto_v_mod95f1_v_make_ida = auto_v_make.id
                    AND auto_v_make.deleted = 0                    
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

async function contactPersonsQuery(conn: mysql.Connection, dealerID: string) {
    // Type asserting as ContactPersonsResult here because mysql.query types don't allow you to pass in a type argument...
    const result: ContactPersonsResult = await conn.query(
        `
            SELECT DISTINCT
                auto_contact_person.name AS id,
                COALESCE(auto_contact_person.full_name, TRIM(CONCAT(auto_contact_person.first_name, ' ', auto_contact_person.last_name))) AS fullName
            FROM
                auto_dealer
                    INNER JOIN
                auto_contac_auto_dealer_c ON auto_contac_auto_dealer_c.auto_contafb84_dealer_ida = auto_dealer.id
                    AND auto_contac_auto_dealer_c.deleted = 0
                    INNER JOIN
                auto_contact_person ON auto_contac_auto_dealer_c.auto_contaff8f_person_idb = auto_contact_person.id
                    AND auto_contact_person.deleted = 0
            WHERE
                auto_dealer.integralink_code = ?
                    AND (auto_contact_person.full_name IS NOT NULL
                    OR auto_contact_person.first_name IS NOT NULL
                    OR auto_contact_person.last_name IS NOT NULL)
        `,
        [dealerID]
    );

    return result ? result : [];
}

async function laborDataQuery(
    conn: mysql.Connection,
    roIds: string
) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    // because the apostrophe parameter is escaped in a query parameter, put directly the value in the query
    const result: ROLaborQueryResult = await conn.query(
        `
            SELECT
                auto_repair_order.id as roId,
                CAST(REPLACE(auto_ro_labor.labor_amount, ',', '') / COUNT(DISTINCT auto_repair_order.id) AS DECIMAL (10 , 2 )) AS laborAmount,
                CAST(REPLACE(auto_ro_labor.parts_amount, ',', '') / COUNT(DISTINCT auto_repair_order.id) AS DECIMAL (10 , 2 )) AS partsAmount,
                REPLACE(REPLACE(GROUP_CONCAT(DISTINCT auto_ro_labor.event_repair_labor_pay_type ORDER BY event_repair_labor_pay_type ASC SEPARATOR ', '), 'C', 'CP'), 'W', 'WP') AS payType
            FROM
                auto_repair_order
                    INNER JOIN
                auto_ro_labrepair_order_c ON auto_ro_labrepair_order_c.auto_ro_laada9r_order_ida = auto_repair_order.id
                    AND auto_ro_labrepair_order_c.deleted = 0
                    INNER JOIN
                auto_ro_labor ON auto_ro_labrepair_order_c.auto_ro_la1301o_labor_idb = auto_ro_labor.id
            WHERE auto_repair_order.id IN ` + roIds + `
                AND auto_ro_labor.event_repair_labor_pay_type in ('C', 'W')
            GROUP BY auto_repair_order.id
        `
    );

    return result && result[0] ? result : [];
}

async function mediaDataQuery(
    conn: mysql.Connection,
    roIds: string
) {
    // Type asserting as CountQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const result: MediaQueryResult = await conn.query(
        `
            SELECT 
                auto_repair_order.id as roId,
                auto_repair_order.has_videos AS hasVideo,
                COUNT(DISTINCT IF(auto_media_file.file_guid IS NOT NULL AND auto_media_file.file_length IS NOT NULL, auto_media_file.id, NULL)) AS videoNo,
                COUNT(DISTINCT IF(auto_media_file.file_guid IS NOT NULL AND auto_media_file.file_length IS NOT NULL, auto_media_file_view.id, NULL)) AS viewNo,
                COUNT(DISTINCT IF(auto_media_file.id IS NOT NULL AND auto_media_file.file_guid IS NULL, auto_media_file.id, NULL)) AS photoNo,
                GROUP_CONCAT(DISTINCT IF(auto_media_file.file_guid IS NOT NULL AND auto_media_file.file_length IS NOT NULL, auto_media_file.name, NULL) ORDER BY auto_media_file.name ASC SEPARATOR ', ') AS videoURLs
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
                    LEFT JOIN
                auto_media_file ON auto_event.id = auto_media_file.event_id
                    AND auto_media_file.date_entered BETWEEN auto_repair_order.service_open_date AND ADDTIME(auto_repair_order.service_closed_date, '23:59:59')
                    LEFT JOIN
                auto_media_file_view ON auto_media_file.id = auto_media_file_view.auto_media_file_id_c
            WHERE
                auto_repair_order.id IN ` + roIds + `
                    AND auto_event.body_type = 'Text'
                    AND auto_event.generated_from = 'Comunicator'
                    AND auto_event.type = 'Not-Pending'
            GROUP BY auto_repair_order.id
        `
    );

    return result && result[0] ? result : [];
}

async function communicationDataQuery(
    conn: mysql.Connection,
    roIds: string
) {
    // Type asserting as CommunicationQueryResult here because mysql.query types don't allow you to pass in a type argument...
    const result: CommunicationQueryResult = await conn.query(
        `
            SELECT
                auto_repair_order.id AS roId,
                COUNT(DISTINCT IF(auto_event.body_type = 'Text' AND auto_event.generated_from = 'Comunicator' AND auto_event.type = 'Not-Pending', auto_event.id, NULL)) AS smsSentNo,
                COUNT(DISTINCT IF(auto_event.body_type = 'Text' AND auto_event.generated_from = 'Reply' AND auto_event.type = 'Reply', auto_event.id, NULL)) AS smsReceivedNo,
                COUNT(DISTINCT IF(auto_campaign.type = 'Email' AND auto_event.type = 'Sent', auto_event.id, NULL)) AS emailSentNo,
                COUNT(DISTINCT IF(auto_campaign.type = 'Email' AND auto_event.type IN ('Open', 'Click Through'), auto_event.id, NULL)) AS emailOpenedNo
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
                auto_recipiuto_campaign_c ON auto_recipiuto_campaign_c.auto_recip885bcipient_idb = auto_recipient.id
                    AND auto_recipiuto_campaign_c.deleted = 0
                    LEFT JOIN
                auto_campaign ON auto_campaign.id = auto_recipiuto_campaign_c.auto_recip8ba3ampaign_ida
                    AND auto_campaign.deleted = 0
            WHERE 
                  auto_repair_order.id IN ` + roIds + `
                  AND auto_event.date_entered BETWEEN auto_repair_order.service_open_date AND ADDTIME(auto_repair_order.service_closed_date, '23:59:59')
            GROUP BY auto_repair_order.id      
        `
    );

    return result && result[0] ? result : []
}

async function closedRORollUpQuery(conn: mysql.Connection, dealerID: string, startDate: string, endDate: string) {
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
            AND auto_media_file.date_entered BETWEEN auto_repair_order.service_open_date AND ADDTIME(auto_repair_order.service_closed_date, '23:59:59')
            AND auto_media_file.file_guid IS NULL
    `
  );
  return countResult && countResult[0] && countResult[0].total ? countResult[0].total : 0;
}

async function getAverageSmsResponseTimeInSeconds(
    conn: mysql.Connection,
    roIds: string,
    isPerRo: boolean
) {
    const textEvents: TextEvents = await conn.query(
        `
            SELECT DISTINCT
                auto_repair_order.id AS roId, 
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
              AND auto_event.sent_date BETWEEN auto_repair_order.service_open_date AND ADDTIME(auto_repair_order.service_closed_date, '23:59:59')
              AND auto_event.body_type = 'Text'
              AND ((auto_event.generated_from = 'Comunicator'
                AND auto_event.type = 'Not-Pending'
                AND auto_media_file.file_guid IS NOT NULL)
                OR (auto_event.generated_from = 'Reply'
                    AND auto_event.type = 'Reply'))
            ORDER BY ` + (isPerRo ? 'auto_repair_order.id' : 'auto_recipient.id') + `, auto_event.sent_date
        `
    );

    const outboundTextEvents: { [key: string]: string } = {};
    const inboundTextEvents: { [key: string]: string } = {};
    let responseTimesInSeconds: { [key: string]: number[] } = {};
    let averageResponseTimesInSeconds: { [key: string]: number } = {};

    // Process the text events in order to find the average response time
    textEvents.forEach((textEvent, index) => {
        const groupBy: string = isPerRo ?
            textEvent.roId ?? '' :
                textEvent.recipientId ?? '';

        // Find outbound text events
        if (textEvent.eventGeneratedFrom == 'Comunicator' && textEvent.eventType == 'Not-Pending') {
            outboundTextEvents[groupBy] = textEvent.eventSentDate;
        }

        // Find inbound text events
        if (textEvent.eventGeneratedFrom == 'Reply' && textEvent.eventType == 'Reply') {
            inboundTextEvents[groupBy] = textEvent.eventSentDate;
        }

        // Calculate response times
        if (outboundTextEvents[groupBy] && inboundTextEvents[groupBy]) {
            const outboundTextEventDate = new Date(outboundTextEvents[groupBy]!);
            const inboundTextEventDate = new Date(inboundTextEvents[groupBy]!);

            // Check diff only for consecutive outbound/inbound pair
            if (
                index > 0 &&
                textEvents[index - 1]!.eventGeneratedFrom == 'Comunicator' &&
                textEvents[index - 1]!.mediaFileId &&
                textEvent.eventGeneratedFrom == 'Reply' &&
                (!isPerRo || textEvents[index - 1]!.roId == textEvent.roId)
            ) {
                // ignore the responses greater than one day
                const diff = getDateDifferenceInSeconds(outboundTextEventDate, inboundTextEventDate);
                if (diff < 86400) {
                    if (isPerRo) {
                        if (!responseTimesInSeconds.hasOwnProperty(groupBy)) {
                            responseTimesInSeconds[groupBy] = [];
                        }

                        responseTimesInSeconds[groupBy]!.push(diff);
                    } else {
                        // put a default key = 0
                        if (!responseTimesInSeconds.hasOwnProperty('0')) {
                            responseTimesInSeconds['0'] = [];
                        }

                        responseTimesInSeconds['0']!.push(diff);
                    }
                }
            }
        }
    });

    // get the average response time for each item
    Object.entries(responseTimesInSeconds).forEach(([key, value]) =>
        averageResponseTimesInSeconds[key] = average(value)
    );

    return averageResponseTimesInSeconds && Object.keys(averageResponseTimesInSeconds).length ? averageResponseTimesInSeconds : {};
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
            AND auto_media_file.date_entered BETWEEN auto_repair_order.service_open_date AND ADDTIME(auto_repair_order.service_closed_date, '23:59:59')  
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
            AND auto_media_file.date_entered BETWEEN auto_repair_order.service_open_date AND ADDTIME(auto_repair_order.service_closed_date, '23:59:59')
            AND auto_media_file.file_guid IS NOT NULL
      `
  );
  return countResult && countResult[0] && countResult[0].total ? countResult[0].total : 0;
}

/////////////////////////////////////////////////
// Constants
/////////////////////////////////////////////////

/**
 * Represents the column names of the output sheet file
 */

enum ReportColumnsROLevel {
    VENDOR_NAME = 'Vendor Name',
    DEALER_NAME = 'Dealer Name',
    DEALER_CODE = 'Dealer Code',
    RO_NUMBER = 'RO #',
    MAKE = 'Make',
    MODEL = 'Model',
    MODEL_YEAR = 'Model Year',
    PAY_TYPE = 'Pay Type',
    CP_LABOR = 'CP Labor $',
    CP_PARTS = 'CP Parts $',
    SERVICE_ADVISOR = 'Service Advisor',
    TECHNICIAN = 'Technician',
    RO_OPEN_DATE = 'RO Open Date',
    RO_CLOSED_DATE = 'RO Closed Date',
    RO_OPEN_VALUE = 'RO Open Value',
    RO_CLOSED_VALUE = 'RO Closed Value',
    RO_UPSELL_AMOUNT = 'Upsell Amount',
    OPTED_IN = 'Opted In',
    VIDEO_NO = ' # of Videos Sent',
    HAS_VIDEO = 'Video on RO (YES=1, NO=0)',
    VIEW_NO = '# of Times Video was Viewed',
    SMS_SENT_NO = '# of SMS Sent',
    SMS_RECEIVED_NO = '# of SMS Received',
    PHOTO_NO = '# of Photos Sent',
    RESPONSE_TIME = 'Response Time',
    EMAIL_SENT_NO = '# of Email Sent',
    EMAIL_RECEIVED_NO = '# of Email Received',
    EMAIL_OPENED_NO = '# of Email Opened/Microsite Clicked',
    VIDEO_URLS = 'Microsite Link to Tech Video'
}

enum ReportColumnsRollUp {
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
    AVERAGE_EMAIL_OPENED = 'Average # of Times Video was Viewed'
}

/////////////////////////////////////////////////
// Types
/////////////////////////////////////////////////

// Query Result
type QueryResult = { id: string }[];

// Aggregate Query Result
type AggregateQueryResult = { total: number }[];

// Repair Orders Query Result
type ROQueryResult = {
    roId: string,
    roNumber: string,
    makeName: string,
    modelName: string,
    modelYear: string,
    payType: string,
    cpLabor: string,
    cpParts: string,
    serviceAdvisor: string,
    technician: string,
    roOpenDate: string,
    roClosedDate: string,
    roOpenValue: string,
    roClosedValue: string,
    roUpsellAmount: string,
    optedIn: string
}[];

// Contact Persons Query Result
type ContactPersonsResult = {
    id: string,
    fullName: string
}[];

// RO Labor Query Result
type ROLaborQueryResult = {
    roId: string,
    laborAmount: number,
    partsAmount: number,
    payType: string
}[];

// Media Query Result
type MediaQueryResult = {
    roId: string,
    hasVideo: number,
    videoNo: number,
    viewNo: number,
    photoNo: number,
    videoURLs: string
}[];

// Communication Query Result
type CommunicationQueryResult = {
    roId: string,
    smsSentNo: number,
    smsReceivedNo: number,
    emailSentNo: number,
    emailOpenedNo: number
}[];

// Average Response Time Result
type TextEvents = {
    roId: string
    recipientId: string
    eventId: string
    eventSentDate: string
    eventType: string
    eventGeneratedFrom: string
    mediaFileId: string
}[];

/////////////////////////////////////////////////
// Interfaces
/////////////////////////////////////////////////


/**
 * Represents a row of the output sheet file
 */
interface ReportRowROLevel {
    [ReportColumnsROLevel.VENDOR_NAME]?: string;
    [ReportColumnsROLevel.DEALER_NAME]?: string;
    [ReportColumnsROLevel.DEALER_CODE]?: string;
    [ReportColumnsROLevel.RO_NUMBER]?: string;
    [ReportColumnsROLevel.MAKE]?: string;
    [ReportColumnsROLevel.MODEL]?: string;
    [ReportColumnsROLevel.MODEL_YEAR]?: string;
    [ReportColumnsROLevel.PAY_TYPE]?: string;
    [ReportColumnsROLevel.CP_LABOR]?: number;
    [ReportColumnsROLevel.CP_PARTS]?: number;
    [ReportColumnsROLevel.SERVICE_ADVISOR]?: string;
    [ReportColumnsROLevel.TECHNICIAN]?: string;
    [ReportColumnsROLevel.RO_OPEN_DATE]?: string;
    [ReportColumnsROLevel.RO_CLOSED_DATE]?: string;
    [ReportColumnsROLevel.RO_OPEN_VALUE]?: number;
    [ReportColumnsROLevel.RO_CLOSED_VALUE]?: number;
    [ReportColumnsROLevel.RO_UPSELL_AMOUNT]?: number;
    [ReportColumnsROLevel.OPTED_IN]?: string;
    [ReportColumnsROLevel.HAS_VIDEO]?: number;
    [ReportColumnsROLevel.VIDEO_NO]?: number;
    [ReportColumnsROLevel.VIEW_NO]?: number;
    [ReportColumnsROLevel.SMS_SENT_NO]?: number;
    [ReportColumnsROLevel.SMS_RECEIVED_NO]?: number;
    [ReportColumnsROLevel.PHOTO_NO]?: number;
    [ReportColumnsROLevel.RESPONSE_TIME]?: string | number;
    [ReportColumnsROLevel.EMAIL_SENT_NO]?: number;
    [ReportColumnsROLevel.EMAIL_RECEIVED_NO]?: string;
    [ReportColumnsROLevel.EMAIL_OPENED_NO]?: number;
    [ReportColumnsROLevel.VIDEO_URLS]?: string;
}

/**
 * Represents a row of the output sheet file
 */
interface ReportRowRollUp {
    [ReportColumnsRollUp.VENDOR_NAME]?: string;
    [ReportColumnsRollUp.DEALER_NAME]?: string;
    [ReportColumnsRollUp.DEALER_CODE]?: string;
    [ReportColumnsRollUp.CLOSED_ROS]?: number;
    [ReportColumnsRollUp.ROS_CONTAINING_VIDEO]?: number;
    [ReportColumnsRollUp.AVERAGE_CP_LABOR]?: number;
    [ReportColumnsRollUp.AVERAGE_CP_PARTS]?: number;
    [ReportColumnsRollUp.AVERAGE_RO_OPEN_VALUE]?: number;
    [ReportColumnsRollUp.AVERAGE_RO_CLOSED_VALUE]?: number;
    [ReportColumnsRollUp.AVERAGE_UPSELL_AMOUNT]?: number;
    [ReportColumnsRollUp.AVERAGE_RESPONSE_TIME]?: string | number;
    [ReportColumnsRollUp.AVERAGE_VIDEO_LENGTH]?: string | number;
    [ReportColumnsRollUp.OPTED_IN_ROS]?: number;
    [ReportColumnsRollUp.OPTED_OUT_ROS]?: number;
    [ReportColumnsRollUp.AVERAGE_SMS_SENT_TO_CUSTOMER]?: number;
    [ReportColumnsRollUp.AVERAGE_PHOTOS_SENT_TO_CUSTOMER]?: number;
    [ReportColumnsRollUp.AVERAGE_EMAIL_OPENED]?: number;
}

/**
 * JSON input of the lambda function
 */

interface VideoReportEvent {
    dealerIDs?: string[];
    dealerGroupName?: string;
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
