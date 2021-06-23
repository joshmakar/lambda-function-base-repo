import * as mysql from 'promise-mysql';

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

// Define types for tables in the unotifi_com_index database
export interface Dealer {
    iddealer: string;
    name?: string;
    address_line_1?: string;
    address_line_2?: string;
    city_name?: string;
    state_code?: string;
    zip_code?: string;
    country_code?: string;
    latitude?: number;
    longitude?: number;
    internal_code: string;
    voip_phone_number?: string;
    voip_phone_number_sid?: string;
    print_campaign_limit: number;
    print_campaign_limit_interval?: string;
    print_campaign_limit_period?: number;
    print_campaign_limit_total?: number;
    APIKey?: string;
    ClientAPIKey?: string;
    instance_idinstance: string;
    created_user: string;
    modified_user: string;
    created_date: Date;
    modified_date: Date;
    disable: number;
    imported_auto_dealer_id?: string;
    external_code: string;
    dealer_group_id?: string;
    watson_credentials: string;
    import_batch_data: number;
    import_real_time_data: number;
    filter_data_by_vehicle_make: number;
    filter_data_by_vehicle_make_details?: string;
    holiday_notification: Date;
    timezone: string;
    send_to_cerebri: number;
    send_docs_to_service: number;
    redcap_code?: string;
    deleted_date?: unknown;
    dealer_management_system_id: string;
    sales_rep?: string;
    technical_onboarding_rep?: string;
    onboarding_rep?: string;
    account_rep: string;
    lob_enabled: number;
    lob_started_at?: unknown;
    lob_address_id?: string;
    lob_address_last_updated_at?: unknown;
    launch_at?: unknown;
}
