import { toUTCDateTimeString } from '../modules/DateHandler';

/**
* BDC Results
*/

// Total no. of texts, total no. of emails, total no. of repair orders, and repair order totals
export const getROICampaigns = (dealerIntegralinkCode: string|number, startDate: Date, endDate: Date): string => {
    const startDateString: string = toUTCDateTimeString(startDate);
    const endDateString: string = toUTCDateTimeString(endDate);

    return `SELECT
            auto_campaign.id as campaignId,
            auto_campaign.name AS campaignName,
            auto_campaign.type AS campaignType,
            COUNT(
                DISTINCT (
                    IF(
                        auto_event.body_type = 'Text',
                        auto_event.id,
                        NULL
                    )
                )
            ) AS textMessageNo,
            COUNT(
                DISTINCT (
                    IF(auto_event.type = 'Sent', auto_event.id, NULL)
                )
            ) AS emailNo,
            COUNT(DISTINCT auto_repair_order.id) AS roNo,
            SUM(replace(repair_order_amount_total, ',', '')) AS roTotal
        FROM
            auto_event
            INNER JOIN auto_event_to_recipient_c ON auto_event.id = auto_event_to_recipient_c.auto_eventfa83o_event_idb
            AND auto_event_to_recipient_c.deleted = 0
            INNER JOIN auto_recipient ON auto_event_to_recipient_c.auto_eventa735cipient_ida = auto_recipient.id
            AND auto_recipient.deleted = 0
            INNER JOIN auto_recipiuto_campaign_c ON auto_recipiuto_campaign_c.auto_recip885bcipient_idb = auto_recipient.id
            AND auto_recipiuto_campaign_c.deleted = 0
            INNER JOIN auto_campaign ON auto_campaign.id = auto_recipiuto_campaign_c.auto_recip8ba3ampaign_ida
            AND auto_campaign.deleted = 0
            INNER JOIN auto_campai_auto_dealer_c ON auto_campai_auto_dealer_c.auto_campa2d6bampaign_idb = auto_campaign.id
            AND auto_campai_auto_dealer_c.deleted = 0
            INNER JOIN auto_dealer ON auto_campai_auto_dealer_c.auto_campa1fd9_dealer_ida = auto_dealer.id
            LEFT JOIN auto_repair_order FORCE INDEX FOR
            JOIN (idx_auto_repair_order_auto_event_id_c) ON auto_repair_order.auto_event_id_c = auto_event.id
            AND auto_repair_order.deleted = 0
        WHERE
            auto_dealer.integralink_code = '${dealerIntegralinkCode}'
            AND auto_campaign.included_in_roi = 1
            AND auto_event.sent_date BETWEEN '${startDateString}'
            AND '${endDateString}'
            AND (
                auto_event.type = 'Sent'
                OR (
                    auto_event.body_type = 'Text'
                    AND auto_event.generated_from = 'System'
                    AND auto_event.type = 'Not-Pending'
                )
            )
        GROUP BY
            auto_campaign.id
        ORDER BY
            auto_campaign.name;`;
}

export const getAppointments = (campaignIds: string[], startDate: Date, endDate: Date): string => {
    const campaignIdsString: string = campaignIds.map(campaignId => `'${campaignId}'`).toString();
    const startDateString: string = toUTCDateTimeString(startDate);
    const endDateString: string = toUTCDateTimeString(endDate);

    return `SELECT
            auto_campaign.id AS campaignId,
            COUNT(
                IF(
                    opportunities.last_contacted_date < auto_appointment.date_entered,
                    auto_appointment.date_entered,
                    auto_appointment.reschedule_date
                )
            ) AS appointmentNo,
            SUM(
                IF(
                    auto_appointment.appointment_with_ade_ro = 1
                    OR auto_appointment.appointment_with_ro = 1,
                    1,
                    0
                )
            ) AS arrivedAppointmentNo
        FROM
            auto_campaign
            INNER JOIN auto_recipiuto_campaign_c ON auto_campaign.id = auto_recipiuto_campaign_c.auto_recip8ba3ampaign_ida
            AND auto_recipiuto_campaign_c.deleted = 0
            INNER JOIN auto_recipient ON auto_recipiuto_campaign_c.auto_recip885bcipient_idb = auto_recipient.id
            AND auto_recipient.deleted = 0
            INNER JOIN auto_vehicle ON auto_recipient.auto_vehicle_id_c = auto_vehicle.id
            AND auto_vehicle.deleted = 0
            INNER JOIN auto_vehiclpportunities_c ON auto_vehiclpportunities_c.auto_vehicce49vehicle_ida = auto_vehicle.id
            AND auto_vehiclpportunities_c.deleted = 0
            INNER JOIN opportunities ON opportunities.id = auto_vehiclpportunities_c.auto_vehicb672unities_idb
            AND opportunities.deleted = 0
            INNER JOIN auto_appointment ON auto_appointment.opportunity_id_c = opportunities.id
            AND auto_appointment.deleted = 0
        WHERE
            auto_campaign.id IN ('${campaignIdsString}')
            AND opportunities.last_contacted_date IS NOT NULL
            AND (
                (
                    IF(
                        opportunities.last_contacted_date < auto_appointment.date_entered,
                        auto_appointment.date_entered,
                        auto_appointment.reschedule_date
                    ) >= '${startDateString}'
                    AND IF(
                        opportunities.last_contacted_date < auto_appointment.date_entered,
                        auto_appointment.date_entered,
                        auto_appointment.reschedule_date
                    ) <= '${endDateString}'
                )
                OR (
                    auto_appointment.appointment_date BETWEEN '${startDateString}'
                    AND '${endDateString}'
                )
            )
        GROUP BY
            auto_campaign.id`;
}
