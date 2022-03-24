import { toUTCDateTimeString } from '../modules/DateHandler';

/**
 * Get DB connection info query for dealerships by their IDs
 * @param dealershipIds Array of dealership id's to query
 * @returns string query
 */
export const getDealershipsDBInfo = (dealershipIds: Array<string>) => {
  return `
    SELECT dealer.iddealer, dealer.internal_code, dealer.name as dealerName, database.name, database.user, database.password, databaseserver.IP FROM dealer 
    INNER JOIN instance ON instance.idinstance = dealer.instance_idinstance
    INNER JOIN \`database\` ON database.iddatabase = instance.database_iddatabase
    INNER JOIN databaseserver ON databaseserver.iddatabaseserver = database.databaseServer_iddatabaseServer
    WHERE iddealer IN (${dealershipIds.join(',')})
  `;
}

/**
 * BDC Results
 */

// Total Opportunities, Total Opportunities Contacted
export const getOpportunitiesContactedQuery = (dealerIntegralinkCode: string|number, startDate: Date, endDate: Date): string => {
  const startDateString: string = toUTCDateTimeString(startDate);
  const endDateString: string = toUTCDateTimeString(endDate);

  return `SELECT
        auto_campaign.name AS autoCampaignName,
        COUNT(DISTINCT opportunities.id) AS totalOpportunities,
        COUNT(last_contacted_date) AS totalOpportunitiesContacted,
        COUNT(
            DISTINCT IF(
                auto_vehicle.sold = 1
                AND auto_vehicle_audit.id IS NOT NULL,
                auto_vehicle.id,
                NULL
            )
        ) AS soldVehicles
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
        INNER JOIN auto_vehiclpportunities_c ON auto_vehiclpportunities_c.auto_vehicce49vehicle_ida = auto_vehicle.id
        AND auto_vehiclpportunities_c.deleted = 0
        INNER JOIN opportunities ON opportunities.id = auto_vehiclpportunities_c.auto_vehicb672unities_idb
        AND opportunities.deleted = 0
        INNER JOIN auto_campaipportunities_c ON auto_campaipportunities_c.auto_campae5baunities_idb = opportunities.id
        AND auto_campaipportunities_c.deleted = 0
        INNER JOIN auto_campaign ON auto_campaipportunities_c.auto_campa1b75ampaign_ida = auto_campaign.id
        AND auto_campaign.deleted = 0
        LEFT JOIN auto_vehicle_audit ON auto_vehicle_audit.parent_id = auto_vehicle.id
        AND field_name = 'sold'
        AND auto_vehicle_audit.before_value_string = '0'
        AND auto_vehicle_audit.after_value_string = '1'
        AND auto_vehicle_audit.date_created BETWEEN '${startDateString}' AND '${endDateString}'
        LEFT JOIN auto_contact_person ON auto_contact_person.user_id_c = opportunities.assigned_user_id
        AND auto_contact_person.deleted = 0
        LEFT JOIN auto_contac_auto_dealer_c ON auto_contac_auto_dealer_c.auto_contaff8f_person_idb = auto_contact_person.id
        AND auto_contac_auto_dealer_c.deleted = 0
    WHERE
        auto_dealer.integralink_code = '${dealerIntegralinkCode}'
        AND opportunities.date_entered BETWEEN '${startDateString}' AND '${endDateString}'
        AND (
            auto_contact_person.id IS NULL
            OR (
                auto_contact_person.id IS NOT NULL
                AND auto_contac_auto_dealer_c.auto_contafb84_dealer_ida = auto_dealer.id
            )
        )
    GROUP BY
        auto_campaign.name
    ORDER BY
        auto_campaign.name;`;
}

// Total Opportunities Texted and Total Opportunities Called
export const getOpportunitiesTextedCalledQuery = (dealerIntegralinkCode: string|number, startDate: Date, endDate: Date): string => {
  const startDateString: string = toUTCDateTimeString(startDate);
  const endDateString: string = toUTCDateTimeString(endDate);

  return `SELECT
        auto_campaign.name AS autoCampaignName,
        SUM(IF(tasks.name = 'Text', 1, 0)) AS totalOpportunitiesTexted,
        SUM(IF(tasks.name = 'Call', 1, 0)) AS totalOpportunitiesCalled
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
        auto_vehiclpportunities_c ON auto_vehiclpportunities_c.auto_vehicce49vehicle_ida = auto_vehicle.id
            AND auto_vehiclpportunities_c.deleted = 0
            INNER JOIN
        opportunities ON opportunities.id = auto_vehiclpportunities_c.auto_vehicb672unities_idb
            AND opportunities.deleted = 0
            INNER JOIN
                auto_campaipportunities_c ON auto_campaipportunities_c.auto_campae5baunities_idb = opportunities.id
                AND auto_campaipportunities_c.deleted = 0
            INNER JOIN
                auto_campaign ON auto_campaipportunities_c.auto_campa1b75ampaign_ida = auto_campaign.id
                AND auto_campaign.deleted = 0
            INNER JOIN
        tasks ON tasks.parent_id = opportunities.id
            AND tasks.parent_type = 'Opportunities'
            LEFT JOIN
        auto_contact_person ON auto_contact_person.user_id_c = opportunities.assigned_user_id
            AND auto_contact_person.deleted = 0
            LEFT JOIN
        auto_contac_auto_dealer_c ON auto_contac_auto_dealer_c.auto_contaff8f_person_idb = auto_contact_person.id
            AND auto_contac_auto_dealer_c.deleted = 0
    WHERE
        auto_dealer.integralink_code = '${dealerIntegralinkCode}'
            AND opportunities.date_entered BETWEEN '${startDateString}' AND '${endDateString}'
            AND (auto_contact_person.id IS NULL
            OR (auto_contact_person.id IS NOT NULL
            AND auto_contac_auto_dealer_c.auto_contafb84_dealer_ida = auto_dealer.id))
    GROUP BY auto_campaign.name;`;
}

// Total Appts
export const getAppointmentsQuery = (dealerIntegralinkCode: string|number, startDate: Date, endDate: Date): string => {
  const startDateString: string = toUTCDateTimeString(startDate);
  const endDateString: string = toUTCDateTimeString(endDate);

  return `SELECT
        auto_campaign.name AS autoCampaignName,
        COUNT(opportunities.last_contacted_date) AS totalAppointments,
        SUM(IF(auto_appointment.appointment_with_ade_ro = 1 OR auto_appointment.appointment_with_ro = 1, 1, 0)) AS totalAppointmentsArrived
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
        auto_vehiclpportunities_c ON auto_vehiclpportunities_c.auto_vehicce49vehicle_ida = auto_vehicle.id
            AND auto_vehiclpportunities_c.deleted = 0
            INNER JOIN
        opportunities ON opportunities.id = auto_vehiclpportunities_c.auto_vehicb672unities_idb
            AND opportunities.deleted = 0
            INNER JOIN
                auto_campaipportunities_c ON auto_campaipportunities_c.auto_campae5baunities_idb = opportunities.id
                AND auto_campaipportunities_c.deleted = 0
            INNER JOIN
                auto_campaign ON auto_campaipportunities_c.auto_campa1b75ampaign_ida = auto_campaign.id
                AND auto_campaign.deleted = 0
            INNER JOIN
        auto_contact_person ON auto_contact_person.user_id_c = opportunities.assigned_user_id
            AND auto_contact_person.deleted = 0
            INNER JOIN
        auto_contac_auto_dealer_c ON auto_contac_auto_dealer_c.auto_contaff8f_person_idb = auto_contact_person.id
            AND auto_contac_auto_dealer_c.deleted = 0
            INNER JOIN
        auto_appointment ON opportunities.id = auto_appointment.opportunity_id_c
            AND auto_appointment.deleted = 0
    WHERE
        auto_dealer.integralink_code = '${dealerIntegralinkCode}'
            AND opportunities.last_contacted_date IS NOT NULL
            AND  (auto_appointment.appointment_date BETWEEN '${startDateString}' AND '${endDateString}')
            AND auto_contac_auto_dealer_c.auto_contafb84_dealer_ida = auto_dealer.id
    GROUP BY auto_campaign.name;`;
}

// Total Repair Orders  and Revenue
export const getRepairOrderRevenueQuery = (dealerIntegralinkCode: string|number, startDate: Date, endDate: Date): string => {
  const startDateString: string = toUTCDateTimeString(startDate);
  const endDateString: string = toUTCDateTimeString(endDate);

  return `SELECT DISTINCT
        auto_campaign.name AS autoCampaignName,
        COUNT(auto_repair_order.id) AS roNo,
        SUM(REPLACE(repair_order_amount_total, ',', '')) AS roAmount
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
            INNER JOIN
        opportunities ON auto_repair_order.opportunity_id_c = opportunities.id
            AND opportunities.deleted = 0
            INNER JOIN
                auto_campaipportunities_c ON auto_campaipportunities_c.auto_campae5baunities_idb = opportunities.id
                AND auto_campaipportunities_c.deleted = 0
            INNER JOIN
                auto_campaign ON auto_campaipportunities_c.auto_campa1b75ampaign_ida = auto_campaign.id
                AND auto_campaign.deleted = 0
            INNER JOIN
        auto_contact_person ON auto_contact_person.user_id_c = opportunities.assigned_user_id
            AND auto_contact_person.deleted = 0
            INNER JOIN
        auto_contac_auto_dealer_c ON auto_contac_auto_dealer_c.auto_contaff8f_person_idb = auto_contact_person.id
            AND auto_contac_auto_dealer_c.deleted = 0
    WHERE
        auto_dealer.integralink_code = '${dealerIntegralinkCode}'
            AND auto_repair_order.service_closed_date BETWEEN '${startDateString}' AND '${endDateString}'
            AND opportunities.last_contacted_date IS NOT NULL
            AND auto_contac_auto_dealer_c.auto_contafb84_dealer_ida = auto_dealer.id
    GROUP BY auto_campaign.name;`;
}

/*
 * Campaign ROI report
 */

// Campaign Type. # of Text Messages
export const CAMPAIGN_TYPE_NO_TEXTS_QUERY = `SELECT
      auto_campaign.id as campaignId,
      auto_campaign.name AS campaignName,
      auto_campaign.type AS campaignType,
      COUNT(DISTINCT (IF(auto_event.body_type = 'Text', auto_event.id, NULL))) AS textMessageNo,
      COUNT(DISTINCT (IF(auto_event.type = 'Sent', auto_event.id, NULL))) AS emailNo,
      COUNT(DISTINCT auto_repair_order.id) AS roNo,
      SUM(replace(repair_order_amount_total,',','')) AS roTotal
  FROM
      auto_event
          INNER JOIN
      auto_event_to_recipient_c ON auto_event.id = auto_event_to_recipient_c.auto_eventfa83o_event_idb
          AND auto_event_to_recipient_c.deleted = 0
          INNER JOIN
      auto_recipient ON auto_event_to_recipient_c.auto_eventa735cipient_ida = auto_recipient.id
          AND auto_recipient.deleted = 0
          INNER JOIN
      auto_recipiuto_campaign_c ON auto_recipiuto_campaign_c.auto_recip885bcipient_idb = auto_recipient.id
          AND auto_recipiuto_campaign_c.deleted = 0
          INNER JOIN
      auto_campaign ON auto_campaign.id = auto_recipiuto_campaign_c.auto_recip8ba3ampaign_ida
          AND auto_campaign.deleted = 0
          INNER JOIN
      auto_campai_auto_dealer_c ON auto_campai_auto_dealer_c.auto_campa2d6bampaign_idb = auto_campaign.id
          AND auto_campai_auto_dealer_c.deleted = 0
          INNER JOIN
      auto_dealer ON auto_campai_auto_dealer_c.auto_campa1fd9_dealer_ida = auto_dealer.id
          LEFT JOIN
      auto_repair_order FORCE INDEX FOR JOIN (\`idx_auto_repair_order_auto_event_id_c\`) ON auto_repair_order.auto_event_id_c = auto_event.id
          AND auto_repair_order.deleted = 0
  WHERE
      auto_dealer.integralink_code = '99999'
      AND auto_campaign.included_in_roi = 1
      AND auto_event.sent_date BETWEEN '2021-12-31 18:00:00' AND '2022-01-31 17:59:59'
      AND (auto_event.type = 'Sent'
          OR (auto_event.body_type = 'Text'
          AND auto_event.generated_from = 'System'
          AND auto_event.type = 'Not-Pending'))
  GROUP BY auto_campaign.id
  ORDER BY auto_campaign.name;`;

// Arrived appointments
export const ARRIVED_APPOINTMENTS_QUERY = `SELECT
      auto_campaign.id AS campaignId,
          COUNT(IF(opportunities.last_contacted_date < auto_appointment.date_entered,
              auto_appointment.date_entered,
              auto_appointment.reschedule_date)) AS appointmentNo,
          SUM(IF(auto_appointment.appointment_with_ade_ro = 1 OR auto_appointment.appointment_with_ro = 1, 1, 0)) AS arrivedAppointmentNo
      FROM
          auto_campaign
          INNER JOIN
      auto_recipiuto_campaign_c ON auto_campaign.id = auto_recipiuto_campaign_c.auto_recip8ba3ampaign_ida
          AND auto_recipiuto_campaign_c.deleted = 0
          INNER JOIN
      auto_recipient ON auto_recipiuto_campaign_c.auto_recip885bcipient_idb = auto_recipient.id
          AND auto_recipient.deleted = 0
          INNER JOIN
      auto_vehicle ON auto_recipient.auto_vehicle_id_c = auto_vehicle.id
          AND auto_vehicle.deleted = 0
          INNER JOIN
      auto_vehiclpportunities_c ON auto_vehiclpportunities_c.auto_vehicce49vehicle_ida = auto_vehicle.id
          AND auto_vehiclpportunities_c.deleted = 0
          INNER JOIN
      opportunities ON opportunities.id = auto_vehiclpportunities_c.auto_vehicb672unities_idb
          AND opportunities.deleted = 0
          INNER JOIN
      auto_appointment ON auto_appointment.opportunity_id_c = opportunities.id
          AND auto_appointment.deleted = 0
  WHERE
      auto_campaign.id IN ('6bf41f2a-e514-7239-9c83-60902c84138c' , '75ee8ab2-0d51-2a36-ec06-60a0ab4997c2' , '39af4775-636f-8d49-bd14-60902c74e266' , 'a77d3bb2-dbd3-782c-cc79-609b6558eae6' , 'e794014e-6fa9-ab9d-67e8-60902ccaaa7a' , 'c669dd79-3f54-bf1d-af3f-609b654cd4ee' , '13960511-3327-9ad9-fdb7-60928eb50fa6' , '258e9fa8-5ff5-a92a-b68e-60b9b88844f5' , 'de940f16-e99f-a8a6-aa20-60902d5239e6' , 'ab88002a-8692-8973-7ee1-609b6551eefd' , 'ceff0c88-5bed-52d5-396f-60902d395f42' , '4125761d-d7e2-5333-a008-609b652d0f94' , '82bc7435-66af-1198-8aa5-60902b950989' , '5f3dc72f-e07a-5209-d93d-609b6559b7e9' , '60667c57-1395-edb1-c435-60902ae41632' , '5f26b352-d971-aa66-78ce-609f5a04cefe' , '33f4c2ba-ab81-ea2d-b0e0-609025aaebbf' , '48270fe7-2e91-d7cb-1e33-609e085ce07c' , 'b2679193-bc6e-6b97-38f2-6090251b124e' , 'cc37e464-62e1-78af-5c3c-60902bf246f9' , '7bfac984-e36a-8472-0fb5-609b65310ec6' , '14b7d84b-09a0-74ec-63eb-60902a4ec96c' , '94790875-08ac-48ef-fd09-609b654e6881' , 'cd30aa8e-9c41-4461-caa3-609b6541dd0c')
          AND opportunities.last_contacted_date IS NOT NULL
          AND ((IF(opportunities.last_contacted_date < auto_appointment.date_entered,
          auto_appointment.date_entered,
          auto_appointment.reschedule_date) >= '2021-01-01 00:00:00'
          AND IF(opportunities.last_contacted_date < auto_appointment.date_entered,
          auto_appointment.date_entered,
          auto_appointment.reschedule_date) <= '2022-01-31 23:59:59')
          OR (auto_appointment.appointment_date BETWEEN '2021-01-01 00:00:00' AND '2022-01-31 23:59:59'))
  GROUP BY auto_campaign.id;`;
