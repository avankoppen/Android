DROP VIEW APPS.XXARG_PO_LINE_LOC_XML;

/* Formatted on 2010/09/06 12:03 (Formatter Plus v4.8.8) */
CREATE OR REPLACE FORCE VIEW apps.xxarg_po_line_loc_xml (shipment_num,
                                                         due_date,
                                                         quantity,
                                                         price_override,
                                                         quantity_cancelled,
                                                         cancel_flag,
                                                         cancel_date,
                                                         cancel_reason,
                                                         taxable_flag,
                                                         start_date,
                                                         end_date,
                                                         attribute_category,
                                                         attribute1,
                                                         attribute2,
                                                         attribute3,
                                                         attribute4,
                                                         attribute5,
                                                         attribute6,
                                                         attribute7,
                                                         attribute8,
                                                         attribute9,
                                                         attribute10,
                                                         attribute11,
                                                         attribute12,
                                                         attribute13,
                                                         attribute14,
                                                         attribute15,
                                                         po_header_id,
                                                         po_line_id,
                                                         line_location_id,
                                                         shipment_type,
                                                         po_release_id,
                                                         consigned_flag,
                                                         ussgl_transaction_code,
                                                         government_context,
                                                         receiving_routing_id,
                                                         accrue_on_receipt_flag,
                                                         closed_reason,
                                                         closed_date,
                                                         closed_by,
                                                         org_id,
                                                         unit_of_measure_class,
                                                         encumber_now,
                                                         inspection_required_flag,
                                                         receipt_required_flag,
                                                         qty_rcv_tolerance,
                                                         qty_rcv_exception_code,
                                                         enforce_ship_to_location_code,
                                                         allow_substitute_receipts_flag,
                                                         days_early_receipt_allowed,
                                                         days_late_receipt_allowed,
                                                         receipt_days_exception_code,
                                                         invoice_close_tolerance,
                                                         receive_close_tolerance,
                                                         ship_to_organization_id,
                                                         source_shipment_id,
                                                         closed_code,
                                                         request_id,
                                                         program_application_id,
                                                         program_id,
                                                         program_update_date,
                                                         last_accept_date,
                                                         encumbered_flag,
                                                         encumbered_date,
                                                         unencumbered_quantity,
                                                         fob_lookup_code,
                                                         freight_terms_lookup_code,
                                                         estimated_tax_amount,
                                                         from_header_id,
                                                         from_line_id,
                                                         from_line_location_id,
                                                         lead_time,
                                                         lead_time_unit,
                                                         price_discount,
                                                         terms_id,
                                                         approved_flag,
                                                         approved_date,
                                                         closed_flag,
                                                         cancelled_by,
                                                         firm_status_lookup_code,
                                                         firm_date,
                                                         last_update_date,
                                                         last_updated_by,
                                                         last_update_login,
                                                         creation_date,
                                                         created_by,
                                                         quantity_received,
                                                         quantity_accepted,
                                                         quantity_rejected,
                                                         quantity_billed,
                                                         unit_meas_lookup_code,
                                                         ship_via_lookup_code,
                                                         global_attribute_category,
                                                         global_attribute1,
                                                         global_attribute2,
                                                         global_attribute3,
                                                         global_attribute4,
                                                         global_attribute5,
                                                         global_attribute6,
                                                         global_attribute7,
                                                         global_attribute8,
                                                         global_attribute9,
                                                         global_attribute10,
                                                         global_attribute11,
                                                         global_attribute12,
                                                         global_attribute13,
                                                         global_attribute14,
                                                         global_attribute15,
                                                         global_attribute16,
                                                         global_attribute17,
                                                         global_attribute18,
                                                         global_attribute19,
                                                         global_attribute20,
                                                         quantity_shipped,
                                                         country_of_origin_code,
                                                         tax_user_override_flag,
                                                         match_option,
                                                         tax_code_id,
                                                         calculate_tax_flag,
                                                         change_promised_date_reason,
                                                         note_to_receiver,
                                                         secondary_unit_of_measure,
                                                         secondary_quantity,
                                                         preferred_grade,
                                                         secondary_quantity_received,
                                                         secondary_quantity_accepted,
                                                         secondary_quantity_rejected,
                                                         secondary_quantity_cancelled,
                                                         vmi_flag,
                                                         retroactive_date,
                                                         supplier_order_line_number,
                                                         amount,
                                                         amount_received,
                                                         amount_billed,
                                                         amount_cancelled,
                                                         amount_accepted,
                                                         amount_rejected,
                                                         drop_ship_flag,
                                                         sales_order_update_date,
                                                         ship_to_location_id,
                                                         ship_to_location_name,
                                                         ship_to_location_desc,
                                                         ship_to_address_line1,
                                                         ship_to_address_line2,
                                                         ship_to_address_line3,
                                                         ship_to_address_line4,
                                                         ship_to_address_info,
                                                         ship_to_country,
                                                         details,
                                                         ship_cont_phone,
                                                         ship_cont_email,
                                                         ultimate_deliver_cont_phone,
                                                         ultimate_deliver_cont_email,
                                                         ship_cont_name,
                                                         ultimate_deliver_cont_name,
                                                         ship_cust_name,
                                                         ship_cust_location,
                                                         ultimate_deliver_cust_name,
                                                         ultimate_deliver_cust_location,
                                                         ship_to_contact_fax,
                                                         ultimate_deliver_to_cont_name,
                                                         ultimate_deliver_to_cont_fax,
                                                         shipping_method,
                                                         shipping_instructions,
                                                         packing_instructions,
                                                         customer_product_desc,
                                                         customer_po_num,
                                                         customer_po_line_num,
                                                         customer_po_shipment_num,
                                                         need_by_date,
                                                         promised_date,
                                                         total_shipment_amount,
                                                         final_match_flag,
                                                         manual_price_change_flag,
                                                         tax_name,
                                                         transaction_flow_header_id,
                                                         nonrecoverable_tax_flag  -- PWINGER added 09/06/2010 for Sabrix tax engine  
                                                        )
AS
   SELECT pll.shipment_num,
          TO_CHAR (NVL (pll.need_by_date, pll.promised_date),
                   'DD-MON-YYYY HH24:MI:SS'
                  ) due_date,
          pll.quantity, pll.price_override price_override,
          pll.quantity_cancelled, pll.cancel_flag,
          TO_CHAR (pll.cancel_date, 'DD-MON-YYYY HH24:MI:SS') cancel_date,
          pll.cancel_reason, pll.taxable_flag,
          TO_CHAR (pll.start_date, 'DD-MON-YYYY HH24:MI:SS') start_date,
          TO_CHAR (pll.end_date, 'DD-MON-YYYY HH24:MI:SS') end_date,
          pll.attribute_category, pll.attribute1, pll.attribute2,
          pll.attribute3, pll.attribute4, pll.attribute5, pll.attribute6,
          pll.attribute7, pll.attribute8, pll.attribute9, pll.attribute10,
          pll.attribute11, pll.attribute12, pll.attribute13, pll.attribute14,
          pll.attribute15, pll.po_header_id, pl.po_line_id,
          pll.line_location_id,
          DECODE (NVL (pll.shipment_type, 'PRICE BREAK'),
                  'PRICE BREAK', 'BLANKET',
                  'SCHEDULED', 'RELEASE',
                  'BLANKET', 'RELEASE',
                  'STANDARD', 'STANDARD',
                  'PLANNED', 'PLANNED'
                 ) shipment_type,
          pll.po_release_id, pll.consigned_flag, pll.ussgl_transaction_code,
          pll.government_context, pll.receiving_routing_id,
          pll.accrue_on_receipt_flag, pll.closed_reason,
          TO_CHAR (pll.closed_date, 'DD-MON-YYYY HH24:MI:SS') closed_date,
          pll.closed_by, pll.org_id, pll.unit_of_measure_class,
          pll.encumber_now, pll.inspection_required_flag,
          pll.receipt_required_flag, pll.qty_rcv_tolerance,
          pll.qty_rcv_exception_code, pll.enforce_ship_to_location_code,
          pll.allow_substitute_receipts_flag, pll.days_early_receipt_allowed,
          pll.days_late_receipt_allowed, pll.receipt_days_exception_code,
          pll.invoice_close_tolerance, pll.receive_close_tolerance,
          pll.ship_to_organization_id, pll.source_shipment_id,
          pll.closed_code, pll.request_id, pll.program_application_id,
          pll.program_id, pll.program_update_date,
          TO_CHAR (pll.last_accept_date,
                   'DD-MON-YYYY HH24:MI:SS'
                  ) last_accept_date,
          pll.encumbered_flag,
          TO_CHAR (pll.encumbered_date,
                   'DD-MON-YYYY HH24:MI:SS'
                  ) encumbered_date,
          pll.unencumbered_quantity, pll.fob_lookup_code,
          pll.freight_terms_lookup_code,
          TO_CHAR (pll.estimated_tax_amount,
                   pgt.format_mask
                  ) estimated_tax_amount,
          pll.from_header_id, pll.from_line_id, pll.from_line_location_id,
          pll.lead_time, pll.lead_time_unit, pll.price_discount, pll.terms_id,
          pll.approved_flag,
          TO_CHAR (pll.approved_date, 'DD-MON-YYYY HH24:MI:SS') approved_date,
          pll.closed_flag, pll.cancelled_by, pll.firm_status_lookup_code,
          TO_CHAR (pll.firm_date, 'DD-MON-YYYY HH24:MI:SS') firm_date,
          TO_CHAR (pll.last_update_date,
                   'DD-MON-YYYY HH24:MI:SS'
                  ) last_update_date,
          pll.last_updated_by, pll.last_update_login,
          TO_CHAR (pll.creation_date, 'DD-MON-YYYY HH24:MI:SS') creation_date,
          pll.created_by, pll.quantity_received, pll.quantity_accepted,
          pll.quantity_rejected, pll.quantity_billed,
          pll.unit_meas_lookup_code, pll.ship_via_lookup_code,
          pll.global_attribute_category, pll.global_attribute1,
          pll.global_attribute2, pll.global_attribute3, pll.global_attribute4,
          pll.global_attribute5, pll.global_attribute6, pll.global_attribute7,
          pll.global_attribute8, pll.global_attribute9,
          pll.global_attribute10, pll.global_attribute11,
          pll.global_attribute12, pll.global_attribute13,
          pll.global_attribute14, pll.global_attribute15,
          pll.global_attribute16, pll.global_attribute17,
          pll.global_attribute18, pll.global_attribute19,
          pll.global_attribute20, pll.quantity_shipped,
          pll.country_of_origin_code, pll.tax_user_override_flag,
          pll.match_option, pll.tax_code_id, pll.calculate_tax_flag,
          pll.change_promised_date_reason, pll.note_to_receiver,
          pll.secondary_unit_of_measure, pll.secondary_quantity,
          pll.preferred_grade, pll.secondary_quantity_received,
          pll.secondary_quantity_accepted, pll.secondary_quantity_rejected,
          pll.secondary_quantity_cancelled, pll.vmi_flag,
          TO_CHAR (pll.retroactive_date,
                   'DD-MON-YYYY HH24:MI:SS'
                  ) retroactive_date,
          pll.supplier_order_line_number,
          TO_CHAR (po_core_s.get_total ('S', pll.line_location_id),
                   pgt.format_mask
                  ) amount,
          TO_CHAR (pll.amount_received, pgt.format_mask) amount_received,
          TO_CHAR (pll.amount_billed, pgt.format_mask) amount_billed,
          TO_CHAR (pll.amount_cancelled, pgt.format_mask) amount_cancelled,
          TO_CHAR (pll.amount_accepted, pgt.format_mask) amount_accepted,
          TO_CHAR (pll.amount_rejected, pgt.format_mask) amount_rejected,
          pll.drop_ship_flag,
          TO_CHAR (pll.sales_order_update_date,
                   'DD-MON-YYYY HH24:MI:SS'
                  ) sales_order_update_date,
          DECODE
             (NVL (pll.ship_to_location_id, -1),
              -1, NULL,
              po_communication_pvt.getlocationinfo (pll.ship_to_location_id)
             ) ship_to_location_id,
          DECODE
               (NVL (pll.ship_to_location_id, -1),
                -1, NULL,
                po_communication_pvt.getlocationname ()
               ) ship_to_location_name,
          xxarg_ship_to_loca_desc
             (po_communication_pvt.getlocationinfo (pll.ship_to_location_id)
             ) loc_description,
          DECODE
               (NVL (pll.ship_to_location_id, -1),
                -1, NULL,
                po_communication_pvt.getaddressline1 ()
               ) ship_to_address_line1,
          DECODE
               (NVL (pll.ship_to_location_id, -1),
                -1, NULL,
                po_communication_pvt.getaddressline2 ()
               ) ship_to_address_line2,
          DECODE
               (NVL (pll.ship_to_location_id, -1),
                -1, NULL,
                po_communication_pvt.getaddressline3 ()
               ) ship_to_address_line3,
          DECODE
               (NVL (pll.ship_to_location_id, -1),
                -1, NULL,
                po_communication_pvt.getaddressline4 ()
               ) ship_to_address_line4,
          DECODE (NVL (pll.ship_to_location_id, -1),
                  -1, NULL,
                  po_communication_pvt.getaddressinfo ()
                 ) ship_to_address_info,
          DECODE
               (NVL (pll.ship_to_location_id, -1),
                -1, NULL,
                po_communication_pvt.getterritoryshortname ()
               ) ship_to_country,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.get_drop_ship_details
                                                         (pll.line_location_id),
              NULL
             ) details,
          DECODE (pll.drop_ship_flag,
                  'Y', po_communication_pvt.getshipcontphone (),
                  NULL
                 ) ship_cont_phone,
          DECODE (pll.drop_ship_flag,
                  'Y', po_communication_pvt.getshipcontemail (),
                  NULL
                 ) ship_cont_email,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getdelivercontphone (),
              NULL
             ) ultimate_deliver_cont_phone,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getdelivercontemail (),
              NULL
             ) ultimate_deliver_cont_email,
          DECODE (pll.drop_ship_flag,
                  'Y', po_communication_pvt.getshipcontname (),
                  NULL
                 ) ship_cont_name,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getdelivercontname (),
              NULL
             ) ultimate_deliver_cont_name,
          DECODE (pll.drop_ship_flag,
                  'Y', po_communication_pvt.getshipcustname (),
                  NULL
                 ) ship_cust_name,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getshipcustlocation (),
              NULL
             ) ship_cust_location,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getdelivercustname (),
              NULL
             ) ultimate_deliver_cust_name,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getdelivercustlocation (),
              NULL
             ) ultimate_deliver_cust_location,
          DECODE
              (pll.drop_ship_flag,
               'Y', po_communication_pvt.getshipcontactfax (),
               NULL
              ) ship_to_contact_fax,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getdelivercontactname (),
              NULL
             ) ultimate_deliver_to_cont_name,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getdelivercontactfax (),
              NULL
             ) ultimate_deliver_to_cont_fax,
          DECODE (pll.drop_ship_flag,
                  'Y', po_communication_pvt.getshippingmethod (),
                  NULL
                 ) shipping_method,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getshippinginstructions (),
              NULL
             ) shipping_instructions,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getpackinginstructions (),
              NULL
             ) packing_instructions,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getcustomerproductdesc (),
              NULL
             ) customer_product_desc,
          DECODE
                (pll.drop_ship_flag,
                 'Y', po_communication_pvt.getcustomerponumber (),
                 NULL
                ) customer_po_num,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getcustomerpolinenum (),
              NULL
             ) customer_po_line_num,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getcustomerposhipmentnum (),
              NULL
             ) customer_po_shipment_num,
          TO_CHAR (pll.need_by_date, 'DD-MON-YYYY HH24:MI:SS') need_by_date,
          TO_CHAR (pll.promised_date, 'DD-MON-YYYY HH24:MI:SS') promised_date,
          TO_CHAR (pll.amount, pgt.format_mask) total_shipment_amount,
          pll.final_match_flag, pll.manual_price_change_flag, pll.tax_name,
          pll.transaction_flow_header_id,
          -- PWINGER added 09/06/2010 for Sabrix tax engine 
          decode((select sum(NVL(pdist.nonrecoverable_tax,0)) from po.po_distributions_all pdist
             where pdist.po_line_id = pl.po_line_id)
             ,0,'N','Y') nonrecoverable_tax_flag
     FROM po_line_locations_all pll, po_lines_all pl, po_communication_gt pgt
    WHERE pll.po_line_id(+) = pl.po_line_id
      AND (pll.shipment_type(+) <> 'BLANKET' AND pgt.po_release_id IS NULL)
   UNION
   SELECT pll.shipment_num,
          TO_CHAR (NVL (pll.need_by_date, pll.promised_date),
                   'DD-MON-YYYY HH24:MI:SS'
                  ) due_date,
          pll.quantity, pll.price_override price_override,
          pll.quantity_cancelled, pll.cancel_flag,
          TO_CHAR (pll.cancel_date, 'DD-MON-YYYY HH24:MI:SS') cancel_date,
          pll.cancel_reason, pll.taxable_flag,
          TO_CHAR (pll.start_date, 'DD-MON-YYYY HH24:MI:SS') start_date,
          TO_CHAR (pll.end_date, 'DD-MON-YYYY HH24:MI:SS') end_date,
          pll.attribute_category, pll.attribute1, pll.attribute2,
          pll.attribute3, pll.attribute4, pll.attribute5, pll.attribute6,
          pll.attribute7, pll.attribute8, pll.attribute9, pll.attribute10,
          pll.attribute11, pll.attribute12, pll.attribute13, pll.attribute14,
          pll.attribute15, pll.po_header_id, pl.po_line_id,
          pll.line_location_id,
          DECODE (pll.shipment_type,
                  'PRICE BREAK', 'BLANKET',
                  'SCHEDULED', 'RELEASE',
                  'BLANKET', 'RELEASE',
                  'STANDARD', 'STANDARD',
                  'PLANNED', 'PLANNED'
                 ) shipment_type,
          pll.po_release_id, pll.consigned_flag, pll.ussgl_transaction_code,
          pll.government_context, pll.receiving_routing_id,
          pll.accrue_on_receipt_flag, pll.closed_reason,
          TO_CHAR (pll.closed_date, 'DD-MON-YYYY HH24:MI:SS') closed_date,
          pll.closed_by, pll.org_id, pll.unit_of_measure_class,
          pll.encumber_now, pll.inspection_required_flag,
          pll.receipt_required_flag, pll.qty_rcv_tolerance,
          pll.qty_rcv_exception_code, pll.enforce_ship_to_location_code,
          pll.allow_substitute_receipts_flag, pll.days_early_receipt_allowed,
          pll.days_late_receipt_allowed, pll.receipt_days_exception_code,
          pll.invoice_close_tolerance, pll.receive_close_tolerance,
          pll.ship_to_organization_id, pll.source_shipment_id,
          pll.closed_code, pll.request_id, pll.program_application_id,
          pll.program_id, pll.program_update_date,
          TO_CHAR (pll.last_accept_date,
                   'DD-MON-YYYY HH24:MI:SS'
                  ) last_accept_date,
          pll.encumbered_flag,
          TO_CHAR (pll.encumbered_date,
                   'DD-MON-YYYY HH24:MI:SS'
                  ) encumbered_date,
          pll.unencumbered_quantity, pll.fob_lookup_code,
          pll.freight_terms_lookup_code,
          TO_CHAR (pll.estimated_tax_amount,
                   pgt.format_mask
                  ) estimated_tax_amount,
          pll.from_header_id, pll.from_line_id, pll.from_line_location_id,
          pll.lead_time, pll.lead_time_unit, pll.price_discount, pll.terms_id,
          pll.approved_flag,
          TO_CHAR (pll.approved_date, 'DD-MON-YYYY HH24:MI:SS') approved_date,
          pll.closed_flag, pll.cancelled_by, pll.firm_status_lookup_code,
          TO_CHAR (pll.firm_date, 'DD-MON-YYYY HH24:MI:SS') firm_date,
          TO_CHAR (pll.last_update_date,
                   'DD-MON-YYYY HH24:MI:SS'
                  ) last_update_date,
          pll.last_updated_by, pll.last_update_login,
          TO_CHAR (pll.creation_date, 'DD-MON-YYYY HH24:MI:SS') creation_date,
          pll.created_by, pll.quantity_received, pll.quantity_accepted,
          pll.quantity_rejected, pll.quantity_billed,
          pll.unit_meas_lookup_code, pll.ship_via_lookup_code,
          pll.global_attribute_category, pll.global_attribute1,
          pll.global_attribute2, pll.global_attribute3, pll.global_attribute4,
          pll.global_attribute5, pll.global_attribute6, pll.global_attribute7,
          pll.global_attribute8, pll.global_attribute9,
          pll.global_attribute10, pll.global_attribute11,
          pll.global_attribute12, pll.global_attribute13,
          pll.global_attribute14, pll.global_attribute15,
          pll.global_attribute16, pll.global_attribute17,
          pll.global_attribute18, pll.global_attribute19,
          pll.global_attribute20, pll.quantity_shipped,
          pll.country_of_origin_code, pll.tax_user_override_flag,
          pll.match_option, pll.tax_code_id, pll.calculate_tax_flag,
          pll.change_promised_date_reason, pll.note_to_receiver,
          pll.secondary_unit_of_measure, pll.secondary_quantity,
          pll.preferred_grade, pll.secondary_quantity_received,
          pll.secondary_quantity_accepted, pll.secondary_quantity_rejected,
          pll.secondary_quantity_cancelled, pll.vmi_flag,
          TO_CHAR (pll.retroactive_date,
                   'DD-MON-YYYY HH24:MI:SS'
                  ) retroactive_date,
          pll.supplier_order_line_number,
          TO_CHAR (po_core_s.get_total ('S', pll.line_location_id),
                   pgt.format_mask
                  ) amount,
          TO_CHAR (pll.amount_received, pgt.format_mask) amount_received,
          TO_CHAR (pll.amount_billed, pgt.format_mask) amount_billed,
          TO_CHAR (pll.amount_cancelled, pgt.format_mask) amount_cancelled,
          TO_CHAR (pll.amount_accepted, pgt.format_mask) amount_accepted,
          TO_CHAR (pll.amount_rejected, pgt.format_mask) amount_rejected,
          pll.drop_ship_flag,
          TO_CHAR (pll.sales_order_update_date,
                   'DD-MON-YYYY HH24:MI:SS'
                  ) sales_order_update_date,
          DECODE
             (NVL (pll.ship_to_location_id, -1),
              -1, NULL,
              po_communication_pvt.getlocationinfo (pll.ship_to_location_id)
             ) ship_to_location_id,
          DECODE
               (NVL (pll.ship_to_location_id, -1),
                -1, NULL,
                po_communication_pvt.getlocationname ()
               ) ship_to_location_name,
          xxarg_ship_to_loca_desc
             (po_communication_pvt.getlocationinfo (pll.ship_to_location_id)
             ) loc_description,
          DECODE
               (NVL (pll.ship_to_location_id, -1),
                -1, NULL,
                po_communication_pvt.getaddressline1 ()
               ) ship_to_address_line1,
          DECODE
               (NVL (pll.ship_to_location_id, -1),
                -1, NULL,
                po_communication_pvt.getaddressline2 ()
               ) ship_to_address_line2,
          DECODE
               (NVL (pll.ship_to_location_id, -1),
                -1, NULL,
                po_communication_pvt.getaddressline3 ()
               ) ship_to_address_line3,
          DECODE
               (NVL (pll.ship_to_location_id, -1),
                -1, NULL,
                po_communication_pvt.getaddressline4 ()
               ) ship_to_address_line4,
          DECODE (NVL (pll.ship_to_location_id, -1),
                  -1, NULL,
                  po_communication_pvt.getaddressinfo ()
                 ) ship_to_address_info,
          DECODE
               (NVL (pll.ship_to_location_id, -1),
                -1, NULL,
                po_communication_pvt.getterritoryshortname ()
               ) ship_to_country,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.get_drop_ship_details
                                                         (pll.line_location_id),
              NULL
             ) details,
          DECODE (pll.drop_ship_flag,
                  'Y', po_communication_pvt.getshipcontphone (),
                  NULL
                 ) ship_cont_phone,
          DECODE (pll.drop_ship_flag,
                  'Y', po_communication_pvt.getshipcontemail (),
                  NULL
                 ) ship_cont_email,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getdelivercontphone (),
              NULL
             ) ultimate_deliver_cont_phone,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getdelivercontemail (),
              NULL
             ) ultimate_deliver_cont_email,
          DECODE (pll.drop_ship_flag,
                  'Y', po_communication_pvt.getshipcontname (),
                  NULL
                 ) ship_cont_name,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getdelivercontname (),
              NULL
             ) ultimate_deliver_cont_name,
          DECODE (pll.drop_ship_flag,
                  'Y', po_communication_pvt.getshipcustname (),
                  NULL
                 ) ship_cust_name,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getshipcustlocation (),
              NULL
             ) ship_cust_location,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getdelivercustname (),
              NULL
             ) ultimate_deliver_cust_name,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getdelivercustlocation (),
              NULL
             ) ultimate_deliver_cust_location,
          DECODE
              (pll.drop_ship_flag,
               'Y', po_communication_pvt.getshipcontactfax (),
               NULL
              ) ship_to_contact_fax,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getdelivercontactname (),
              NULL
             ) ultimate_deliver_to_cont_name,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getdelivercontactfax (),
              NULL
             ) ultimate_deliver_to_cont_fax,
          DECODE (pll.drop_ship_flag,
                  'Y', po_communication_pvt.getshippingmethod (),
                  NULL
                 ) shipping_method,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getshippinginstructions (),
              NULL
             ) shipping_instructions,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getpackinginstructions (),
              NULL
             ) packing_instructions,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getcustomerproductdesc (),
              NULL
             ) customer_product_desc,
          DECODE
                (pll.drop_ship_flag,
                 'Y', po_communication_pvt.getcustomerponumber (),
                 NULL
                ) customer_po_num,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getcustomerpolinenum (),
              NULL
             ) customer_po_line_num,
          DECODE
             (pll.drop_ship_flag,
              'Y', po_communication_pvt.getcustomerposhipmentnum (),
              NULL
             ) customer_po_shipment_num,
          TO_CHAR (pll.need_by_date, 'DD-MON-YYYY HH24:MI:SS') need_by_date,
          TO_CHAR (pll.promised_date, 'DD-MON-YYYY HH24:MI:SS') promised_date,
          TO_CHAR (pll.amount, pgt.format_mask) total_shipment_amount,
          pll.final_match_flag, pll.manual_price_change_flag, pll.tax_name,
          pll.transaction_flow_header_id,
          -- PWINGER added 09/06/2010 for Sabrix tax engine 
          decode((select sum(NVL(pdist.nonrecoverable_tax,0)) from po.po_distributions_all pdist
             where pdist.po_line_id = pl.po_line_id)
             ,0,'N','Y') nonrecoverable_tax_flag
     FROM po_line_locations_all pll, po_lines_all pl, po_communication_gt pgt
    WHERE pll.po_line_id(+) = pl.po_line_id
      AND (pll.shipment_type(+) = 'BLANKET' AND pgt.po_release_id IS NOT NULL);


DROP SYNONYM XXWAGRO.XXARG_PO_LINE_LOC_XML;

CREATE SYNONYM XXWAGRO.XXARG_PO_LINE_LOC_XML FOR APPS.XXARG_PO_LINE_LOC_XML;


