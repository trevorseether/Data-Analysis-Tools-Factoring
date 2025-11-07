--CREATE OR REPLACE VIEW "fac_outst_fecha_desembolso_jmontoya" AS 
WITH
  codmes AS (
   SELECT DISTINCT DATE_FORMAT(FROM_unixtime((CAST(json_extract_scalar(_c1, '$.created_at["$date"]') AS BIGINT) / 1000)), '%Y%m') codmes
   FROM
     "prod_datalake_master"."prestamype__fac_auctions"
) 
, auctions AS (
   SELECT
     a.code
   , a.request_id
   , b.product product_type
   , (CASE WHEN (b.product = 'factoring') THEN b.user_third_party_name ELSE b.company_name END) client_name
   , (CASE WHEN (b.product = 'factoring') THEN b.user_third_party_ruc ELSE b.company_ruc END) client_ruc
   , (CASE WHEN (b.product = 'confirming') THEN b.user_third_party_name ELSE b.company_name END) provider_name
   , (CASE WHEN (b.product = 'confirming') THEN b.user_third_party_ruc ELSE b.company_ruc END) provider_ruc
   , (CASE WHEN (b.interest_proforma_disbursement_date IS NOT NULL) THEN DATE_ADD('hour', -5, b.interest_proforma_disbursement_date) WHEN (c.fecha_de_desembolso__factoring_ IS NOT NULL) THEN c.fecha_de_desembolso__factoring_ WHEN (d.fecha_de_desembolso_registrado IS NOT NULL) THEN d.fecha_de_desembolso_registrado ELSE DATE_ADD('hour', -5, b.proforma_disbursement_date) END) transfer_date
   , DATE_ADD('hour', -5, a.closed_at) ANTERIOR_TRANSFER
   , a.currency currency_auctions
   , b.currency currency_request
   , row_number() OVER (PARTITION BY (CASE WHEN (b.product = 'factoring') THEN b.user_third_party_ruc ELSE b.company_ruc END) ORDER BY a.closed_at ASC) flag_newclient
   , row_number() OVER (PARTITION BY (CASE WHEN (b.product = 'confirming') THEN b.user_third_party_ruc ELSE b.company_ruc END) ORDER BY a.closed_at ASC) flag_newprovider
   , (CASE WHEN (b.product = 'factoring') THEN a.proforma_start_financing_INTerest_rate ELSE a.proforma_start_simulation_financing_cost_rate END) assigned_financing_rate
   , b.invoice_net_amount total_net_amount_pending_payment
   , a.proforma_end_client_payment_date_expected e_payment_date
   , (CASE WHEN (b.product = 'factoring') THEN b.proforma_simulation_financing_total ELSE b.proforma_simulation_financing END) amount_financed
   , (CASE WHEN (b.interest_proforma_simulation_financing_cost_period IS NOT NULL) THEN b.interest_proforma_simulation_financing_cost_period ELSE b.proforma_simulation_financing_cost_period END) terms
   , (CASE WHEN (b.product = 'factoring') THEN b.proforma_simulation_financing_advance ELSE b.invoice_net_amount END) amount_advance
   , (CASE WHEN ((b.product = 'factoring') AND (b.invoice_net_amount > 0E0)) THEN (b.proforma_simulation_financing_advance / b.invoice_net_amount) WHEN ((b.product = 'confirming') AND (b.invoice_net_amount > 0E0)) THEN (b.invoice_net_amount / b.invoice_net_amount) ELSE null END) advance_percentage
   , b.invoice_nominal_amount amount_of_invoices
   , b.invoice_count invoice_count
   , b.assigned_name
   , b.assigned_last_name
   , b.company_id
   , b.user_third_party_id
   , (CASE WHEN (b.product = 'factoring') THEN b.user_third_party_id ELSE b.company_id END) client_id
   , (CASE WHEN (b.product = 'factoring') THEN b.company_id ELSE b.user_third_party_id END) provider_id
   FROM
     ((("prod_datalake_analytics"."fac_auctions" a
   INNER JOIN "prod_datalake_analytics"."fac_requests" b ON (a.request_id = b._id))
   LEFT JOIN (
      SELECT DISTINCT
        dealname
      , fecha_de_desembolso__factoring_
      FROM
        "prod_datalake_master"."hubspot__deal"
      WHERE ((pipeline = '14026011') AND (dealstage = '14026017'))
   )  c ON (c.dealname = b.code))
   LEFT JOIN (
      SELECT
        c_digo_de_subasta
      , MIN(fecha_de_desembolso_registrado) fecha_de_desembolso_registrado
      FROM
        "prod_datalake_master"."hubspot__pagos_facturas"
      WHERE (fecha_de_desembolso_registrado IS NOT NULL)
      GROUP BY c_digo_de_subasta
   )  d ON (d.c_digo_de_subasta = b.code))
   WHERE (((a.status = 'finished') AND (a.type_sale = 'auction')) OR ((a.status = 'confirmed') AND (a.type_sale = 'direct')))
) 
, client_payments_1 AS (
   SELECT
     *
   , (CASE WHEN ((currency <> distribution_provider_currency_distribution) AND (distribution_provider_currency_distribution = 'USD')) THEN 'pen2usd' WHEN ((currency <> distribution_provider_currency_distribution) AND (distribution_provider_currency_distribution = 'PEN')) THEN 'usd2pen' ELSE 'mantain' END) amount_paid_exchange_flag
   , (CASE WHEN ((distribution_provider_currency_amount_bussinesman <> distribution_provider_currency_distribution) AND (distribution_provider_currency_distribution = 'USD')) THEN 'pen2usd' WHEN ((distribution_provider_currency_amount_bussinesman <> distribution_provider_currency_distribution) AND (distribution_provider_currency_distribution = 'PEN')) THEN 'usd2pen' ELSE 'mantain' END) guarantee_exchange_flag
   , (CASE WHEN (COALESCE(distribution_provider_amount_bussinesman, 0) > 0) THEN distribution_provider_amount_bussinesman WHEN ((COALESCE(pay_order_businessman_amount, 0) > 0) AND (COALESCE(distribution_provider_igv, 0) > 0)) THEN 0 WHEN ((COALESCE(pay_order_businessman_amount, 0) > 0) AND (COALESCE(distribution_provider_igv, 0) = 0)) THEN pay_order_businessman_amount ELSE 0 END) guarantee_paid
   FROM
     "prod_datalake_analytics"."fac_client_payment_payments"
) 
, client_payments_2 AS (
   SELECT
     a.guarantee_exchange_flag
   , (CASE WHEN (cardinality(a.distribution) = 0) THEN 'new' ELSE 'old' END) flag
   , a.created_at
   , b.auction_code
   , b.status
   , a.payment_type
   , a.client_payment_id
   , (CASE WHEN (amount_paid_exchange_flag = 'usd2pen') THEN COALESCE(round((a.amount * a.distribution_provider_rate), 2), 0) WHEN (amount_paid_exchange_flag = 'pen2usd') THEN COALESCE(round((a.amount / a.distribution_provider_rate), 2), 0) ELSE a.amount END) original_amount_paid
   , a.distribution_provider_currency_distribution currency_distribution
   , a.amount client_amount_paid
   , (CASE WHEN ((COALESCE(pay_order_businessman_amount, 0) > 0) AND (COALESCE(distribution_provider_igv, 0) > 0)) THEN (COALESCE(a.distribution_provider_INTeres_payment, 0) + COALESCE(distribution_provider_igv, 0)) WHEN ((COALESCE(pay_order_businessman_amount, 0) = 0) AND (COALESCE(distribution_provider_igv, 0) > 0)) THEN (COALESCE(a.distribution_provider_INTeres_payment, 0) + COALESCE(distribution_provider_igv, 0)) WHEN ((COALESCE(a.distribution_provider_INTeres_payment, 0) = 0) AND (COALESCE(a.distribution_provider_amount_capital_payment, 0) = 0) AND COALESCE((guarantee_paid = 0))) THEN a.distribution_provider_INTeres ELSE COALESCE(a.distribution_provider_INTeres_payment, 0) END) interest_paid
   , (COALESCE(A.amount, 0) - ((COALESCE(A.distribution_provider_INTeres, 0) + COALESCE(a.diff_interest_proforma_interes_total_real, 0)) + COALESCE(a.guarantee_paid, 0))) capital_paid
   , (CASE WHEN (guarantee_exchange_flag = 'usd2pen') THEN COALESCE(round((a.guarantee_paid * a.distribution_provider_rate), 2), 0) WHEN (guarantee_exchange_flag = 'pen2usd') THEN COALESCE(round((a.guarantee_paid / a.distribution_provider_rate), 2), 0) ELSE COALESCE(guarantee_paid, 0) END) guarantee_paid
   , CAST(FROM_iso8601_timestamp(REPLACE(SUBSTRING(CAST(a.date AS VARCHAR), 1, 19), ' ', 'T')) AS DATE) payment_date
   , max(CAST(FROM_iso8601_timestamp(REPLACE(SUBSTRING(CAST(a.date AS VARCHAR), 1, 19), ' ', 'T')) AS DATE)) OVER (PARTITION BY b.auction_code) last_paid_date
   , CAST(DATE_FORMAT(date_parse(replace(CAST(a.date AS VARCHAR), '.000', ''), '%Y-%m-%d %H:%i:%s'), '%Y%m') AS INT) codmes
   , distribution_provider_amount_bussinesman
   , pay_order_businessman_amount
   , distribution_provider_igv
   , distribution_provider_new_capital
   FROM
     (client_payments_1 a
   LEFT JOIN "prod_datalake_analytics"."fac_client_payments" b ON (a.client_payment_id = b._id))
) 
, client_payments_3 AS (
   SELECT
     *
   , COALESCE(client_amount_paid, 0) total_amount_paid
   FROM
     client_payments_2
) 
, dataset AS (
   SELECT
     b.codmes
   , (CASE WHEN (current_date < date_add('day', -1, date_add('hour', 5, date_add('month', 1, date_parse(concat(b.codmes, '01'), '%Y%m%d'))))) THEN CAST(DATE_FORMAT(DATE_ADD('hour', -5, current_timestamp), '%Y-%m-%d 05:00:00.000') AS timestamp) ELSE date_add('day', -1, date_add('hour', 5, date_add('month', 1, date_parse(concat(b.codmes, '01'), '%Y%m%d')))) END) fecha_cierre
   , a.*
   FROM
     (auctions a
   LEFT JOIN codmes b ON true)
   WHERE (DATE_FORMAT(a.transfer_date, '%Y%m') <= b.codmes)
) 
, dataset_principal AS (
   SELECT
     a.code
   , date_add('day', -1, date_trunc('month', date_add('month', 1, DATE(a.fecha_cierre)))) fecha_cierre
   , a.codmes
   , a.product_type
   , max(a.client_ruc) client_ruc
   , max(a.client_name) client_name
   , max(a.provider_ruc) provider_ruc
   , max(a.provider_name) provider_name
   , max(a.flag_newclient) flag_newclient
   , max(a.flag_newprovider) flag_newprovider
   , max(a.transfer_date) transfer_date
   , max(a.ANTERIOR_TRANSFER) ANTERIOR_TRANSFER
   , max(a.currency_request) currency_request
   , max(a.currency_auctions) currency_auctions
   , max(a.assigned_financing_rate) assigned_financing_rate
   , max(a.total_net_amount_pending_payment) total_net_amount_pending_payment
   , max(a.e_payment_date) e_payment_date_original
   , max(a.amount_financed) amount_financed
   , max(a.terms) terms
   , max(a.amount_advance) amount_advance
   , max(a.advance_percentage) advance_percentage
   , max(a.invoice_count) invoice_count
   , max(a.amount_of_invoices) amount_of_invoices
   , max(a.assigned_name) assigned_name
   , max(a.assigned_last_name) assigned_last_name
   , max(a.company_id) company_id
   , max(a.user_third_party_id) user_third_party_id
   , max(a.client_id) client_id
   , max(a.provider_id) provider_id
   , max(a.request_id) request_id
   , max(b.client_payment_id) client_payment_id
   , max(b.currency_distribution) payment_currency
   , max(b.payment_date) payment_date
   , sum(COALESCE(b.total_amount_paid, 0)) total_amount_paid
   , sum(b.capital_paid) capital_paid
   , sum(b.interest_paid) interest_paid
   , sum(b.guarantee_paid) guarantee_paid
   , max(b.status) last_status
   , max(b.last_paid_date) last_paid_date
   , COALESCE(max_by(b.distribution_provider_new_capital, b.payment_date), max(a.amount_financed)) distribution_provider_new_capital
   FROM
     (dataset a
   LEFT JOIN client_payments_3 b ON ((b.payment_date >= a.transfer_date) AND (b.payment_date <= a.fecha_cierre) AND (a.code = b.auction_code)))
   GROUP BY a.code, a.fecha_cierre, a.codmes, a.product_type
   ORDER BY a.fecha_cierre ASC
) 
, fac_notification AS (
   SELECT
     json_extract_scalar(_c1, '$.auction_code') auction_code
   , max(DATE_FORMAT(FROM_unixtime((CAST(json_extract_scalar(_c1, '$.payment_date["$date"]') AS BIGINT) / 1000)), '%Y-%m-%d %H:%i:%s.%f')) max_payment_date
   FROM
     "prod_datalake_master"."prestamype__fac_notifications"
   GROUP BY json_extract_scalar(_c1, '$.auction_code')
) 
, fechas_factura AS (
   SELECT
     request_id
   , (CASE WHEN (COUNT(DISTINCT payment_date) = 1) THEN 1 ELSE 0 END) facturas_vencimientos_iguales
   FROM
     "prod_datalake_analytics"."im_invoices"
   WHERE (request_id IS NOT NULL)
   GROUP BY request_id
) 
, dataset_principal2 AS (
   SELECT
     a.*
   , c.facturas_vencimientos_iguales
   , d.fecha_de_pago___confirmado_por_correo fecha_confirmada_hubspot
   , b.max_payment_date max_payment_date_invoices
   , (CASE WHEN ((facturas_vencimientos_iguales = 1) AND (fecha_de_pago___confirmado_por_correo IS NOT NULL)) THEN fecha_de_pago___confirmado_por_correo WHEN ((facturas_vencimientos_iguales = 0) AND (CAST(e_payment_date_original AS timestamp) < fecha_de_pago___confirmado_por_correo)) THEN fecha_de_pago___confirmado_por_correo WHEN ((max_payment_date IS NOT NULL) AND (CAST(codmes AS INT) >= 202405)) THEN CAST(max_payment_date AS timestamp) ELSE CAST(e_payment_date_original AS timestamp) END) e_payment_date
   FROM
     (((dataset_principal a
   LEFT JOIN fac_notification b ON (a.code = b.auction_code))
   LEFT JOIN fechas_factura c ON (a.request_id = c.request_id))
   LEFT JOIN (
      SELECT
        subject
      , fecha_de_pago___confirmado_por_correo
      , hs_pipeline
      FROM
        "prod_datalake_master"."hubspot__ticket"
      WHERE (hs_pipeline = '26417284')
   )  d ON (lower(a.code) = lower(d.subject)))
) 
, cierres AS (
   SELECT
     *
   , (CASE WHEN (CAST(e_payment_date AS DATE) = CAST(e_payment_date_original AS DATE)) THEN 0 ELSE 1 END) cambio_fecha_vencimiento
   , (CASE WHEN (codmes = CAST(DATE_FORMAT(transfer_date, '%Y%m') AS VARCHAR)) THEN 1 ELSE null END) q_desembolso
   , (CASE WHEN (codmes = CAST(DATE_FORMAT(transfer_date, '%Y%m') AS VARCHAR)) THEN amount_financed ELSE null END) m_desembolso
   , (CASE WHEN ((flag_newclient = 1) AND (codmes = CAST(DATE_FORMAT(transfer_date, '%Y%m') AS VARCHAR))) THEN 1 ELSE 0 END) new_clients
   , (CASE WHEN ((flag_newclient > 1) AND (codmes = CAST(DATE_FORMAT(transfer_date, '%Y%m') AS VARCHAR))) THEN 1 ELSE 0 END) recurrent_clients
   , (CASE WHEN ((flag_newprovider = 1) AND (codmes = CAST(DATE_FORMAT(transfer_date, '%Y%m') AS VARCHAR))) THEN 1 ELSE 0 END) new_providers
   , (CASE WHEN ((flag_newprovider > 1) AND (codmes = CAST(DATE_FORMAT(transfer_date, '%Y%m') AS VARCHAR))) THEN 1 ELSE 0 END) recurrent_providers
   , distribution_provider_new_capital remaining_capital
   , (CASE WHEN (ABS((total_net_amount_pending_payment - COALESCE(total_amount_paid, 0))) < 1) THEN 0 WHEN ((product_type = 'confirming') AND ((total_net_amount_pending_payment - COALESCE(total_amount_paid, 0)) < 1)) THEN 0 ELSE GREATEST((total_net_amount_pending_payment - COALESCE(total_amount_paid, 0)), 0) END) remaining_total_amount
   , (CASE WHEN ((DATE_FORMAT(last_paid_date, '%Y%m') = codmes) AND (last_status = 'finalizado')) THEN last_status WHEN ((ABS((amount_financed - COALESCE(capital_paid, 0))) < 1) AND (CAST(codmes AS INT) >= 202405)) THEN 'finalizado' WHEN ((code IN ('lgwcnyax')) AND (CAST(codmes AS INT) >= 202401)) THEN 'CASTigo' WHEN ((code IN ('Pbz6O1PQ', 'jwLrnXYb', 'lgwcx4xa')) AND (CAST(codmes AS INT) >= 202402)) THEN 'CASTigo' WHEN ((code IN ('bNdzoWG6')) AND (CAST(codmes AS INT) >= 202403)) THEN 'CASTigo' WHEN ((code IN ('yj90Y0v8', 'JEgdnQKF', 'iNWIhJ0p')) AND (CAST(codmes AS INT) >= 202406)) THEN 'CASTigo' WHEN ((code IN ('le5w55ao')) AND (CAST(codmes AS INT) >= 202311)) THEN 'CASTigo' WHEN ((code IN ('2bCzIusn', '4vQ5cEuU', 'xcC3tvLG')) AND (CAST(codmes AS INT) >= 202403)) THEN 'CASTigo' WHEN ((code IN ('ktUqf0tz', '63liGJEc', 'UB6Wnclj', 'WtmYWjuo', 'x5PDLyLR')) AND (CAST(codmes AS INT) >= 202504)) THEN 'CASTigo' WHEN ((code IN ('93DfOT0S', 'taZ0mAYW')) AND (CAST(codmes AS INT) >= 202506)) THEN 'CASTigo' WHEN ((client_ruc = '20100102171') AND (CAST(codmes AS INT) > 202311)) THEN 'condonado' ELSE 'vigente' END) actual_status
   , (CASE WHEN ((last_status = 'finalizado') AND (DATE_FORMAT(last_paid_date, '%Y%m') < codmes)) THEN true ELSE false END) flag_excluir
   FROM
     dataset_principal2
) 
, tipo_de_cambio AS (
   SELECT
     DATE_FORMAT(tc_date, '%Y%m') mes
   , tc_contable
   , ROW_NUMBER() OVER (PARTITION BY DATE_FORMAT(tc_date, '%Y%m') ORDER BY tc_date DESC) fila
   FROM
     "prod_datalake_master"."prestamype__tc_contable"
   WHERE (tc_contable <> 0E0)
) 
, cierres_final_1 AS (
   SELECT
     a.*
   , (CASE WHEN ((actual_status <> 'finalizado') AND (e_payment_date >= fecha_cierre)) THEN 0 WHEN ((actual_status <> 'finalizado') AND (e_payment_date < fecha_cierre)) THEN date_diff('day', e_payment_date, fecha_cierre) WHEN (actual_status = 'finalizado') THEN 0 END) dias_atraso
   , (CASE WHEN (a.currency_auctions = 'PEN') THEN m_desembolso WHEN (a.currency_auctions = 'USD') THEN (m_desembolso * b.tc_contable) END) m_desembolso_soles
   , (CASE WHEN (a.currency_auctions = 'PEN') THEN remaining_capital WHEN (a.currency_auctions = 'USD') THEN (remaining_capital * b.tc_contable) END) remaining_capital_soles
   , (CASE WHEN (a.currency_auctions = 'PEN') THEN amount_financed WHEN (a.currency_auctions = 'USD') THEN (amount_financed * b.tc_contable) END) amount_financed_soles
   , b.tc_contable exchange_rate
   FROM
     (cierres a
   LEFT JOIN (
      SELECT *
      FROM
        tipo_de_cambio
      WHERE (fila = 1)
   )  b ON (b.mes = a.codmes))
   WHERE (flag_excluir = false)
   ORDER BY transfer_date ASC, code ASC, a.codmes ASC
) 
SELECT DISTINCT
  a.code
, a.fecha_cierre
, a.codmes
, a.product_type
, a.client_ruc
, a.client_name
, a.provider_ruc
, a.provider_name
, a.flag_newclient
, a.flag_newprovider
, a.transfer_date
, a.ANTERIOR_TRANSFER
, a.currency_request
, a.currency_auctions
, a.assigned_financing_rate
, a.total_net_amount_pending_payment
, a.e_payment_date_original
, a.amount_financed
, a.terms
, a.amount_advance
, a.advance_percentage
, a.invoice_count
, a.amount_of_invoices
, a.assigned_name
, a.assigned_last_name
, a.company_id
, a.user_third_party_id
, a.client_id
, a.provider_id
, a.request_id
, a.client_payment_id
, a.payment_currency
, a.payment_date
, a.total_amount_paid
, a.capital_paid
, a.interest_paid
, a.guarantee_paid
, a.last_status
, a.last_paid_date
, a.facturas_vencimientos_iguales
, a.fecha_confirmada_hubspot
, CAST(a.max_payment_date_invoices AS timestamp) max_payment_date_invoices
, a.e_payment_date
, a.cambio_fecha_vencimiento
, a.q_desembolso
, a.m_desembolso
, a.new_clients
, a.recurrent_clients
, a.new_providers
, a.recurrent_providers
, a.remaining_capital
, a.remaining_total_amount
, a.actual_status
, a.flag_excluir
, a.dias_atraso
, a.m_desembolso_soles
, a.remaining_capital_soles
, a.amount_financed_soles
, a.exchange_rate
, CAST(DATE_FORMAT(transfer_date, '%Y%m') AS VARCHAR) codmes_transfer
, (CASE WHEN (a.dias_atraso >= 1) THEN a.remaining_capital ELSE 0 END) PAR1_m
, (CASE WHEN (a.dias_atraso > 15) THEN a.remaining_capital ELSE 0 END) PAR15_m
, (CASE WHEN (a.dias_atraso > 30) THEN a.remaining_capital ELSE 0 END) PAR30_m
, (CASE WHEN (a.dias_atraso > 60) THEN a.remaining_capital ELSE 0 END) PAR60_m
, (CASE WHEN (a.dias_atraso > 90) THEN a.remaining_capital ELSE 0 END) PAR90_m
, (CASE WHEN (a.dias_atraso > 120) THEN a.remaining_capital ELSE 0 END) PAR120_m
, (CASE WHEN (a.dias_atraso > 180) THEN a.remaining_capital ELSE 0 END) PAR180_m
, (CASE WHEN (a.dias_atraso > 360) THEN a.remaining_capital ELSE 0 END) PAR360_m
, (CASE WHEN (a.dias_atraso >= 1) THEN 1 ELSE 0 END) PAR1_q
, (CASE WHEN (a.dias_atraso > 15) THEN 1 ELSE 0 END) PAR15_q
, (CASE WHEN (a.dias_atraso > 30) THEN 1 ELSE 0 END) PAR30_q
, (CASE WHEN (a.dias_atraso > 60) THEN 1 ELSE 0 END) PAR60_q
, (CASE WHEN (a.dias_atraso > 90) THEN 1 ELSE 0 END) PAR90_q
, (CASE WHEN (a.dias_atraso > 120) THEN 1 ELSE 0 END) PAR120_q
, (CASE WHEN (a.dias_atraso > 180) THEN 1 ELSE 0 END) PAR180_q
, (CASE WHEN (a.dias_atraso > 360) THEN 1 ELSE 0 END) PAR360_q
, (CASE WHEN (a.actual_status = 'vigente') THEN 1 ELSE 0 END) q_vigente
, (CASE WHEN ((a.client_ruc IN ('20100102171')) AND (CAST(a.codmes AS INT) > 202311)) THEN 1 ELSE 0 END) condoned
, (CASE WHEN ((b.ruc IS NOT NULL) AND (a.dias_atraso > 60)) THEN 'Judicial' ELSE 'No Judicial' END) judicialized
, (CASE WHEN (a.dias_atraso = 0) THEN U&'a. 0 d\00EDas' WHEN (a.dias_atraso BETWEEN 1 AND 15) THEN 'b. Entre 1 - 15' WHEN (a.dias_atraso BETWEEN 16 AND 30) THEN 'c. Entre 16 - 30' WHEN (a.dias_atraso BETWEEN 31 AND 60) THEN 'd. Entre 31 - 60' WHEN (a.dias_atraso BETWEEN 61 AND 90) THEN 'e. Entre 61 - 90' WHEN (a.dias_atraso BETWEEN 91 AND 120) THEN 'f. Entre 91 - 120' WHEN (a.dias_atraso BETWEEN 121 AND 180) THEN 'g. Entre 121 - 180' WHEN (a.dias_atraso BETWEEN 181 AND 365) THEN 'h. Entre 181 - 365' ELSE U&'i. M\00E1s de 365 d\00EDas' END) rango_dias_atraso
, (CASE WHEN (CAST(a.terms AS INT) < 31) THEN '1 Mes' WHEN (CAST(a.terms AS INT) < 61) THEN '2 Meses' WHEN (CAST(a.terms AS INT) < 91) THEN '3 Meses' ELSE U&'M\00E1s de 4' END) rango_duracion
, (CASE WHEN (a.dias_atraso >= 1) THEN a.remaining_capital_soles ELSE 0 END) PAR1_ms
, (CASE WHEN (a.dias_atraso > 15) THEN a.remaining_capital_soles ELSE 0 END) PAR15_ms
, (CASE WHEN (a.dias_atraso > 30) THEN a.remaining_capital_soles ELSE 0 END) PAR30_ms
, (CASE WHEN (a.dias_atraso > 60) THEN a.remaining_capital_soles ELSE 0 END) PAR60_ms
, (CASE WHEN (a.dias_atraso > 90) THEN a.remaining_capital_soles ELSE 0 END) PAR90_ms
, (CASE WHEN (a.dias_atraso > 120) THEN a.remaining_capital_soles ELSE 0 END) PAR120_ms
, (CASE WHEN (a.dias_atraso > 180) THEN a.remaining_capital_soles ELSE 0 END) PAR180_ms
, (CASE WHEN (a.dias_atraso > 360) THEN a.remaining_capital_soles ELSE 0 END) PAR360_ms
FROM
  (cierres_final_1 a
LEFT JOIN "prod_datalake_master"."prestamype__fac_judicial" b ON ((CAST(a.client_ruc AS VARCHAR) = CAST(b.ruc AS VARCHAR)) AND (CAST(a.codmes AS INT) = CAST(b.codmes AS INT))))