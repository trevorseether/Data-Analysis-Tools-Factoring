--CREATE OR REPLACE VIEW "fac_outst_unidos_f_desembolso_jmontoya" AS 
WITH
  fac_outstandings_unidos AS (
   SELECT
     *
   , 'online' FLAG_ORIGEN_OPERACION
   FROM
     prod_datalake_analytics."fac_outst_fecha_desembolso_request_jmontoya"
UNION ALL    SELECT
     *
   , 'offline' FLAG_ORIGEN_OPERACION
   FROM
     prod_datalake_analytics.fac_outst_offline_jmontoya
   WHERE (NOT (code IN (SELECT DISTINCT code
FROM
  prod_datalake_analytics."fac_outst_fecha_desembolso_request_jmontoya"
WHERE (code IS NOT NULL)
UNION SELECT DISTINCT code
FROM
  prod_datalake_analytics.fac_outst_compra_interna_jmontoya
WHERE (code IS NOT NULL)
)))
UNION ALL    SELECT
     *
   , 'compra interna' FLAG_ORIGEN_OPERACION
   FROM
     prod_datalake_analytics.fac_outst_compra_interna_jmontoya
) 
SELECT
  code
, fecha_cierre
, codmes
, product_type
, client_ruc
, client_name
, provider_ruc
, provider_name
, ROW_NUMBER() OVER (PARTITION BY client_ruc ORDER BY fecha_cierre ASC, transfer_date ASC) flag_newclient
, ROW_NUMBER() OVER (PARTITION BY provider_ruc ORDER BY fecha_cierre ASC, transfer_date ASC) flag_newprovider
, transfer_date
, ANTERIOR_TRANSFER ANTERIOR_TRANSFER
, currency_request
, currency_auctions
, assigned_financing_rate
, total_net_amount_pending_payment
, e_payment_date_original
, amount_financed
, terms
, amount_advance
, advance_percentage
, invoice_count
, amount_of_invoices
, assigned_name
, assigned_last_name
, company_id
, user_third_party_id
, client_id
, provider_id
, request_id
, client_payment_id
, payment_currency
, payment_date
, total_amount_paid
, capital_paid
, interest_paid
, guarantee_paid
, last_status
, last_paid_date
, facturas_vencimientos_iguales
, fecha_confirmada_hubspot
, max_payment_date_invoices
, e_payment_date
, cambio_fecha_vencimiento
, q_desembolso
, m_desembolso
, (CASE WHEN (client_n = 1) THEN 1 ELSE 0 END) new_clients
, (CASE WHEN (client_n = 1) THEN 0 ELSE 1 END) recurrent_clients
, (CASE WHEN (provider_n = 1) THEN 1 ELSE 0 END) new_providers
, (CASE WHEN (provider_n = 1) THEN 0 ELSE 1 END) recurrent_providers
, remaining_capital
, remaining_total_amount
, actual_status
, flag_excluir
, dias_atraso
, m_desembolso_soles
, remaining_capital_soles
, amount_financed_soles
, exchange_rate
, CAST(codmes_transfer AS VARCHAR) codmes_transfer
, PAR1_m
, PAR15_m
, PAR30_m
, PAR60_m
, PAR90_m
, PAR120_m
, PAR180_m
, PAR360_m
, PAR1_q
, PAR15_q
, PAR30_q
, PAR60_q
, PAR90_q
, PAR120_q
, PAR180_q
, PAR360_q
, q_vigente
, condoned
, judicialized
, rango_dias_atraso
, rango_duracion
, PAR1_ms
, PAR15_ms
, PAR30_ms
, PAR60_ms
, PAR90_ms
, PAR120_ms
, PAR180_ms
, PAR360_ms
, FLAG_ORIGEN_OPERACION
FROM
  (
   SELECT
     *
   , ROW_NUMBER() OVER (PARTITION BY client_ruc ORDER BY fecha_cierre ASC, transfer_date ASC) client_n
   , ROW_NUMBER() OVER (PARTITION BY provider_ruc ORDER BY fecha_cierre ASC, transfer_date ASC) provider_n
   FROM
     fac_outstandings_unidos
) 
WHERE ((code IS NOT NULL) AND (codmes_transfer IS NOT NULL))
