--CREATE OR REPLACE VIEW "fac_outst_compra_interna_jmontoya" AS 
WITH
  cortes_mensuales AS (
   SELECT DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', (x + 1), DATE '2021-03-31'))) fecha_eomonth
   FROM
     UNNEST(SEQUENCE(0, 120)) t (x)
   WHERE (DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', (x + 1), DATE '2021-03-31'))) <= DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, current_date))))
) 
, datos_planos AS (
   SELECT
     hubspot__deal.dealname "Code"
   , hubspot__deal.proveedor "company_name"
   , CAST(hubspot__deal.ruc_proveedor AS VARCHAR) "company_ruc"
   , hubspot__deal.cliente "user_third_party_name"
   , hubspot__deal.ruc_cliente "user_third_party_ruc"
   , hubspot__deal.tipo_de_producto "product"
   , ROUND(hubspot__deal.monto_adelanto, 2) "monto_de_adelanto"
   , ROUND(hubspot__deal.monto_financiado, 2) "monto_financiado"
   , hubspot__deal.moneda_de_la_factura "Moneda_Monto_Factura"
   , hubspot__deal.moneda_del_monto_financiado "moneda_del_monto_financiado"
   , (TRY_CAST(REPLACE(REPLACE(hubspot__deal.tasa_de_financiamiento____, '%', ''), ',', '.') AS DOUBLE) / 100) "tasa_de_financiamiento_asignada"
   , CAST((hubspot__deal.closedate - INTERVAL  '5' HOUR) AS DATE) "fecha_de_cierre_de_subasta"
   , hubspot__deal.fecha_de_pago__factoring_ "fecha_esperada_pago"
   , hubspot__deal.fecha_de_pago___confirmado_por_correo "fecha_confirmada_correo"
   , (CASE WHEN (hubspot__deal.fecha_de_pago___confirmado_por_correo IS NOT NULL) THEN hubspot__deal.fecha_de_pago___confirmado_por_correo ELSE hubspot__deal.fecha_de_pago__factoring_ END) "e_payment_date"
   , hubspot__deal.comision_estructuracion "comision_total"
   , round((hubspot__deal.comision_estructuracion * (1 / 1.18E0)), 2) "comision_sin_igv"
   , CAST(hubspot__deal.fecha_de_registro_de_compra___compra_operacion AS date) "Fecha_de_desembolso_hub"
   , ((date_trunc('month', CAST(hubspot__deal.fecha_de_registro_de_compra___compra_operacion AS date)) + INTERVAL  '1' MONTH) - INTERVAL  '1' DAY) "MES_DESEMBOLSO"
   , (CASE WHEN (hubspot__deal.flag_comprado = 'Si') THEN 'COMPRA INTERNA' ELSE 'OP OFFLINE' END) flag_es_offline
   , COALESCE(FC1."Fecha de Cierre Final", FC2.FECHA_PAGO_HUB_TICKETS) "FECHA CANCELACION"
   , ((date_trunc('month', FC2.FECHA_PAGO_HUB_TICKETS) + INTERVAL  '1' MONTH) - INTERVAL  '1' DAY) "MES CANCELACION"
   , hubspot__deal.n__facturas
   , hubspot__deal.monto_neto_de_facturas__factoring_
   , hubspot_owner_id
   FROM
     ((((prod_datalake_master.hubspot__deal hubspot__deal
   LEFT JOIN prod_datalake_master.factoring__fac_auctions_disbursement_date fac_disb ON (hubspot__deal.dealname = fac_disb.codigo_de_subasta))
   LEFT JOIN (
      SELECT
        hubspot__pagos_facturas.c_digo_de_subasta code
      , MIN(DATE(hubspot__pagos_facturas.fecha_de_desembolso_registrado)) Fecha_Minima_de_Desembolso
      FROM
        prod_datalake_master.hubspot__pagos_facturas hubspot__pagos_facturas
      WHERE ((hubspot__pagos_facturas.hs_pipeline_stage = '89540624') AND (hubspot__pagos_facturas.fecha_de_desembolso_registrado IS NOT NULL) AND (hubspot__pagos_facturas.fecha_de_desembolso_registrado > DATE('2024-08-31')))
      GROUP BY hubspot__pagos_facturas.c_digo_de_subasta
   )  pagos_fact ON (hubspot__deal.dealname = pagos_fact.code))
   LEFT JOIN (
      SELECT
        max(a."date") "Fecha de Cierre Final"
      , b.code "Código de Subasta"
      FROM
        ((prod_datalake_analytics.fac_client_payment_payments a
      LEFT JOIN prod_datalake_analytics.fac_client_payments c ON (a.client_payment_id = c._id))
      LEFT JOIN prod_datalake_analytics.fac_requests b ON (c.request_id = b._id))
      WHERE ((b.status = 'closed') AND (c.status = 'finalizado'))
      GROUP BY b.code
   )  FC1 ON (FC1."Código de Subasta" = hubspot__deal.dealname))
   LEFT JOIN (
      SELECT
        subject
      , closed_date "FECHA_PAGO_HUB_TICKETS"
      , tipo_de_pago
      --, fecha_de_registro_de_compra___compra_operacion as "fecha_de_registro_de_compra" -- esta columna la han retirado de tickets, y la han movido a Deals
      FROM
        prod_datalake_master.hubspot__ticket
      WHERE ((hs_pipeline = '26417284') AND (subject LIKE '% - %'))
   )  FC2 ON (FC2.SUBJECT = hubspot__deal.dealname))
   WHERE ((hubspot__deal.pipeline = '1105313628') OR (hubspot__deal.flag_comprado = 'Si'))
) 
, cobranza AS (
   SELECT
     monto_registrado
   , fecha_de_pago_registrado
   , ((date_trunc('month', fecha_de_pago_registrado) + INTERVAL  '1' MONTH) - INTERVAL  '1' DAY) mes_pago
   , c_digo_de_subasta
   FROM
     prod_datalake_master.hubspot__pagos_facturas
   WHERE ((hs_pipeline = '42484623') AND (fecha_de_pago_registrado IS NOT NULL))
) 
, cobranza_cortes_mensuales_desagrupada AS (
   SELECT
     cm.fecha_eomonth
   , cob.c_digo_de_subasta
   , cob.monto_registrado
   FROM
     (cortes_mensuales cm
   LEFT JOIN cobranza cob ON true)
   WHERE (cob.mes_pago <= cm.fecha_eomonth)
) 
, cobranza_agrupada AS (
   SELECT
     fecha_eomonth
   , c_digo_de_subasta
   , sum(monto_registrado) "COBRANZA"
   FROM
     cobranza_cortes_mensuales_desagrupada
   GROUP BY fecha_eomonth, c_digo_de_subasta
) 
, cartera_1 AS (
   SELECT
     dp.code code
   , cm.fecha_eomonth fecha_cierre
   , DATE_FORMAT(cm.fecha_eomonth, '%Y%m') codmes
   , DP.product product_type
   , CAST(CAST(CAST(DP.user_third_party_ruc AS DOUBLE) AS BIGINT) AS VARCHAR) client_ruc
   , DP.user_third_party_name client_name
   , CAST(CAST(CAST(DP.company_ruc AS DOUBLE) AS BIGINT) AS VARCHAR) provider_ruc
   , DP.company_name provider_name
   , CAST(0 AS BIGINT) flag_newclient
   , CAST(0 AS BIGINT) flag_newprovider
   , DP.Fecha_de_desembolso_hub transfer_date
   , DP.Fecha_de_desembolso_hub ANTERIOR_TRANSFER
   , DP.Moneda_Monto_Factura currency_request
   , DP.moneda_del_monto_financiado currency_auctions
   , ROUND(DP.tasa_de_financiamiento_asignada, 5) assigned_financing_rate
   , (CASE WHEN (CM.fecha_eomonth = DP."MES CANCELACION") THEN 0 ELSE dp.monto_neto_de_facturas__factoring_ END) total_net_amount_pending_payment
   , DP.fecha_esperada_pago e_payment_date_original
   , DP.monto_financiado amount_financed
   , 0 terms
   , DP.monto_de_adelanto amount_advance
   , (CASE WHEN ((dp.monto_de_adelanto IS NULL) OR (dp.monto_de_adelanto = 0)) THEN null ELSE (dp.monto_de_adelanto / dp.monto_financiado) END) advance_percentage
   , dp.n__facturas invoice_count
   , dp.monto_neto_de_facturas__factoring_ amount_of_invoices
   , CAST(dp.hubspot_owner_id AS VARCHAR) assigned_name
   , ' ' assigned_last_name
   , ' ' company_id
   , ' ' user_third_party_id
   , ' ' client_id
   , ' ' provider_id
   , ' ' request_id
   , ' ' client_payment_id
   , DP.moneda_del_monto_financiado payment_currency
   , DP."FECHA CANCELACION" payment_date
   , CA.COBRANZA total_amount_paid
   , (CASE WHEN (CM.fecha_eomonth = DP."MES CANCELACION") THEN DP.monto_financiado ELSE 0 END) capital_paid
   , (CASE WHEN (CM.fecha_eomonth = DP."MES CANCELACION") THEN round((DP.monto_financiado * (pow((1 + DP.tasa_de_financiamiento_asignada), (DATE_DIFF('day', DP.Fecha_de_desembolso_hub, DP."FECHA CANCELACION") / 30)) - 1)), 2) ELSE 0 END) interest_paid
   , (CASE WHEN (CM.fecha_eomonth = DP."MES CANCELACION") THEN GREATEST(ROUND((COALESCE(CA."COBRANZA", 0) - (DP.monto_financiado * pow((1 + DP.tasa_de_financiamiento_asignada), (DATE_DIFF('day', DP.Fecha_de_desembolso_hub, DP."FECHA CANCELACION") / 30)))), 2), 0) ELSE 0 END) guarantee_paid
   , (CASE WHEN (CM.fecha_eomonth = DP."MES CANCELACION") THEN 'finalizado' ELSE 'por pagar' END) last_status
   , DP."FECHA CANCELACION" last_paid_date
   , 1 facturas_vencimientos_iguales
   , DP.fecha_confirmada_correo fecha_confirmada_hubspot
   , CAST(DP.fecha_esperada_pago AS timestamp) max_payment_date_invoices
   , CAST(DP.e_payment_date AS timestamp) e_payment_date
   , CAST(0 AS BIGINT) cambio_fecha_vencimiento
   , 0 q_desembolso
   , 0 m_desembolso
   , 0 new_clients
   , 0 recurrent_clients
   , 0 new_providers
   , 0 recurrent_providers
   , (CASE WHEN (dp."MES CANCELACION" = cm.fecha_eomonth) THEN 0 ELSE round(dp.monto_financiado, 2) END) remaining_capital
   , (CASE WHEN (cm.fecha_eomonth < DP."FECHA CANCELACION") THEN dp.monto_neto_de_facturas__factoring_ ELSE 0 END) remaining_total_amount
   , (CASE WHEN (CM.fecha_eomonth = DP."MES CANCELACION") THEN 'finalizado' ELSE 'vigente' END) actual_status
   , true flag_excluir
   , (CASE WHEN (DP.e_payment_date > CM.fecha_eomonth) THEN 0 WHEN ((DP.e_payment_date < CM.fecha_eomonth) AND (DP."FECHA CANCELACION" IS NOT NULL) AND (DP."FECHA CANCELACION" > CM.fecha_eomonth)) THEN DATE_DIFF('day', DP.e_payment_date, CM.fecha_eomonth) WHEN (DP."FECHA CANCELACION" IS NULL) THEN DATE_DIFF('day', DP.e_payment_date, CM.fecha_eomonth) WHEN (DP."FECHA CANCELACION" < CM.fecha_eomonth) THEN 0 END) "dias_atraso"
   , (CASE WHEN ((cm.fecha_eomonth = DP.MES_DESEMBOLSO) AND (DP.moneda_del_monto_financiado = 'PEN')) THEN dp.monto_financiado WHEN ((cm.fecha_eomonth = DP.MES_DESEMBOLSO) AND (DP.moneda_del_monto_financiado = 'USD')) THEN (dp.monto_financiado * tc.exchange_rate) ELSE 0 END) m_desembolso_soles
   , (CASE WHEN (dp."MES CANCELACION" = cm.fecha_eomonth) THEN 0 WHEN (DP.moneda_del_monto_financiado = 'PEN') THEN ROUND(dp.monto_financiado, 2) WHEN (DP.moneda_del_monto_financiado = 'USD') THEN ROUND((dp.monto_financiado * tc.exchange_rate), 2) END) remaining_capital_soles
   , (CASE WHEN (DP.moneda_del_monto_financiado = 'PEN') THEN dp.monto_financiado WHEN (DP.moneda_del_monto_financiado = 'USD') THEN (dp.monto_financiado * tc.exchange_rate) END) amount_financed_soles
   , tc.exchange_rate exchange_rate
   , DATE_FORMAT(DP.Fecha_de_desembolso_hub, '%Y%m') codmes_transfer
   FROM
     (((cortes_mensuales cm
   LEFT JOIN datos_planos DP ON (true AND (DP.MES_DESEMBOLSO <= CM.fecha_eomonth) AND (CM.fecha_eomonth <= COALESCE(DP."MES CANCELACION", ((date_trunc('month', current_date) + INTERVAL  '1' MONTH) - INTERVAL  '1' DAY)))))
   LEFT JOIN prod_datalake_analytics.tipo_cambio_jmontoya "TC" ON (cm.fecha_eomonth = TC.mes_tc))
   LEFT JOIN cobranza_agrupada CA ON ((CM.fecha_eomonth = CA.fecha_eomonth) AND (DP.Code = CA.c_digo_de_subasta)))
) 
SELECT DISTINCT
  c1.*
, (CASE WHEN (c1.dias_atraso > 0) THEN c1.remaining_capital ELSE 0 END) "PAR1_m"
, (CASE WHEN (c1.dias_atraso > 15) THEN c1.remaining_capital ELSE 0 END) "PAR15_m"
, (CASE WHEN (c1.dias_atraso > 30) THEN c1.remaining_capital ELSE 0 END) "PAR30_m"
, (CASE WHEN (c1.dias_atraso > 60) THEN c1.remaining_capital ELSE 0 END) "PAR60_m"
, (CASE WHEN (c1.dias_atraso > 90) THEN c1.remaining_capital ELSE 0 END) "PAR90_m"
, (CASE WHEN (c1.dias_atraso > 120) THEN c1.remaining_capital ELSE 0 END) "PAR120_m"
, (CASE WHEN (c1.dias_atraso > 180) THEN c1.remaining_capital ELSE 0 END) "PAR180_m"
, (CASE WHEN (c1.dias_atraso > 360) THEN c1.remaining_capital ELSE 0 END) "PAR360_m"
, (CASE WHEN (c1.dias_atraso > 0) THEN 1 ELSE 0 END) "PAR1_q"
, (CASE WHEN (c1.dias_atraso > 15) THEN 1 ELSE 0 END) "PAR15_q"
, (CASE WHEN (c1.dias_atraso > 30) THEN 1 ELSE 0 END) "PAR30_q"
, (CASE WHEN (c1.dias_atraso > 60) THEN 1 ELSE 0 END) "PAR60_q"
, (CASE WHEN (c1.dias_atraso > 90) THEN 1 ELSE 0 END) "PAR90_q"
, (CASE WHEN (c1.dias_atraso > 120) THEN 1 ELSE 0 END) "PAR120_q"
, (CASE WHEN (c1.dias_atraso > 180) THEN 1 ELSE 0 END) "PAR180_q"
, (CASE WHEN (c1.dias_atraso > 360) THEN 1 ELSE 0 END) "PAR360_q"
, (CASE WHEN (c1.actual_status = 'vigente') THEN 1 ELSE 0 END) "q_vigente"
, (CASE WHEN (c1.client_ruc IN (' ')) THEN 1 ELSE 0 END) "condoned"
, (CASE WHEN ((jud.ruc IS NOT NULL) AND (c1.dias_atraso > 60)) THEN 'Judicial' ELSE 'No Judicial' END) "judicialized"
, (CASE WHEN (c1.dias_atraso = 0) THEN U&'a. 0 d\00EDas' WHEN (c1.dias_atraso BETWEEN 1 AND 15) THEN 'b. Entre 1 - 15' WHEN (c1.dias_atraso BETWEEN 16 AND 30) THEN 'c. Entre 16 - 30' WHEN (c1.dias_atraso BETWEEN 31 AND 60) THEN 'd. Entre 31 - 60' WHEN (c1.dias_atraso BETWEEN 61 AND 90) THEN 'e. Entre 61 - 90' WHEN (c1.dias_atraso BETWEEN 91 AND 120) THEN 'f. Entre 91 - 120' WHEN (c1.dias_atraso BETWEEN 121 AND 180) THEN 'g. Entre 121 - 180' WHEN (c1.dias_atraso BETWEEN 181 AND 365) THEN 'h. Entre 181 - 365' ELSE U&'i. M\00E1s de 365 d\00EDas' END) rango_dias_atraso
, (CASE WHEN (CAST(c1.terms AS INT) < 31) THEN '1 Mes' WHEN (CAST(c1.terms AS INT) < 61) THEN '2 Meses' WHEN (CAST(c1.terms AS INT) < 91) THEN '3 Meses' ELSE U&'M\00E1s de 4' END) rango_duracion
, (CASE WHEN (c1.dias_atraso > 0) THEN c1.remaining_capital_soles ELSE 0 END) "PAR1_ms"
, (CASE WHEN (c1.dias_atraso > 15) THEN c1.remaining_capital_soles ELSE 0 END) "PAR15_ms"
, (CASE WHEN (c1.dias_atraso > 30) THEN c1.remaining_capital_soles ELSE 0 END) "PAR30_ms"
, (CASE WHEN (c1.dias_atraso > 60) THEN c1.remaining_capital_soles ELSE 0 END) "PAR60_ms"
, (CASE WHEN (c1.dias_atraso > 90) THEN c1.remaining_capital_soles ELSE 0 END) "PAR90_ms"
, (CASE WHEN (c1.dias_atraso > 120) THEN c1.remaining_capital_soles ELSE 0 END) "PAR120_ms"
, (CASE WHEN (c1.dias_atraso > 180) THEN c1.remaining_capital_soles ELSE 0 END) "PAR180_ms"
, (CASE WHEN (c1.dias_atraso > 360) THEN c1.remaining_capital_soles ELSE 0 END) "PAR360_ms"
FROM
  (cartera_1 c1
LEFT JOIN "prod_datalake_master"."prestamype__fac_judicial" jud ON (CAST(c1.client_ruc AS VARCHAR) = CAST(jud.ruc AS VARCHAR)))
