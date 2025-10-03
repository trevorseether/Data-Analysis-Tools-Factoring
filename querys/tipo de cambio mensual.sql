--CREATE OR REPLACE VIEW "tipo_cambio_jmontoya" AS 
WITH
  tc_con_mes AS (
   SELECT
     pk
   , tc_date
   , ((DATE_TRUNC('month', tc_date) + INTERVAL  '1' MONTH) - INTERVAL  '1' DAY) mes_tc
   , tc_contable exchange_rate
   , ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', tc_date) ORDER BY tc_date DESC) rn
   FROM
     prod_datalake_master.prestamype__tc_contable
   WHERE (tc_contable <> 0)
) 
SELECT
  pk
, tc_date
, mes_tc
, CAST(date_format(mes_tc, '%Y%m') AS INTEGER) tc_codmes
, exchange_rate
FROM
  tc_con_mes
WHERE (rn = 1)