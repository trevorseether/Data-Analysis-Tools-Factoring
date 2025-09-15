--create or replace view prod_datalake_analytics.fac_costo_fin_madres_jmontoya as

with fechas_desembolso_offline as (
                select
                    dealname                             AS "Código de Subasta",
                    fecha_de_desembolso__factoring_      AS "fecha_desembolso",
                    cast(tasa_de_venta____  as varchar)  AS "tasa_inversionista"

                from prod_datalake_master.hubspot__deal
                where dealname is not null
                and pipeline = '14026011'
                and dealstage not in ('14026016' , '14026018')
                and tasa_de_venta____ is not null
                
                -- no se deben considerar ni rechazados ni cancelados(subasta desierta) porque no tienen fecha de desembolso y generan duplicidad
),fechas_desembolso_online as (
        SELECT
            fr.code AS "Código de Subasta",
            CASE WHEN fr.interest_proforma_disbursement_date IS NOT NULL THEN  fr.interest_proforma_disbursement_date
                ELSE off."fecha_desembolso"
            END AS "fecha_desembolso"

        FROM prod_datalake_analytics.fac_requests as fr
        left join fechas_desembolso_offline as off on fr.code = off."Código de Subasta"
            WHERE 1 = 1
            --and fr.STATUS not in ('rejected', 'canceled')
            AND fr.interest_proforma_disbursement_date IS NOT NULL
),desembolsos_totales AS (
        SELECT * FROM fechas_desembolso_online
        UNION all
        SELECT
            "Código de Subasta",
            "fecha_desembolso"
        FROM
            fechas_desembolso_offline WHERE "Código de Subasta" NOT IN (SELECT "Código de Subasta" FROM fechas_desembolso_online)
), tasa_inversionista_online as(
select
    f.code,
    CASE
    WHEN f.proforma_strategy_name = 'factoring-v1-new' THEN f.proforma_profit_interest_rate--*(0.9)
    ELSE f.proforma_financing_interest_rate --*(0.9)
    end as "tasa_inversionista"

FROM prod_datalake_analytics.FAC_REQUESTS AS F
where f.status not in ('rejected', 'canceled')


), tasa_inversionista_unido as (

select
    "code",
    cast("tasa_inversionista" AS VARCHAR) AS "tasa_inversionista"

from tasa_inversionista_online
    union all
select
    "Código de Subasta",
    "tasa_inversionista"
from fechas_desembolso_offline
where "Código de Subasta" not in (select code from tasa_inversionista_online)

), resultado_final as (

SELECT
    FR.CODE,
    interest_proforma_simulation_financing_cost_value as costo_financiamiento,
    C.code as factura_costo_financiamiento,
    dt."fecha_desembolso"

FROM prod_datalake_analytics.fac_requests AS FR

left join (select * from prod_datalake_analytics.view_prestamype_fac_cpe where concept='interest-factoring') c
on FR._id=c.request_id

left join desembolsos_totales as dt on dt."Código de Subasta" = fr.code
LEFT JOIN tasa_inversionista_unido  AS TI ON TI.CODE = FR.CODE
)
select * from resultado_final