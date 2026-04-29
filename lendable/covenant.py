# -*- coding: utf-8 -*-
"""
Created on Fri Apr 24 09:37:02 2026

@author: Joseph Montoya
"""
# =============================================================================
# COVENANT
# resultados deben estar en dolares
# =============================================================================

import pandas as pd
import requests
from io import BytesIO
# import numpy as np
# import boto3
from pyathena import connect
# import openpyxl
from openpyxl import load_workbook
from openpyxl.styles import NamedStyle
import os

import shutil
from datetime import datetime

import warnings
warnings.filterwarnings("ignore")

#%% Credenciales de AmazonAthena
import json
with open(r"C:/Users/Joseph Montoya/Desktop/credenciales actualizado.txt") as f:
    creds = json.load(f)

conn = connect(
    aws_access_key_id     = creds["AccessKeyId"],
    aws_secret_access_key = creds["SecretAccessKey"],
    aws_session_token     = creds["SessionToken"],
    s3_staging_dir        = creds["s3_staging_dir"],
    region_name           = creds["region_name"]
    
    )

#%%
lista_lendable = ['P02175',
                        'P02483',
                        'P02639',
                        'P02677',
                        'P02733',
                        'P02773',
                        'P02791',
                        'P02809',
                        'P02949',
                        'P03045',
                        'P03049',
                        'P03501',
                        'P03570',
                        'P03419']
query = ''' 
select * from prod_datalake_master.ba__data_portafolio_pgh
order by cierre desc
'''
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
df = pd.DataFrame(resultados, columns = column_names)

df_filtrado = df[df['status'] == 'VIGENTE']
df_gestora = df_filtrado[df_filtrado['financiamiento'] == 'GESTORA']

df_filtrado = df_filtrado[df_filtrado['contract_id'].isin(lista_lendable)]

agrup = df_filtrado.pivot_table(index   = 'cierre',
                                values  = ['capital_soles', 'capital_30d','capital_60d','capital_90d'],
                                aggfunc = 'sum').reset_index()
agrup['%par30'] = agrup['capital_30d'] / agrup['capital_soles']
agrup['%par60'] = agrup['capital_60d'] / agrup['capital_soles']
agrup['%par90'] = agrup['capital_90d'] / agrup['capital_soles']
# agrup.to_excel('asd.xlsx')

# calculado el capital 


gestora_par_90 = df_gestora.pivot_table(index   = 'cierre',
                                values  = ['capital_soles', 'capital_30d','capital_60d','capital_90d'],
                                aggfunc = 'sum').reset_index()
gestora_par_90['%par90'] = gestora_par_90['capital_90d'] / gestora_par_90['capital_soles']
# gestora_par_90.to_excel('asd.xlsx')

#%% porcentaje de concentración

concen = df_filtrado[['cierre', 'contract_id', 'capital_soles']]

concen = concen.pivot_table(index   = ['cierre', 'contract_id'],
                            values  = 'capital_soles',
                            aggfunc = 'sum').reset_index()

saldo_mes = concen.pivot_table(index   = 'cierre',
                               values  = 'capital_soles',
                               aggfunc = 'sum').reset_index()
saldo_mes = saldo_mes.rename(columns={'capital_soles': 'capital del mes'})

concen = concen.merge(saldo_mes,
                      on  = 'cierre',
                      how = 'left')
concen['%concent'] = concen['capital_soles'] / concen['capital del mes']

concen = concen.pivot_table(index  = 'cierre',
                            values = '%concent',
                            aggfunc = 'mean').reset_index()


#%%
query = ''' 

with
  cortes_mensuales AS (
   SELECT DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', (x + 1), DATE '2017-03-31'))) fecha_eomonth
   FROM
     UNNEST(SEQUENCE(0, 120)) t (x)
   WHERE (DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', (x + 1), DATE '2017-03-31'))) <= DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('month', 1, current_date))))

), finalizados as (

    select
  "codigo_de_cuota" ,
  "codigo_operacion", 
  "codigo_contrato" ,
  "codigo_empresario", 
  "empresario" ,
  "codigo_inversionista", 
  "inversionista" ,
  "tipo_de_prestamo" ,
  "situacion_del_credito", 
  "distrito" ,
  "cuota_esperada_mensual",
  "nro_cuotas" ,
  "fecha_de_pago_esperada_original", 
  "saldo_por_cancelar" ,
  "interes_esperado_fraccionado_original", 
  "amortizacion_esperada_fraccionado_original", 
  "interes_esperado_original" ,
  "amortizacion_esperada_original" ,
  "igv_esperada_original" ,
  "cuota_esperada_actualizada" ,
  "saldo_por_cancelar_esperado_actualizada" ,
  "capital_fraccionado_actualizado" ,
  "interes_fraccionado_actualizado" ,
  "interes_esperado_actualizado" ,
  "amortizacion_esperada_actualizado" ,
  "igv_esperada_actualizado" ,
  "fecha_de_pago_del_cliente" ,
  "monto_total_pagado_al_credito" ,
  "capital_fraccionado_pagado" ,
  "interes_fraccionado_pagado" ,
  "capital_pagado" ,
  "interes_pagado" ,
  "igv_pagado" ,
  "descuento_de_interes" ,
  "descuento_de_capital" ,
  "monto_ampliado_renovado_o_sustituido" ,
  "penalidades" ,
  "saldo_a_favor" ,
  "dias_de_atraso_de_pago" ,
  "status" ,
  "dias_de_adelanto_de_pago" ,
  "seguimiento_de_pagos" ,
  "condicion_actual_del_credito" ,
  "detalle_de_condicion" ,
  "condicion_asignada" ,
  "causal_de_cancelacion" ,
  "medio_de_cancelacion" ,
  "puntualidad" ,
  "moneda" ,
  "fondo" ,
  "estado_de_prestamo"
  
    from prod_datalake_master.bd_pagos__bd_pagos_finalizados

), vigentes as (       

    select   
  v."codigo_de_cuota" ,
  v."codigo_operacion", 
  v."codigo_contrato" ,
  v."codigo_empresario", 
  v."empresario" ,
  v."codigo_inversionista", 
  v."inversionista" ,
  v."tipo_de_prestamo" ,
  v."situacion_del_credito", 
  v."distrito" ,
  v."cuota_esperada_mensual",
  v."nro_cuotas" ,
  v."fecha_de_pago_esperada_original", 
  v."saldo_por_cancelar" ,
  v."interes_esperado_fraccionado_original", 
  v."amortizacion_esperada_fraccionado_original", 
  v."interes_esperado_original" ,
  v."amortizacion_esperada_original" ,
  v."igv_esperada_original" ,
  v."cuota_esperada_actualizada" ,
  v."saldo_por_cancelar_esperado_actualizada" ,
  v."capital_fraccionado_actualizado" ,
  v."interes_fraccionado_actualizado" ,
  v."interes_esperado_actualizado" ,
  v."amortizacion_esperada_actualizado" ,
  v."igv_esperada_actualizado" ,
  v."fecha_de_pago_del_cliente" ,
  v."monto_total_pagado_al_credito" ,
  v."capital_fraccionado_pagado" ,
  v."interes_fraccionado_pagado" ,
  v."capital_pagado" ,
  v."interes_pagado" ,
  v."igv_pagado" ,
  v."descuento_de_interes" ,
  v."descuento_de_capital" ,
  v."monto_ampliado_renovado_o_sustituido" ,
  v."penalidades" ,
  v."saldo_a_favor" ,
  v."dias_de_atraso_de_pago" ,
  v."status" ,
  v."dias_de_adelanto_de_pago" ,
  v."seguimiento_de_pagos" ,
  v."condicion_actual_del_credito" ,
  v."detalle_de_condicion" ,
  v."condicion_asignada" ,
  v."causal_de_cancelacion" ,
  v."medio_de_cancelacion" ,
  v."puntualidad" ,
  v."moneda" ,
  v."fondo" ,
  v."estado_de_prestamo"
  from prod_datalake_master.bd_pagos__bd_pagos as v
  left join (select "codigo_operacion" from finalizados) as f
  on v."codigo_operacion" = f."codigo_operacion"
  where f."codigo_operacion" is null

), pagos_unidos as (       
select 
    *
from vigentes as v
    union all       
select * from finalizados as f
    
), pagos_cols as (

select 
/*
pu."codigo_operacion" as a,
pu."codigo_contrato" as b, 
pu."cuota_esperada_mensual" as c,
pu."fecha_de_pago_esperada_original" as d,
pu."saldo_por_cancelar" as e,
pu."interes_esperado_original" as f,
pu."amortizacion_esperada_original" as g,
pu."saldo_por_cancelar_esperado_actualizada" as h,
pu."fecha_de_pago_del_cliente" as i,
pu."monto_total_pagado_al_credito" as j,
pu."capital_pagado" as k,
pu."interes_pagado" as l,
pu."dias_de_atraso_de_pago" as m,
pu."condicion_actual_del_credito" as n,
pu."estado_de_prestamo" as o,
pu."moneda" as p,
*/
case when pu."moneda" = 'SOLES' THEN pu."saldo_por_cancelar" 
ELSE pu."saldo_por_cancelar"*tc.exchange_rate END AS "saldo_por_cancelar_soles",

case when pu."moneda" = 'SOLES' THEN pu."saldo_por_cancelar_esperado_actualizada" 
ELSE pu."saldo_por_cancelar_esperado_actualizada"*tc.exchange_rate END AS "saldo_por_cancelar_esperado_actualizada_soles",

case when pu."moneda" = 'SOLES' THEN pu."monto_total_pagado_al_credito" 
ELSE pu."monto_total_pagado_al_credito"*tc.exchange_rate END AS "monto_total_pagado_al_credito_soles",

case when pu."moneda" = 'SOLES' THEN pu."capital_pagado"
ELSE pu."capital_pagado"*tc.exchange_rate END AS "capital_pagado_soles",

case when pu."moneda" = 'SOLES' THEN pu."interes_pagado"
ELSE pu."interes_pagado"*tc.exchange_rate END AS "interes_pagado_soles",



case when pu."moneda" = 'SOLES' THEN pu."monto_total_pagado_al_credito" 
ELSE coalesce(pu."monto_total_pagado_al_credito", pu."cuota_esperada_mensual")* 3.8 END AS "monto_total_pagado_al_credito_38",
case when pu."moneda" = 'SOLES' THEN pu."capital_pagado"
ELSE coalesce(pu."capital_pagado", pu."amortizacion_esperada_original")*3.8 END AS "capital_pagado_38",
case when pu."moneda" = 'SOLES' THEN pu."interes_pagado"
ELSE coalesce(pu."interes_pagado", pu."interes_esperado_original")*3.8 END AS "interes_pagado_38",



case when pu."moneda" = 'SOLES' THEN pu."cuota_esperada_mensual" 
ELSE pu."cuota_esperada_mensual"* 3.8 END AS "cuota_esperada_mensual_38",
case when pu."moneda" = 'SOLES' THEN pu."amortizacion_esperada_original"
ELSE pu."amortizacion_esperada_original"*3.8 END AS "amortizacion_esperada_original_38",
case when pu."moneda" = 'SOLES' THEN pu."interes_esperado_original"
ELSE pu."interes_esperado_original"*3.8 END AS "interes_esperado_original_38",




date_add('day',-1,date_trunc('month', date_add('month', 1, pu."fecha_de_pago_del_cliente"))) as mes_pago,
cast(date_format(pu."fecha_de_pago_del_cliente", '%Y%m') as int) as codmes_pago,
tc.exchange_rate as exchange_rate_realized_payment,
tc2.exchange_rate as exchange_rate_proforma_payment,
pu.* 
from pagos_unidos as pu 

left join prod_datalake_analytics.tipo_cambio_jmontoya as tc
on cast(tc.tc_codmes as int) = cast(date_format(pu."fecha_de_pago_del_cliente", '%Y%m') as int)

left join prod_datalake_analytics.tipo_cambio_jmontoya as tc2
on cast(tc2.tc_codmes as int) = cast(date_format(pu."fecha_de_pago_esperada_original", '%Y%m') as int)


), fecha_finalizacion as (

SELECT
    codigo_operacion as "codigo_prestamo",
    max(fecha_de_pago_del_cliente) as "fecha_finalizacion",
    date_add('day',-1,date_trunc('month', date_add('month', 1, max(fecha_de_pago_del_cliente))) ) as "mes_finalizacion"
    
FROM pagos_unidos
GROUP BY codigo_operacion
HAVING
    SUM(CASE WHEN estado_de_prestamo is null or estado_de_prestamo <> 'FINALIZADO' THEN 1 ELSE 0 END) = 0
)
select 

    *,
    last_day_of_month(cast("fecha_de_pago_esperada_original" as date)) as mes_pago_teorico,
    last_day_of_month(cast("fecha_de_pago_del_cliente" as date)) as mes_pago_real

from pagos_cols
where codigo_contrato in ('P02175',
                        'P02483',
                        'P02639',
                        'P02677',
                        'P02733',
                        'P02773',
                        'P02791',
                        'P02809',
                        'P02949',
                        'P03045',
                        'P03049',
                        'P03501',
                        'P03570',
                        'P03419')
and condicion_actual_del_credito != 'FINALIZADO'

'''
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
df_pagos = pd.DataFrame(resultados, columns = column_names)

df_pagos_reales = df_pagos.pivot_table(index   = 'mes_pago_real',
                                       values  = ["monto_total_pagado_al_credito_38", "capital_pagado_38", "interes_pagado_38"],
                                       aggfunc = 'sum').reset_index()

df_pagos_teoricos = df_pagos.pivot_table(index   = 'mes_pago_teorico',
                                         values  = ["cuota_esperada_mensual_38", "amortizacion_esperada_original_38", "interes_esperado_original_38"],
                                         aggfunc = 'sum').reset_index()

#%% tasación LTV
query = '''
with primera_fecha as (   
select 
    codigo_contrato
    ,min(cast(fecha_desembolso as date)) as fecha_desembolso
from prod_datalake_master.bd_operaciones__operaciones
group by codigo_contrato

), ops_originales as (

select 
    bdo.*,
    cast(last_day_of_month(cast(bdo.fecha_desembolso as date)) as date) as eom_fecha_desembolso,
    case when moneda = 'DOLARES' then cast(tasa_de_cambio as double)*cast(monto_prestamo_recibido as double) else monto_prestamo_recibido end as monto_financiado_sol
from  prod_datalake_master.bd_operaciones__operaciones as bdo
left join primera_fecha as pf  
on (bdo.codigo_contrato = pf.codigo_contrato) and (cast(bdo.fecha_desembolso as date) = pf.fecha_desembolso)

), monto_financiado as (     

select
    codigo_contrato,
    sum(monto_financiado_sol) monto_financiado_sol,
    max(moneda) moneda,
    avg(cast(tasa_de_cambio as double)) tasa_de_cambio
from ops_originales
group by codigo_contrato

), tasacion as (

select 
    max(coalesce(inm.tasacion_aprobada__dolares_,inm.tasacion_aprobada__dolares__borrador,inm.tasacion_aprobada__dolares__borrador2,inm.tasacion_preliminar__dolares_)) tasacion_aprobada__dolares_,
    hd.codigo_de_contrato
    
 from prod_datalake_master.hubspot__inmueble as inm
left join (select * from prod_datalake_master."hubspot__associations" where type = 'deal_to_inmueble') as aso
on inm.hs_object_id = aso.hs_object_id_2
left join prod_datalake_master.hubspot__deal as hd
on hd.hs_object_id = aso.hs_object_id_1

where hd.codigo_de_contrato is not null
group by hd.codigo_de_contrato

)
select
    mf.*,
    t.tasacion_aprobada__dolares_,
    t.tasacion_aprobada__dolares_*3.6 as tasacion_aprobada__soles_,
    MF.monto_financiado_sol / (t.tasacion_aprobada__dolares_*3.6) AS LTV
from monto_financiado as mf
left join tasacion as t
on t.codigo_de_contrato = mf.codigo_contrato

where t.codigo_de_contrato in ('P02175',
                        'P02483',
                        'P02639',
                        'P02677',
                        'P02733',
                        'P02773',
                        'P02791',
                        'P02809',
                        'P02949',
                        'P03045',
                        'P03049',
                        'P03501',
                        'P03570',
                        'P03419')

'''

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
df_ltv = pd.DataFrame(resultados, columns = column_names)

ltv_total = df_ltv['monto_financiado_sol'].sum() / df_ltv['tasacion_aprobada__soles_'].sum()
print(ltv_total)

#%%



