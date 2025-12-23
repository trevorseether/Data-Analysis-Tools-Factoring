# -*- coding: utf-8 -*-
"""
Created on Mon Nov  3 09:42:04 2025

@author: Joseph Montoya
"""

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

#%%
fecha_ = pd.Timestamp('2025-10-21')
limit_ = pd.Timestamp('2025-12-20')

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
query = ''' 

with onlines as (
SELECT 
    fr.code,
    array_join(array_agg(ii.code), ', ') AS facturas,
    'online' as flag
    --max(fr.product) as prod
FROM prod_datalake_analytics.im_invoices AS ii
LEFT JOIN prod_datalake_analytics.fac_requests AS fr
    ON fr._id = ii.request_id
WHERE ii.request_id IS NOT NULL
and fr.code not in ('rejected', 'canceled')

  --AND fr.code = 'KVuW7d5S'
GROUP BY fr.code
),
offlines as (
select
    dealname,
    facturas,
    'off' as flag
    --tipificacion_operativa
from prod_datalake_master.hubspot__deal
where pipeline = '14026011' -- Prestamype - Factoring
and facturas is not null

and dealstage not in ('14026018','14026016')

and dealname not in (select code from prod_datalake_analytics.fac_requests)

)
, unidos as (
select * from onlines
union all
select * from offlines where dealname not in (select code from onlines)

)

select 
    u.*,
    case when lower(fr.product) is not null then lower(fr.product)
        else hd.tipo_de_operacion
        end as producto,
    case 
        when lower(hd.tipificacion_operativa) = 'adelanto' then 'adelanto'
        when lower(fr.product) is not null then lower(fr.product)
        else lower(hd.tipificacion_operativa)
end as tipificacion_operativa

from unidos as u
left join prod_datalake_analytics.fac_requests as fr on u.code = fr.code
left join (select * from prod_datalake_master.hubspot__deal where pipeline = '14026011' and facturas is not null and lower(tipificacion_operativa)= 'adelanto' AND DEALSTAGE not in ('14026018','14026016')) as hd on hd.dealname = u.code


'''

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
datos = pd.DataFrame(resultados, columns = column_names)

datos['facturas'] = datos['facturas'].str.strip()

datos = datos.sort_values(by="tipificacion_operativa", ascending=True)

datos = datos.drop_duplicates(subset=["code"], keep="first")

aver = datos[datos.duplicated(subset=['code'], keep=False)]

###################################################################################################

datos.loc[datos['code'] == 'LUCARBAL.11.12.2024', 'facturas'] =  'F101-4026, F101-4029'
datos.loc[datos['code'] == 'JJA.31.12.2024',      'facturas'] =  'E001-7209, E001-7210'
datos.loc[datos['code'] == 'HENRRY04.12.2024',    'facturas'] =  'E001-45'
datos.loc[datos['code'] == 'GEMCO11.12.2024',     'facturas'] =  'E001-85'

#%% amplificación por factura
datos['facturas'] = datos['facturas'].str.strip()
datos['facturas'] = datos['facturas'].str.replace('\n', ',')  # reemplaza saltos de línea por coma
datos['facturas'] = datos['facturas'].str.replace('\r', ',')  # por si vienen con retorno de carro (Windows)
datos['facturas'] = datos['facturas'].str.replace('|', ',')  # por si vienen con retorno de carro (Windows)
datos['facturas'] = datos['facturas'].str.replace(';', ',')  # por si vienen con retorno de carro (Windows)
datos['facturas'] = datos['facturas'].str.replace(',+', ',', regex=True)  # limpia comas duplicadas
datos['facturas'] = datos['facturas'].str.strip()


datos['facturas'] = datos['facturas'].str.split(',')
datos = datos.explode('facturas')
datos['facturas'] = datos['facturas'].str.strip()

#%%%
query = """ --- TABLA PRIORITARIA,información que se llena desde diciembre 2024
SELECT
    code AS "Código de Subasta",
    date(interest_proforma_disbursement_date) AS "fecha"

FROM prod_datalake_analytics.fac_requests
    WHERE STATUS = 'closed'
    AND interest_proforma_disbursement_date IS NOT NULL
"""
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()
# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]
# Convertir los resultados a un DataFrame de pandas
FECHA_FOTO1 = pd.DataFrame(resultados, columns = column_names)

FECHA_FOTO1['fecha'] = pd.to_datetime(FECHA_FOTO1['fecha'])
########################################################################################FECHA
# FECHA_FOTO1['Código de Subasta'] = FECHA_FOTO1['Código de Subasta'].str.lower()
# fecha de desembolso real desde el lake
query = """ --- para llenar vacías
select

    dealname                                AS "Código de Subasta",
    date(fecha_de_desembolso__factoring_)   AS "Fecha Mínima de Desembolso"

from prod_datalake_master.hubspot__deal
where fecha_de_desembolso__factoring_ is not null
and dealstage not in ('14026016' , '14026018')
-- no se deben considerar ni rechazados ni cancelados(subasta desierta) porque no tienen fecha de desembolso y generan duplicidad

"""
# SELECT
#     c_digo_de_subasta,
#     MIN(fecha_de_desembolso_registrado) AS "Fecha Mínima de Desembolso"
# FROM
#     prod_datalake_master.hubspot__pagos_facturas
# WHERE
#     hs_pipeline_stage = 89540624
#     AND fecha_de_desembolso_registrado IS NOT NULL
#     AND fecha_de_desembolso_registrado > DATE('2024-08-31')
# GROUP BY
#     c_digo_de_subasta
# ORDER BY
#     c_digo_de_subasta
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()
# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]
# Convertir los resultados a un DataFrame de pandas
desem = pd.DataFrame(resultados, columns = column_names)

desem = desem[ ~pd.isna(desem["Fecha Mínima de Desembolso"]) ]
desem = desem[ ~pd.isna(desem["Código de Subasta"]) ]
desem.drop_duplicates(subset = "Código de Subasta", inplace = True)
# desem['Código de Subasta'] = desem['Código de Subasta'].str.lower()

# UNIÓN
df = datos.copy()
df.rename(columns = {'code': 'Código de Subasta'}, inplace = True)
df = df.merge(FECHA_FOTO1,
              on  = 'Código de Subasta',
              how = 'left')
df = df.merge(desem,
              on  = 'Código de Subasta',
              how = 'left')

print(df.shape)
def f_d_p(df):
    if not pd.isna(df['fecha']):
        return df['fecha']
    elif not pd.isna(df['Fecha Mínima de Desembolso']):
        return df['Fecha Mínima de Desembolso']

df['Fecha de Desembolso Proveedor'] = df.apply(f_d_p, axis = 1)
df['Fecha de Desembolso Proveedor'] = pd.to_datetime(df['Fecha de Desembolso Proveedor']).dt.normalize()

sin_fecha_desembolso = df[pd.isna(df['Fecha de Desembolso Proveedor'])]
if sin_fecha_desembolso.shape[0] > 0:
    print(f"alerta, hay {sin_fecha_desembolso.shape[0]} casos sin fecha de desembolso")

del df['fecha']
del df['Fecha Mínima de Desembolso']

##### parchamiento puntual de datos ############################################
# retirar una vez modificado en la base de datos
df.loc[df['Código de Subasta'].str.lower() == 'itw43msi', 'Fecha de Desembolso Proveedor'] = pd.Timestamp('2025-09-29')

df['Fecha de Desembolso Proveedor'] = pd.to_datetime(df['Fecha de Desembolso Proveedor']).dt.normalize()

#%%
# [10] Columna FECHA DE CIERRE FINAL (fecha en que se finaliza la operación porque la pagan completa)
####### query principal ###############################################################
query = '''   ------- query para cambio, REALIZAR COMPARACIONES
SELECT
    max(a."date") AS "Fecha de Cierre Final",
    b.code        AS "Código de Subasta"

FROM prod_datalake_analytics.fac_client_payment_payments as a
left join prod_datalake_analytics.fac_client_payments as c
on a.client_payment_id = c._id
left join prod_datalake_analytics.fac_requests  as b
on c.request_id = b._id

where b.status = 'closed'
and c.status = 'finalizado'
    group by b.code
'''
cursor = conn.cursor()
cursor.execute(query)
# Obtener los resultados
resultados = cursor.fetchall()
# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]
# Convertir los resultados a un DataFrame de pandas
fecha_cierre = pd.DataFrame(resultados, columns = column_names)
fecha_cierre["Fecha de Cierre Final"] = fecha_cierre["Fecha de Cierre Final"].dt.normalize()

df = df.merge(fecha_cierre,
              on  = "Código de Subasta",
              how = 'left')
print(df.shape)
###### complementando información de cierre final de la operación (momento en que se pague la subasta)
query = """
SELECT
    hs_object_id,
    subject,
    tipo_de_pago,
    closed_date AS "FECHA_PAGO_HUB_TICKETS", --<<<<<<<<<<
    hs_pipeline_stage,
    hs_pipeline
FROM prod_datalake_master.hubspot__ticket    --- según cobranzas, esta es la fecha en la que el deudor termina de pagar
WHERE hs_pipeline = '26417284'  --- cobranzas factoring/confirming

"""
cursor = conn.cursor()
cursor.execute(query)
# Obtener los resultados
resultados = cursor.fetchall()
# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]
# Convertir los resultados a un DataFrame de pandas
fecha_cierre = pd.DataFrame(resultados, columns = column_names)
fecha_cierre["FECHA_PAGO_HUB_TICKETS"] = fecha_cierre["FECHA_PAGO_HUB_TICKETS"].dt.normalize()
fecha_cierre = fecha_cierre[ ~fecha_cierre['subject'].isin(['PRUEBA', 'PRUEBA - 1', 'PRUEBA123', 'PRUEBA123 - 1']) ]
# # fecha_cierre['subject'] = fecha_cierre['subject'].str.lower()
dups_cierre_hub_ticket = fecha_cierre[ fecha_cierre.duplicated(subset=['subject']) ]
if dups_cierre_hub_ticket.shape[0] > 0:
    print('alerta duplicados en fecha de pago de hubspot tickets')
fecha_cierre.drop_duplicates(subset='subject', inplace = True)

df = df.merge(fecha_cierre[['subject', 'FECHA_PAGO_HUB_TICKETS']],
              left_on  = 'Código de Subasta',
              right_on = 'subject',
              how = 'left')
del df['subject']

def ajuste_fecha_cierre_final(df):
    if pd.isna(df["Fecha de Cierre Final"]):
        return df['FECHA_PAGO_HUB_TICKETS']
    else:
        return df["Fecha de Cierre Final"]
df["Fecha de Cierre Final"] = df.apply(ajuste_fecha_cierre_final, axis = 1)

del df['FECHA_PAGO_HUB_TICKETS']

#%%
# fecha_ = pd.Timestamp('2024-10-20')
df_filtrado = df[ (df["Fecha de Cierre Final"] >= fecha_) & (df["Fecha de Cierre Final"] <= limit_)]

#%%
query = ''' 

select code, company_ruc  from prod_datalake_master.base_tiempo_real_1

'''

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
rucs = pd.DataFrame(resultados, columns = column_names)

#%%
df_filtrado = df_filtrado.merge(rucs,
                                left_on  = 'Código de Subasta',
                                right_on = 'code',
                                how      = 'left')

df_filtrado = df_filtrado[ ~pd.isna(df_filtrado['company_ruc'])]

#%%
df_filtrado['TIPO_COMPROBANTE'] = '01'

df_filtrado['RUC_PROVEEDOR'] = df_filtrado['company_ruc']

df_filtrado['SERIE'] = df_filtrado['facturas'].str.split('-').str[0]
df_filtrado['NUMERACION'] = df_filtrado['facturas'].str.split('-').str[1]

df_filtrado['NRO_AUTORIZACION'] = ''

df_filtrado['NRO_CUOTA'] = '0'

df_filtrado['FECHA_EFECTIVA_PAGO'] = df_filtrado['Fecha de Cierre Final'].dt.strftime('%d/%m/%Y')

df_filtrado['PARTICIPANTE_ORIGEN'] = '856'
 
#%%%
df_final = df_filtrado[['TIPO_COMPROBANTE','RUC_PROVEEDOR','SERIE','NUMERACION',
                        'NRO_AUTORIZACION','NRO_CUOTA','FECHA_EFECTIVA_PAGO','PARTICIPANTE_ORIGEN','Código de Subasta']]

df_final['_t'] = pd.to_datetime(df_final['FECHA_EFECTIVA_PAGO'], dayfirst= True)
df_final = df_final.sort_values( by = ['_t', 'Código de Subasta'], ascending = True)

del df_final['_t']
#%%
os.chdir(r'C:\Users\Joseph Montoya\Desktop\pruebas\redencion de facturas')

df_final.to_excel(f'RM-RD {str(fecha_)[0:10]}.xlsx', index = False)

#%%
print('fin')

