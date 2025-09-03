# -*- coding: utf-8 -*-
"""
Created on Tue Aug  5 18:26:34 2025

@author: Joseph Montoya
"""

import pandas as pd
import os

#%%%
datos = pd.read_excel(r'C:/Users/Joseph Montoya/Downloads/bbdd tiempo real (script 05-08-2025).xlsx',
                      dtype = {'RUC Proveedor' : str,
                               'RUC Cliente' : str})

datos = datos[[
    'Código de Subasta',
    'Fecha de Desembolso Proveedor',
    'RUC Proveedor',
      'Proveedor',
      'RUC Cliente',
      'Cliente',
      'Moneda',
      'Monto Financiado',
      'Monto Financiado en Soles',
      
      'Comisión de Estructuración',
      'Tipo de Producto']]

datos = datos[ datos['Fecha de Desembolso Proveedor'].dt.year == 2024 ]

#%%
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
dotos2 = pd.DataFrame(resultados, columns = column_names)

dotos2['facturas'] = dotos2['facturas'].str.strip()

dotos2 = dotos2.sort_values(by="tipificacion_operativa", ascending=True)

dotos2 = dotos2.drop_duplicates(subset=["code"], keep="first")

aver = dotos2[dotos2.duplicated(subset=['code'], keep=False)]

#%% query de los comprobantes de comisión de estructuración emitidos
query = ''' 
select 
    a.code as codigo_subasta,
    b.code as factura_comision,
    b.created_at as fecha_emision

from
prod_datalake_analytics.fac_requests a
left join (select * from prod_datalake_analytics.view_prestamype_fac_cpe 
           where concept='commission-factoring') b
on a._id=b.request_id

where a.status not in  ('rejected', 'canceled')
and b.code is not null


'''
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
facturas1 = pd.DataFrame(resultados, columns = column_names)

###############################################################################
offlines = pd.read_excel(r'G:/Mi unidad/Gestión de Comprobantes Factoring.xlsx',
                         sheet_name = 'Offline')

offlines = offlines[['Subasta OFFLINE', 'Factura Comisión']] # columnas necesarias
offlines['Factura Comisión'] = offlines['Factura Comisión'].str.strip() # limpieza de espacios
offlines = offlines[ ~pd.isna(offlines['Factura Comisión']) ] # eliminación de nulos
offlines = offlines[ offlines['Factura Comisión'] != 'No facturar' ] # eliminación de texto innecesario
offlines = offlines.rename(columns={
                                "Subasta OFFLINE": "codigo_subasta",
                                "Factura Comisión": "factura_comision"
                                    })
offlines['fecha_emision'] = ''

########## facturas emitidas por comisión de estructuración ###################
df_facturas = pd.concat([facturas1, offlines], ignore_index=True)
df_facturas['fecha_emision'] = pd.to_datetime(df_facturas['fecha_emision'])

df_facturas = df_facturas.drop_duplicates(subset=["codigo_subasta"], keep="first")

#%%
datos = datos.merge(dotos2,
                    left_on = 'Código de Subasta',
                    right_on = 'code',
                    how = 'left')

dotos2['tipificacion_operativa'].unique()

# import numpy as np
# datos['Tipo de Producto'] = np.where(datos['tipificacion_operativa'] == 'Adelanto',
#                                      'Adelanto',
#                                      datos['Tipo de Producto'])
aver = datos[datos.duplicated(subset=['Código de Subasta'], keep=False)]

dup_id = datos[datos.duplicated(subset=['Código de Subasta'], keep=False)]
if dup_id.shape[0] >0:
    print('alerta de duplicados')

#%% unión con df_facturas (son facturas de comision)
datos = datos.merge(df_facturas,
                    left_on  = 'Código de Subasta',
                    right_on = 'codigo_subasta',
                    how      = 'left')
del datos['codigo_subasta']

datos['factura emitida por comisión de estructuración'] = datos['factura_comision']
datos['fecha emision factura comisión de estructuración'] = datos['fecha_emision']
#%%
datos['RUC de la entidad informante'] = '20606100893'
datos['Tipo de documento crediticio'] = 'Factura'
datos['Tipo de desembolso'] = 'Depósito en Cuenta'

datos.loc[datos['Código de Subasta'] == 'LUCARBAL.11.12.2024', 'facturas'] =  'F101-4026, F101-4029'
datos.loc[datos['Código de Subasta'] == 'JJA.31.12.2024',      'facturas'] =  'E001-7209, E001-7210'
datos.loc[datos['Código de Subasta'] == 'HENRRY04.12.2024',    'facturas'] =  'E001-45'
datos.loc[datos['Código de Subasta'] == 'GEMCO11.12.2024',     'facturas'] =  'E001-85'

datos = datos[[
               'RUC de la entidad informante',
               'RUC Proveedor',
               'Proveedor',
               'RUC Cliente',
               'Cliente',
               'Tipo de documento crediticio',
               'facturas',
'Moneda',
'Monto Financiado',
'Monto Financiado en Soles',   
'Tipo de desembolso',  

'Comisión de Estructuración',          
               'Código de Subasta',
               'Tipo de Producto',
               'tipificacion_operativa',
               
               'factura emitida por comisión de estructuración',
               
               'Fecha de Desembolso Proveedor',
               'fecha emision factura comisión de estructuración'
               ]]

#%%
datos.to_excel(r'C:\Users\Joseph Montoya\Desktop\pruebas\desembolsos 2024 nuevito.xlsx', index = False)


