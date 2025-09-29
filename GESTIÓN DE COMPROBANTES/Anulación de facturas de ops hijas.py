# -*- coding: utf-8 -*-
"""
Created on Mon Sep 22 09:54:31 2025

@author: Joseph Montoya
"""


import pandas as pd
from pyathena import connect
import json
import numpy as np
from datetime import datetime

import warnings
warnings.filterwarnings("ignore")

#%%
ubi = r'C:\Users\Joseph Montoya\Desktop\notas de crédito y débito\eliminación de facturas de ops hijas'
#%% Credenciales de AmazonAthena
with open(r"C:/Users/Joseph Montoya/Desktop/credenciales actualizado.txt") as f:
    creds = json.load(f)

conn = connect(
    aws_access_key_id     = creds["AccessKeyId"],
    aws_secret_access_key = creds["SecretAccessKey"],
    aws_session_token     = creds["SessionToken"],
    s3_staging_dir        = creds["s3_staging_dir"],
    region_name           = creds["region_name"]
    
    )

#%% obtenemos operaciones con 
query = ''' 
with ops_y_facturas as (        
    select 
        fr1.code as subasta,
        cpe1.code as factura_madre
    from prod_datalake_analytics.fac_requests as fr1
    left join (select * from prod_datalake_analytics."view_prestamype_fac_cpe" where concept = 'interest-factoring') as cpe1
    on cpe1.request_id = fr1._id

), filtrado as (        
select 
    fr.proforma_simulation_warranty_zero_status, 
    fr.code,
    fr.status,
    cpe.code as fact_cost_financiamiento,
    fr.created_at,
    fr.interest_proforma_disbursement_date,
    fr.interest_proforma_simulation_financing_cost_value,
    
    mh.hija as operacion_hija,
    mh.madre as operacion_madre,
    oyf.factura_madre as factura_de_la_madre,
    
    fr.company_ruc as Ruc_proveedor,
    fr.company_name as Razon_Social,
    fr.proforma_simulation_currency

from prod_datalake_analytics.fac_requests as fr
left join (select * from prod_datalake_analytics."view_prestamype_fac_cpe" where concept = 'interest-factoring') as cpe
on fr._id = cpe.request_id

left join (select * from prod_datalake_master.ba__fac_madres_hijas where hija is not null) as mh
on mh.hija = fr.code

left join ops_y_facturas as oyf
on oyf.subasta = mh.madre

where fr.proforma_simulation_warranty_zero_status = True
and cpe.code is not null
and mh.hija is not null
)
select * from filtrado

'''

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
df_facturas_hijas = pd.DataFrame(resultados, columns = column_names)

df_facturas_hijas = df_facturas_hijas[ ~pd.isna(df_facturas_hijas['factura_de_la_madre']) ]

df = df_facturas_hijas[ ~pd.isna(df_facturas_hijas['interest_proforma_simulation_financing_cost_value']) ]

df = df[ ~df['code'].isin(['dgOrbr8w', 
                           'xUzKpddU',
                           'UAt2W4JI',
                           'tN5KmEdE',
                           'IUEExsEn',
                           '9gR9ER8z',
                           'VvQCylSz'])] # anulaciones ya realizadas

#%% aquí poner filtrado de operaciones que ya se hayan emitido

#%% estructura para Tandia
# 1 Hoja Comprobantes
df_comprobantes = pd.DataFrame()

df_comprobantes['aux1'] = df['code']
df_comprobantes['aux2'] = df['interest_proforma_simulation_financing_cost_value']
df_comprobantes['aux3'] = df['fact_cost_financiamiento']
df_comprobantes['aux4'] = df['Ruc_proveedor']
df_comprobantes['aux5'] = df['Razon_Social']
df_comprobantes['aux6'] = df['proforma_simulation_currency']

df_comprobantes['Grupo'] = np.arange(1, len(df_comprobantes) + 1)
df_comprobantes['Serie'] = 'FFC3'
df_comprobantes['Correlativo'] = ''
df_comprobantes['Tipo de nota'] = 'Crédito'
df_comprobantes['Fecha de emisión'] = pd.to_datetime(datetime.now()).normalize()
df_comprobantes["Fecha de emisión"] = (df_comprobantes["Fecha de emisión"].dt.strftime("%Y-%m-%d").astype("string"))
df_comprobantes['Motivo'] = 'Anulación de la operación'
df_comprobantes['Comprobante Relacionado'] = df_comprobantes['aux3']
df_comprobantes['Fecha Comprobante Relacionado'] = ''
df_comprobantes['Tipo de doc. Cliente'] = 'RUC'
df_comprobantes['N° de doc. Cliente'] = df_comprobantes['aux4']
df_comprobantes['Nombre cliente'] = df_comprobantes['aux5']
df_comprobantes['Correo cliente'] = ''
df_comprobantes['Moneda'] = df_comprobantes['aux6']

del df_comprobantes['aux1']
del df_comprobantes['aux2']
del df_comprobantes['aux3']
del df_comprobantes['aux4']
del df_comprobantes['aux5']
del df_comprobantes['aux6']

#%% 2 Hoja Items
df_items = pd.DataFrame()

df_items['aux1'] = df['code']
df_items['aux2'] = df['interest_proforma_simulation_financing_cost_value']

df_items['Grupo'] = np.arange(1, len(df_items) + 1)
df_items['Código del item'] = df_items['aux1']
df_items['Descripción del item'] = 'Descuento por operación de Factoring en referencia el Contrato Empresario. El descuento es realizado por Factoring Prestamype.'
df_items['Unidad del item'] = 'ZZ'
df_items['Cantidad del item'] = '1'
df_items['Precio del item'] = df_items['aux2'].abs()
df_items['Impuesto'] = 'INA'
df_items['Gratuito'] = 'No'
df_items['ICBPER'] = 'No'

del df_items['aux1']
del df_items['aux2']

#%%

hoy_formateado = datetime.today().strftime('%d-%m-%Y')

with pd.ExcelWriter(ubi + f'\\Carga_masiva_Anulación de Facturas OPS hijas {hoy_formateado}.xlsx', engine='xlsxwriter') as writer:
    df_comprobantes.to_excel(writer, index=False, sheet_name='Comprobantes')
    df_items.to_excel(writer, index=False, sheet_name='Items')
