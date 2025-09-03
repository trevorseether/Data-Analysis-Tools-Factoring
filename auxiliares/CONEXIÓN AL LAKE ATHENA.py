# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 15:28:45 2025

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
query = ''' select  * from prod_datalake_analytics.fac_outst_unidos_f_desembolso_jmontoya '''


''' 
select
    max(A.provider_name)        as "customer name"
    ,max(b.sectores_economicos) as "industry"
    ,''                         as "adress"
    ,A.provider_ruc             AS "TAX ID"
    ,A.currency_auctions        as "moneda"
    ,avg(a.terms)               as "payment terms"
    ,sum(A.remaining_capital) - sum(a.par1_m)   as "unpaid invoices not due"
    ,sum(case when dias_atraso between 1 and 30 then a.remaining_capital
    else 0 end) as "1 a 30"
    ,sum(case when dias_atraso between 31 and 60 then a.remaining_capital
    else 0 end) as "31 a 60"
    ,sum(case when dias_atraso between 61 and 90 then a.remaining_capital
    else 0 end) as "61 a 90"
    ,sum(case when dias_atraso between 91 and 120 then a.remaining_capital
    else 0 end) as "91 a 120"
    ,sum(par120_m)
    ,sum(A.remaining_capital)   as "Total Accounts Receivable"

from prod_datalake_analytics.fac_outst_unidos_jmontoya  AS A
LEFT JOIN prod_datalake_master.hubspot__company AS B 
ON A.provider_ruc = CAST(CAST(b.ruc AS decimal(20,0)) AS varchar)

where codmes = '202506'

group by provider_ruc, currency_auctions







'''





'''

select
    code,
    closed_at,
    case when proforma_strategy_name = 'factoring-v1-new' then round(proforma_profit_interest_rate*(0.9),8)
    else round(proforma_financing_interest_rate*(0.9),8)
    end as "tasa efectiva",
    proforma_simulation_currency
    
--                      select *
from prod_datalake_analytics.fac_requests 
--where proforma_strategy_name not in ('factoring-v1-new')
where status = 'closed'
and closed_at >= date '2024-01-01'

 '''



'''

SELECT 
json_extract_scalar(b._c1, '$.auction.code')  AS auction_code,
json_extract_scalar(b._c1, '$.pay_order_businessman_id["$oid"]') as codigo_deposito,
json_extract_scalar(a._c1, '$.concept') as concept_deposito,
json_extract_scalar(b._c1, '$.real_amount') as monto_transferencia,
json_extract_scalar(b._c1, '$.type_transfer') as tipo_transferencia,
json_extract_scalar(b._c1, '$.code_real') as codigo_transferencia,
date_format(from_unixtime((CAST(json_extract_scalar(b._c1, '$.created_at["$date"]') AS bigint) / 1000)), '%Y-%m-%d %H:%i:%s') as fecha_transferencia
FROM "prod_datalake_master"."prestamype__fac_pay_order_businessmen" a 
left join "prod_datalake_master"."prestamype__fac_pay_order_businessman_details" b on a._c0 =json_extract_scalar(b._c1, '$.pay_order_businessman_id["$oid"]')
where json_extract_scalar(b._c1, '$.status') != 'rechazado'
AND json_extract_scalar(b._c1, '$.auction.code') in ('l1h2k7mu') --('V0EBfe2B')
order by json_extract_scalar(b._c1, '$.updated_at["$date"]') desc
'''

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
dotos2 = pd.DataFrame(resultados, columns = column_names)

dotos2.to_excel(r'C:\Users\Joseph Montoya\Desktop\pruebas\fac ou.xlsx',
                index = False)
# duplicados = mayo[mayo.duplicated(subset=['code'], keep=False)]
# dups_tick = dotos2[dotos2.duplicated(subset=['subject'], keep=False)]
# dotos2.to_excel(r'C:\Users\Joseph Montoya\Desktop\pruebas\NUEVOS PROVEEDORES.xlsx')

# pivoteado = dotos2.pivot_table(index   = 'codmes',
#                                values  = 'remaining_capital_soles',
#                                aggfunc = 'sum').reset_index()


#%%
df_doto_auxiliar_copia = dotos.copy()
#%%
df_pivoteado = df_doto_auxiliar_copia.pivot_table(values  = 'codigo_deposito',
                                                  index   = 'auction_code',
                                                  aggfunc = 'count').reset_index()

unos = df_pivoteado[df_pivoteado['codigo_deposito'] == 2]

unos_aver = df_doto_auxiliar_copia[df_doto_auxiliar_copia ['auction_code'].isin(list(unos['auction_code'])) ]
conteo = df_pivoteado.pivot_table(values = 'auction_code',
                                  index = 'codigo_deposito',
                                  aggfunc = 'count').reset_index()

conteo_ult_adel = unos_aver.pivot_table(index = 'concept_deposito',
                                        values = 'auction_code',
                                        aggfunc = 'count')

#%%% resultado final

final = df_doto_auxiliar_copia[df_doto_auxiliar_copia['concept_deposito'] != 'Ultimo']

final['f fact'] = pd.to_datetime(    final['fecha_transferencia']     )

final2 = final.pivot_table(index = 'auction_code',
                           values = 'f fact',
                           aggfunc = 'max').reset_index()
import os
os.chdir(r'C:\Users\Joseph Montoya\Desktop\pruebas')
final2.to_excel('f fact final.xlsx', index = False)

#%% investigando los últimos:
ults = df_doto_auxiliar_copia[df_doto_auxiliar_copia['concept_deposito'] == 'Ultimo']
ult_pivot = ults.pivot_table(values  = 'codigo_deposito',
                             index   = 'auction_code',
                             aggfunc = 'count').reset_index()


#%%

import os
os.chdir(r'C:\Users\Joseph Montoya\Desktop\pruebas')

dotos['fecha facturacion'] = pd.to_datetime(dotos['created_at_max'])

dotos.to_excel('f fact 2.xlsx',
               index = False)














