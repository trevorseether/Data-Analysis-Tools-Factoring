# -*- coding: utf-8 -*-
"""
Created on Mon Apr 28 12:27:02 2025

@author: Joseph Montoya
"""

import pandas as pd
from pyathena import connect

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
SELECT      
    c.customer_document_type
    , c.customer_document_value
    , c.customer_email
    , c.customer_last_name
    , c.customer_name
    , A.CODE
    , a.closed_at
    , a.proforma_simulation_financing_total as monto_financiado
    , c.amount
    , a.proforma_simulation_currency
    , CASE
        WHEN c.customer_email LIKE 'gestora@prestamype.com' then 'GESTORA'
        WHEN c.customer_email LIKE 'factoringfinanzas@prestamype.com' then 'ONBALANCE'
        ELSE 'CROWD'
        END "TIPO"
        
    , (CASE WHEN (A.product = 'factoring') THEN A.proforma_simulation_financing_total ELSE A.proforma_simulation_financing END) Monto_Financiado

 FROM prod_datalake_analytics.fac_bids AS c
 LEFT JOIN prod_datalake_analytics.fac_requests AS A
 ON A._ID = C.request_id
 
 where c.status = 'ganado'
 
 '''

'gestora@prestamype.com'
'factoringfinanzas@prestamype.com'

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
dfowo = pd.DataFrame(resultados, columns = column_names)

#%%
correos = sorted(dfowo['customer_email'].unique())

#%%
df_filtrado = dfowo[dfowo['CODE'].isin([
        '63liGJEc',
        'UB6Wnclj',
        '7zcMP60t',
        'taZ0mAYW',
        '93DfOT0S',
        'yNOjGZdP',
        'oWtvQ83D',
        'XdVanX0p',
        'oJTCemRX',
        'PfwpAEcd',
        '2lFWUOEp',
        'IMhbOsUc',
        'QcXtFVwQ',
        'nzpRgVgx'
                ])]

pivoteado = df_filtrado.pivot_table(index = 'CODE',
                                    columns = "TIPO",
                                    values = 'amount',
                                    aggfunc = 'sum').reset_index()

aux = df_filtrado.drop_duplicates(subset='CODE')
aux = aux[['CODE','monto_financiado']]
pivoteado = pivoteado.merge(aux,
                            on = 'CODE',
                            how = 'left')

pivoteado = pivoteado.fillna(0)


#%%

df_filtrado['CODE'].unique()


customers = sorted(df_filtrado['customer_name'].unique())

import os
os.chdir(r'C:\Users\Joseph Montoya\Desktop')




