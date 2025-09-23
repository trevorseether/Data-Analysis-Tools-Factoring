# -*- coding: utf-8 -*-
"""
Created on Mon May 12 16:03:55 2025

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
    , c.amount --- MONTO FINANCIADO
    , a.proforma_simulation_currency
    , a.proforma_client_payment_date_expected
    , CASE
        WHEN c.customer_email LIKE 'gestora@prestamype.com' then 'GESTORA'
        WHEN c.customer_email LIKE 'factoringfinanzas@prestamype.com' then 'ONBALANCE'
        ELSE 'CROWD'
        END "TIPO"
        
    , (CASE WHEN (A.product = 'factoring') THEN A.proforma_simulation_financing_total ELSE A.proforma_simulation_financing END) 
    AS Monto_Financiado_completo
    
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
dfowo = dfowo[   ~pd.isna(dfowo['CODE'])   ]
#%%
pivoteado = dfowo.pivot_table(index   = 'CODE',
                              columns = "TIPO",
                              values  = 'amount',
                              aggfunc = 'sum').reset_index()

#%%
aux = dfowo.drop_duplicates(subset = 'CODE')
aux = aux[['CODE','Monto_Financiado_completo', 'proforma_client_payment_date_expected']]

pivoteado = pivoteado.merge(aux,
                            on = 'CODE',
                            how = 'left')

pivoteado['CROWD'] = pivoteado['CROWD'].fillna(0)
pivoteado['GESTORA'] = pivoteado['GESTORA'].fillna(0)
pivoteado['ONBALANCE'] = ['ONBALANCE'].fillna(0)

pivoteado['m fin calculado'] = pivoteado['CROWD'] + pivoteado['GESTORA'] + pivoteado['ONBALANCE']

def validacion(df):
    if df['m fin calculado'] == df['Monto_Financiado_completo']:
        return 'ok'
    else:
        return 'alerta'
pivoteado['validacion'] = pivoteado.apply(validacion, axis = 1)

#########################################################################
alerta = pivoteado[ pivoteado['validacion'] == 'alerta'   ]
if alerta.shape[0] > 0 :
    print(f'alerta, hay {alerta.shape[0]} casos que no cuadran')
#########################################################################

#%%
def tipo_financiamiento(df):
    if (df['GESTORA'] + df['ONBALANCE']== 0):
        return 'FULL CROWD'
    if (df['CROWD'] + df['ONBALANCE']== 0):
        return 'FULL GESTORA'
    if (df['CROWD'] + df['GESTORA']== 0):
        return 'FULL ONBALANCE'
    
    percen_ges   = round((df['GESTORA']/df['m fin calculado']   ) *100, 1)
    percen_crowd = round((df['CROWD']/df['m fin calculado']     ) *100, 1)
    percen_onb   = round((df['ONBALANCE']/df['m fin calculado'] ) *100, 1)
    if df['GESTORA']   == 0:
        return f'{percen_crowd}% CROWD y {percen_onb}% ONBALANCE'
    if df['CROWD']     == 0:
        return f'{percen_ges}% GESTORA y {percen_onb}% ONBALANCE'
    if df['ONBALANCE'] == 0:
        return f'{percen_crowd}% CROWD y {percen_ges}% GESTORA'
    
    else:
        return f'{percen_ges}% GESTORA, {percen_crowd}% CROWD, {percen_onb}% ONBALANCE'
pivoteado['tipo txt'] = pivoteado.apply(tipo_financiamiento , axis = 1)

#%%
df = pivoteado[['CODE', 'proforma_client_payment_date_expected', 'CROWD', 'GESTORA', 'ONBALANCE', ]]
df.rename(columns = {'proforma_client_payment_date_expected' : 'Fecha Vencimiento'}, inplace = True)

def crowd(df):
    if df['CROWD'] > 0:
        return 'SI'
    else:
        return 'NO'
df['Inversionista CROWD'] = df.apply(crowd, axis = 1)

def GESTORA(df):
    if df['GESTORA'] > 0:
        return 'SI'
    else:
        return 'NO'
df['fondo GESTORA'] = df.apply(GESTORA, axis = 1)

def ONBALANCE(df):
    if df['ONBALANCE'] > 0:
        return 'SI'
    else:
        return 'NO'
df['fondo ONBALANCE'] = df.apply(ONBALANCE, axis = 1)






