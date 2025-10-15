# -*- coding: utf-8 -*-
"""
Created on Mon May 12 16:03:55 2025

@author: Joseph Montoya
"""

import pandas as pd
from pyathena import connect
import json

import warnings
warnings.filterwarnings("ignore")

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

pivoteado[['CROWD', 'GESTORA', 'ONBALANCE']] = pivoteado[['CROWD', 'GESTORA', 'ONBALANCE']].fillna(0)

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

#%%
df.to_excel(r'C:\Users\Joseph Montoya\Desktop\pruebas\tipo financiamiento online.xlsx', index = False)

#%%
query = '''

select
    dealname,
    monto_financiado,
    fuente_fondeo

from prod_datalake_master.hubspot__deal    
where pipeline = '14026011'
and fuente_fondeo is not null
 
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
df_fuente_fondeo_hubspot = pd.DataFrame(resultados, columns = column_names)

hubspots = df_fuente_fondeo_hubspot[ ~df_fuente_fondeo_hubspot['dealname'].isin(list(df['CODE'])) ]

#%%%
hubspots.to_excel(r'C:\Users\Joseph Montoya\Desktop\pruebas\tipo financiamiento hubspot.xlsx', index = False)

#%%%%%%%%%%%%%
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# # # # # #  nueva columna para riesgos
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
import pandas as pd


# cartera
data = pd.read_excel(r'G:/.shortcut-targets-by-id/1alT0hxGsi0dfv0NYh_LB4NrT2tKEgPK8/Cierre Factoring/Reportes/Inputs/DATA portafolio factoring (202509) 14-10-2025.xlsx')
data['Codigo de Subasta'] = data['Codigo de Subasta'].str.lower()

# cosecha propia
cosecha = pd.read_excel(r'G:/.shortcut-targets-by-id/1alT0hxGsi0dfv0NYh_LB4NrT2tKEgPK8/Cierre Factoring/Reportes/Inputs/DATA cosecha factoring (202509) 14-10-2025.xlsx')
cosecha['Codigo de Subasta'] = cosecha['Codigo de Subasta'].str.lower()


proporciones = pd.read_excel(r'C:/Users/Joseph Montoya/Desktop/pruebas/tipo financiamiento online 202509.xlsx')
proporciones['CODE'] = proporciones['CODE'].str.lower()
proporciones = proporciones.drop_duplicates(subset=['CODE'], keep='first')
proporciones = proporciones[['CODE', 'CROWD', 'GESTORA', 'ONBALANCE']]

data = data[['Codigo de Subasta',
            'fecha_cierre',
            'Cierre',
            
            'amount_financed',
            'amount_financed_soles',
            
            'remaining_capital',
            'Saldo Capital soles'
            ]]

data = data.merge(proporciones,
                  left_on  = 'Codigo de Subasta',
                  right_on = 'CODE',
                  how = 'left')

# data[['CROWD', 'GESTORA', 'ONBALANCE']] = data[['CROWD', 'GESTORA', 'ONBALANCE']].fillna(0)
data['CROWD']     = data['CROWD'].fillna(0)
data['GESTORA']   = data['GESTORA'].fillna(0)
data['ONBALANCE'] = data['ONBALANCE'].fillna(0)
#data['amount_financed_soles'] = data['amount_financed_soles'].fillna(0)

data['CROWD']     = data['CROWD'].astype(float)
data['GESTORA']   = data['GESTORA'].astype(float)
data['ONBALANCE'] = data['ONBALANCE'].astype(float)

data['suma'] = data['CROWD'] + data['GESTORA'] + data['ONBALANCE']
data['suma'] = data['suma'].fillna(0)

import numpy as np
data['CROWD']  = np.where(data['suma'] == 0,
                          data['amount_financed_soles'],
                          data['CROWD'])
data['suma'] = data['CROWD'] + data['GESTORA'] + data['ONBALANCE']

data['crowd %']      = data['CROWD'] / data['suma']
data['gestora %']    = data['GESTORA'] / data['suma']
data['onbalance %']  = data['ONBALANCE'] / data['suma']

data['crowd %']     = data['crowd %'].fillna(0)
data['gestora %']   = data['gestora %'].fillna(0)
data['onbalance %'] = data['onbalance %'].fillna(0)

for columna in ['amount_financed_soles', 'Saldo Capital soles']:
    data[f'crowd_{columna}']     = data[columna] * data['crowd %']
    data[f'gestora_{columna}']   = data[columna] * data['gestora %']
    data[f'onbalance_{columna}'] = data[columna] * data['onbalance %']

#%%
cosecha = cosecha[['Codigo de Subasta']]

sin_dups = data.drop_duplicates(subset = 'Codigo de Subasta')

cosecha = cosecha.merge(sin_dups[['Codigo de Subasta',
                                  'CROWD',
                                  'GESTORA',
                                  'ONBALANCE',
                                  'suma',
                                  'crowd %',
                                  'gestora %',
                                  'onbalance %',
                                  'crowd_amount_financed_soles',
                                  'gestora_amount_financed_soles',
                                  'onbalance_amount_financed_soles']],
                        on  = 'Codigo de Subasta',
                        how = 'left')

#%%
from datetime import datetime
hoy_formateado = datetime.today().strftime('%d-%m-%Y')
#data.to_excel(rf'C:\Users\Joseph Montoya\Desktop\pruebas\columnas adicionales {hoy_formateado}.xlsx')

with pd.ExcelWriter(rf'C:\Users\Joseph Montoya\Desktop\pruebas\columnas adicionales {hoy_formateado}.xlsx', engine="xlsxwriter") as writer:
    data.to_excel(writer, sheet_name="portafolio", index=False)
    cosecha.to_excel(writer, sheet_name="cosecha", index=False)

#%%
print('fin')
