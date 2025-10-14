# -*- coding: utf-8 -*-
"""
Created on Mon Sep  8 16:40:10 2025

@author: Joseph Montoya
"""

# =============================================================================
# fac_outstanding_monthly_snapshot
# =============================================================================

import pandas as pd
import boto3
import json
import io
import os
from datetime import datetime
import numpy as np

from pyathena import connect

hoy_formateado = datetime.today().strftime('%Y-%m-%d')

#%% corte actual
mes_incorporar = '2025-09-30' # último día del mes, cambiar en cada ejecución

path = r'C:/Users/Joseph Montoya/Desktop/fac_outs' # no cambiar

txt_credenciales_athena = r"C:/Users/Joseph Montoya/Desktop/credenciales actualizado.txt" # no cambiar

#%% funciones de transformación de fechas
mes_incorporar = pd.Timestamp(mes_incorporar)

def eomonth(fecha):
    """
    Devuelve el último día del mes de la fecha dada.
    """
    fecha = pd.Timestamp(fecha)
    return fecha + pd.offsets.MonthEnd(0)

def prev_month_eomonth(fecha):
    """
    Devuelve el último día del mes anterior a la fecha dada.
    """
    fecha = pd.Timestamp(fecha)  # asegurar tipo Timestamp
    fech2 = (fecha - pd.offsets.MonthBegin(1)) - pd.offsets.Day(1)
    return pd.Timestamp(fech2)

def convertir_codmes(fecha):
    fecha = pd.Timestamp(mes_incorporar)
    yyyymm = fecha.strftime("%Y%m")
    return yyyymm
    
eo_mes_actual   = eomonth(mes_incorporar)
eo_mes_anterior = prev_month_eomonth(mes_incorporar)
codmes          = convertir_codmes(mes_incorporar)

#%%
# Crea la carpeta si no existe
os.chdir(path)
         
folder_name = codmes
os.makedirs(folder_name, exist_ok=True)

os.chdir(path + '/' + folder_name)

#%% Credenciales de AmazonAthena
with open(txt_credenciales_athena) as f:
    creds = json.load(f)

conn = connect(
    aws_access_key_id     = creds["AccessKeyId"],
    aws_secret_access_key = creds["SecretAccessKey"],
    aws_session_token     = creds["SessionToken"],
    s3_staging_dir        = creds["s3_staging_dir"],
    region_name           = creds["region_name"]
    
    )

#%% lectura del corte anterior

query = ''' select * from prod_datalake_master."ba__fac_outstanding_monthly_snapshot"  '''

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
df_corte = pd.DataFrame(resultados, columns = column_names)

df_corte.to_excel(f'extraido {hoy_formateado}.xlsx', index = False)

del df_corte['_timestamp']

print(df_corte.shape)

#%% mes actual que vamos a incorporar

query = ''' select * from prod_datalake_analytics."fac_outst_unidos_f_desembolso_jmontoya"  '''

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
df_view = pd.DataFrame(resultados, columns = column_names)
print(df_view.shape)

df_view.to_excel(f'fac_outstanding_tiempo_real_{codmes} - {hoy_formateado}.xlsx',
                  index = False)

df_view_completo = df_view.copy()

#%% agregando el mes actual
df_corte['codmes'] = df_corte['codmes'].astype(int)
df_corte = df_corte[ df_corte['codmes'] < int(codmes) ] #fitrando el mes actual en caso de que quisieramos añadir datos nuevamente

df_view['codmes'] = df_view['codmes'].astype(int)
df_view  = df_view[ df_view['codmes'] == int(codmes)]

#ASEGURANDO EL ORDENAMIENTO, PUES EXISTEN DIFERENCIAS POR MAYÚSCULAS Y MINÚSCULAS
columnas = ['code', 'fecha_cierre', 'codmes', 'product_type', 'client_ruc', 'client_name', 'provider_ruc', 'provider_name', 'flag_newclient', 'flag_newprovider', 'transfer_date', 'ANTERIOR_TRANSFER', 'currency_request', 'currency_auctions', 'assigned_financing_rate', 'total_net_amount_pending_payment', 'e_payment_date_original', 'amount_financed', 'terms', 'amount_advance', 'advance_percentage', 'invoice_count', 'amount_of_invoices', 'assigned_name', 'assigned_last_name', 'company_id', 'user_third_party_id', 'client_id', 'provider_id', 'request_id', 'client_payment_id', 'payment_currency', 'payment_date', 'total_amount_paid', 'capital_paid', 'interest_paid', 'guarantee_paid', 'last_status', 'last_paid_date', 'facturas_vencimientos_iguales', 'fecha_confirmada_hubspot', 'max_payment_date_invoices', 'e_payment_date', 'cambio_fecha_vencimiento', 'q_desembolso', 'm_desembolso', 'new_clients', 'recurrent_clients', 'new_providers', 'recurrent_providers', 'remaining_capital', 'remaining_total_amount', 'actual_status', 'flag_excluir', 'dias_atraso', 'm_desembolso_soles', 'remaining_capital_soles', 'amount_financed_soles', 'exchange_rate', 'codmes_transfer', 'PAR1_m', 'PAR15_m', 'PAR30_m', 'PAR60_m', 'PAR90_m', 'PAR120_m', 'PAR180_m', 'PAR360_m', 'PAR1_q', 'PAR15_q', 'PAR30_q', 'PAR60_q', 'PAR90_q', 'PAR120_q', 'PAR180_q', 'PAR360_q', 'q_vigente', 'condoned', 'judicialized', 'rango_dias_atraso', 'rango_duracion', 'PAR1_ms', 'PAR15_ms', 'PAR30_ms', 'PAR60_ms', 'PAR90_ms', 'PAR120_ms', 'PAR180_ms', 'PAR360_ms', 'FLAG_ORIGEN_OPERACION']
columnas_mayusc = [col.upper() for col in columnas]
cc = df_corte.columns
cc = [col.upper() for col in cc]
cv = df_view.columns
cv = [col.upper() for col in cv]

df_corte.columns = cc
df_corte = df_corte[columnas_mayusc]

df_view.columns = cv
df_view = df_view[columnas_mayusc]

df_corte.columns = columnas
df_view.columns  = columnas

df_concatenado = pd.concat([df_corte, df_view], ignore_index=True)
print(df_concatenado.shape)

#%% CONVERTIR COLUMNAS DE FECHAS A UN SOLO FORMATO

df_concatenado["fecha_cierre"] = pd.to_datetime(df_concatenado["fecha_cierre"],  format="mixed")
df_concatenado["fecha_cierre"] = df_concatenado["fecha_cierre"].dt.strftime("%Y-%m-%d")
alert = df_concatenado[ df_concatenado["fecha_cierre"].isna() ]
if alert.shape[0]>0:
    print('alerta de nulos en fecha_cierre')
    
df_concatenado["transfer_date"] = pd.to_datetime(df_concatenado["transfer_date"],  format="mixed")
df_concatenado["transfer_date"] = df_concatenado["transfer_date"].dt.strftime("%Y-%m-%d")
alert = df_concatenado[ df_concatenado["transfer_date"].isna() ]
if alert.shape[0]>0:
    print('alerta de nulos en transfer_date')    
    
df_concatenado["e_payment_date_original"] = pd.to_datetime(df_concatenado["e_payment_date_original"],  format="mixed")
df_concatenado["e_payment_date_original"] = df_concatenado["e_payment_date_original"].dt.strftime("%Y-%m-%d")
alert = df_concatenado[ df_concatenado["e_payment_date_original"].isna() ]
if alert.shape[0]>0:
    print('alerta de nulos en e_payment_date_original')    

df_concatenado["payment_date"] = pd.to_datetime(df_concatenado["payment_date"],  format="mixed")
df_concatenado["payment_date"] = df_concatenado["payment_date"].dt.strftime("%Y-%m-%d")
alert = df_concatenado[ df_concatenado["payment_date"].isna() ]

df_concatenado["e_payment_date"] = pd.to_datetime(df_concatenado["e_payment_date"],  format="mixed")
df_concatenado["e_payment_date"] = df_concatenado["e_payment_date"].dt.strftime("%Y-%m-%d")
alert = df_concatenado[ df_concatenado["e_payment_date"].isna() ]
if alert.shape[0]>0:
    print('alerta de nulos en e_payment_date')    

#%% obtener ruc del corte más reciente, para unirlo por codigo de subasta, a aquellos casos donde falte ruc
rucs = df_view_completo[['code', 'client_ruc', 'codmes']]
rucs = rucs.sort_values(by = 'codmes', ascending = False)
rucs = rucs.drop_duplicates(subset=['code'], keep="first")
rucs = rucs[ ~pd.isna(rucs['client_ruc']) ]
del rucs['codmes']
rucs.columns = ['code_aux', 'ruc_aux']

df_concatenado = df_concatenado.merge(rucs,
                                      left_on  = 'code',
                                      right_on = 'code_aux',
                                      how      = 'left')
df_concatenado['client_ruc'] = np.where( ~df_concatenado['ruc_aux'].isnull(),
                                         df_concatenado['ruc_aux'],
                                         df_concatenado['client_ruc'] )
del df_concatenado['code_aux']
del df_concatenado['ruc_aux']

#%% limpieza de texto del client_ruc y provider_ruc
col_ruc = 'client_ruc'
df_concatenado[col_ruc] = (
    df_concatenado[col_ruc]
    .astype(str)                 # convierte todo a string
    .str.replace('.0','', regex=False)  # limpia floats mal guardados
    .replace('None', pd.NA)      # convierte 'None' a valor nulo real
)
vr = df_concatenado[ (df_concatenado[col_ruc] == 'None') | (pd.isna(df_concatenado[col_ruc]))] 

col_ruc = 'provider_ruc'
df_concatenado[col_ruc] = (
    df_concatenado[col_ruc]
    .astype(str)                 # convierte todo a string
    .str.replace('.0','', regex=False)  # limpia floats mal guardados
    .replace('None', pd.NA)      # convierte 'None' a valor nulo real
)
vr = df_concatenado[ (df_concatenado[col_ruc] == 'None') | (pd.isna(df_concatenado[col_ruc]))] 

#%% parchamiento del nombre del client_ruc
clientes = df_concatenado[['client_ruc', 'client_name', 'codmes']]
clientes = clientes.sort_values(by='codmes', ascending=False)
clientes = clientes.drop_duplicates(subset=['client_ruc'], keep="first")
clientes = clientes[ ~pd.isna(clientes['client_ruc'])]
clientes = clientes[ ~pd.isna(clientes['client_name'])]
clientes.columns = ['client_ruc_aux', 'client_name_aux', 'codmes']

df_concatenado = df_concatenado.merge(clientes[['client_ruc_aux', 'client_name_aux']],
                                  left_on  = 'client_ruc',
                                  right_on = 'client_ruc_aux',
                                  how      = 'left')
df_concatenado['client_name'] = np.where( ~df_concatenado['client_name_aux'].isnull(),
                                         df_concatenado['client_name_aux'],
                                         df_concatenado['client_name'])
del df_concatenado['client_ruc_aux']
del df_concatenado['client_name_aux']

#%%

# Cliente de S3
s3 = boto3.client(
    "s3",
    aws_access_key_id       = creds["AccessKeyId"],
    aws_secret_access_key   = creds["SecretAccessKey"],
    aws_session_token       = creds["SessionToken"],
    region_name             = creds["region_name"]
)

# ==== CONFIGURACIÓN ==== 
bucket_name = "prod-datalake-raw-730335218320" 
s3_prefix = "manual/ba/fac_outstanding_monthly_snapshot/" # carpeta lógica en el bucket 

# ==== EXPORTAR A PARQUET EN MEMORIA ====
csv_buffer = io.StringIO() 
df_concatenado.to_csv(csv_buffer, index=False, encoding="utf-8-sig") 

# Nombre de archivo con timestamp (opcional, para histórico) 
s3_key = f"{s3_prefix}fac_outstanding_monthly_snapshot.csv" 

# Subir directamente desde el buffer 
s3.put_object(Bucket = bucket_name, 
              Key    = s3_key, 
              Body   = csv_buffer.getvalue() 
              )

print(f"✅ Archivo subido a s3://{bucket_name}/{s3_key}")

