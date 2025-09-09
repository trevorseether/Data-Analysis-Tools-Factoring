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
# from datetime import datetime

from pyathena import connect

#%% corte actual
mes_incorporar = '2025-08-31' # último día del mes, cambiar en cada ejecución

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

df_view.to_excel(f'fac_outstanding_tiempo_real_{codmes}.xlsx',
                 index = False)
#%% agregando el mes actual
df_corte['transfer_date'] = pd.to_datetime(df_corte['transfer_date'])

df_corte = df_corte[ df_corte['transfer_date'] <= eo_mes_anterior ]

df_view['codmes'] = df_view['codmes'].astype(int)
df_view  = df_view[ df_view['codmes'] == int(codmes)]

df_concatenado = pd.concat([df_corte, df_view], ignore_index=True)
print(df_concatenado.shape)

#%%
# Cliente de S3
s3 = boto3.client(
    "s3",
    aws_access_key_id        = creds["AccessKeyId"],
    aws_secret_access_key    = creds["SecretAccessKey"],
    aws_session_token        = creds["SessionToken"],
    region_name              = creds["region_name"]
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





