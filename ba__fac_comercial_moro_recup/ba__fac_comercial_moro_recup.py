# -*- coding: utf-8 -*-
"""
Created on Fri Nov 28 10:14:41 2025

@author: Joseph Montoya
"""

# =============================================================================
#    Actualizar ejecutivos factoring
# =============================================================================

import pandas as pd
import boto3
import json
import io
# import os
# from datetime import datetime

from pyathena import connect

#%% mes para insertar
codmes = '2025-12-31'

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
# Cliente Athena
athena = boto3.client(
    'athena',
    aws_access_key_id=creds["AccessKeyId"],
    aws_secret_access_key=creds["SecretAccessKey"],
    aws_session_token=creds["SessionToken"],
    region_name=creds["region_name"]
)

# ID de la query guardada (lo ves en la URL de Athena)
named_query_id = "51eb3903-f7b6-4b2c-a527-c35765e74134"

# Obtener SQL
response = athena.get_named_query(NamedQueryId=named_query_id)
query_sql = response["NamedQuery"]["QueryString"]
cursor = conn.cursor()
cursor.execute(query_sql)

df = pd.DataFrame(cursor.fetchall(), columns=[c[0] for c in cursor.description])


df['Periodo_Cierre'] = df['Periodo_Cierre'].astype(int)

# datos que se van a insertar
codmes_yyyymm = pd.to_datetime(codmes).strftime('%Y%m')
df.columns = df.columns.str.lower()

df = df [df['periodo_cierre'] == int(codmes_yyyymm) ]

#%% columna _timestamp
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
################################################################################
# Hora actual en Perú (UTC-5)
now = datetime.now(ZoneInfo("America/Lima"))

# Guardar directamente el objeto datetime
df["_timestamp"] = now - timedelta(hours=5)

#%% leer datos estaticos de meses anteriores
query = ''' select * from prod_datalake_sandbox.ba__fac_comercial_moro_recup '''

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
df_estatico         = pd.DataFrame(resultados, columns = column_names)
df_estatico.columns = df_estatico.columns.str.lower()

df_estatico['periodo_cierre'] = df_estatico['periodo_cierre'].astype(int)

df_estatico = df_estatico[df_estatico['periodo_cierre'] != int(codmes_yyyymm)]

#%% concatenación para incluir el mes actual
df_final = pd.concat([df_estatico, df], ignore_index = True)

df_final = df_final.sort_values(by = ['periodo_cierre', 'ejecutivo', 'codigo_subasta'], ascending = [False, True, True])

#%% CARGA AL LAKE
nombre_tabla = 'fac_comercial_moro_recup'

# Cliente de S3
s3 = boto3.client(
    "s3",
    aws_access_key_id     = creds["AccessKeyId"],
    aws_secret_access_key = creds["SecretAccessKey"],
    aws_session_token     = creds["SessionToken"],
    region_name           = creds["region_name"]
)

# ==== CONFIGURACIÓN ====
bucket_name = "prod-datalake-sandbox-730335218320"
s3_prefix   = f"{nombre_tabla}/"  # carpeta lógica en el bucket

# ==== EXPORTAR A PARQUET EN MEMORIA ====
parquet_buffer = io.BytesIO()
df_final.to_parquet(parquet_buffer, index=False, engine="pyarrow")
# también puedes usar engine="fastparquet" si lo prefieres

# Nombre de archivo con timestamp (opcional)
s3_key = f"{s3_prefix}{nombre_tabla}.parquet"

# Subir directamente desde el buffer
s3.put_object(
    Bucket = bucket_name,
    Key    = s3_key,
    Body   = parquet_buffer.getvalue()
)

print(f"✅ Archivo subido a s3://{bucket_name}{s3_key}")

#%%
print('fin')

