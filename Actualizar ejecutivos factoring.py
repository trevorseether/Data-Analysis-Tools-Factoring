# -*- coding: utf-8 -*-
"""
Created on Sat Nov 15 14:39:09 2025

@author: Joseph Montoya
"""

# =============================================================================
#    Actualizar ejecutivos factoring
#  ba__fac_ejecutivos
# =============================================================================

import pandas as pd
import boto3
import json
import io
# import os
# from datetime import datetime

from pyathena import connect

#%% mes a actualizar
codmes = '2026-03-31' # formato YYYY-MM-DD

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
select  * from prod_datalake_sandbox.ba__fac_ejecutivos

'''
 
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
df_actual = pd.DataFrame(resultados, columns = column_names)

df_actual['corte_mensual'] = pd.to_datetime( df_actual['corte_mensual'] )
df_actual['codmes'] = df_actual['codmes'].astype(int)

df_actual.columns = df_actual.columns.str.lower()

# df_actual = df_actual.head(0)
#%% especificar tipo de dato de flag_comisiones
df_actual['FLAG_COMISIONES'] = 0

#%% LECTURA DE FUENTE PRINCIPAL
# sobre este excel se pueden hacer las modificaciones
df = pd.read_excel(r'G:/.shortcut-targets-by-id/1wzewbtJQv6Fr_f0uKnZrRg-jPtPM9D8a/BUSINESS ANALYTICS/FACTORING/COMISIONES/Ejecutivos Factoring/fac_ejecutivos.xlsx',
                   sheet_name = 'Ejecutivos',
                   dtype = str)

df = df.drop(df.filter(regex="Unnamed").columns, axis=1)

df.columns = df.columns.str.lower()

df['flag de actividad'] = df['flag de actividad'].astype(int)
df['final_ejecutivo_id'] = df['final_ejecutivo_id'].str.strip()

duplicados = df[df['final_ejecutivo_id'].duplicated(keep=False)]
if duplicados.shape[0]:
    print('alerta de duplicados en la final_ejecutivo_id ')

#%%
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
################################################################################
# Hora actual en Perú (UTC-5)
now = datetime.now(ZoneInfo("America/Lima"))

# Guardar directamente el objeto datetime
df["_timestamp"] = now - timedelta(hours=5)

#%% añadir columnas corte_mensual y codmes
columnas = df.columns
columnas = list(columnas)

df['corte_mensual'] = pd.Timestamp(codmes)
codmes_yyyymm = pd.to_datetime(codmes).strftime('%Y%m')
df['codmes'] =int(codmes_yyyymm)

df = df[['corte_mensual', 'codmes'] + columnas]

#%% concatenar dataframes para cargar al lake
df_actual = df_actual[df_actual['corte_mensual'] != pd.Timestamp(codmes) ]

df = pd.concat([df_actual, df], ignore_index = True)

# ordenaditos
df = df.sort_values(['codmes', 'ejecutivo_final'], ascending = [False, True])

df['flag_comisiones'] = df['flag_comisiones'].astype(float)

df['canal']           = df['canal'].str.upper()
df['zona']            = df['zona'].str.upper()
df['cargo']           = df['cargo'].str.upper()
df['nombre completo'] = df['nombre completo'].str.upper()
df['equipo']          = df['equipo'].str.upper()

df = df[ df['final_ejecutivo_id'].notna()]

df['flag_comisiones'] = df['flag_comisiones'].astype(str).str.replace('.0', '')
df['flag_comisiones'] = df['flag_comisiones'].fillna('')

#%%
# datos_para_multiplicar = df[df['corte_mensual'] == pd.Timestamp('2024-12-31')]
# del datos_para_multiplicar['corte_mensual']
# datos_ya_cargados = df[df['corte_mensual'] > pd.Timestamp('2024-12-31')]

# datos_por_corregir = df[df['corte_mensual'] <= pd.Timestamp('2024-12-31')]
# datos_por_corregir = datos_por_corregir[['corte_mensual']]
# datos_por_corregir = datos_por_corregir.drop_duplicates(subset = 'corte_mensual')
# datos_por_corregir['a'] = 'a'
# datos_para_multiplicar['a'] = 'a'

# nuevos = datos_por_corregir.merge(datos_para_multiplicar,
#                                   on = 'a',
#                                   how = 'left')
# del nuevos['a']

# df = pd.concat([datos_ya_cargados,nuevos], ignore_index = True)

# df['corte_mensual'] = pd.to_datetime( df['corte_mensual'] )
# df['codmes'] = df['corte_mensual'].dt.strftime('%Y%m')
# df['codmes'] = df['codmes'].astype(int)

# df = df.sort_values(['codmes', 'ejecutivo_final'], ascending = [False, True])


#%%
nombre_tabla = 'fac_ejecutivos'

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
df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
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
print('')
print(f'cargado el mes {codmes}')

#%%
if duplicados.shape[0]:
    print('alerta de duplicados en la final_ejecutivo_id ')




