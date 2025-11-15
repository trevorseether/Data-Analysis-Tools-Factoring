# -*- coding: utf-8 -*-
"""
Created on Sat Nov 15 14:39:09 2025

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

#%% LECTURA DE FUENTE PRINCIPAL
# sobre este excel se pueden hacer las modificaciones
df = pd.read_excel(r'G:/.shortcut-targets-by-id/1wzewbtJQv6Fr_f0uKnZrRg-jPtPM9D8a/BUSINESS ANALYTICS/FACTORING/COMISIONES/Ejecutivos Factoring/fac_ejecutivos.xlsx',
                   sheet_name = 'Ejecutivos',
                   dtype = str)

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
s3_prefix = "manual/ba/fac_ejecutivos/" # carpeta lógica en el bucket 

# ==== EXPORTAR A PARQUET EN MEMORIA ====
csv_buffer = io.StringIO() 
df.to_csv(csv_buffer, index=False, encoding="utf-8-sig") 

# Nombre de archivo con timestamp (opcional, para histórico) 
s3_key = f"{s3_prefix}fac_ejecutivos.csv" 

# Subir directamente desde el buffer 
s3.put_object(Bucket  = bucket_name, 
              Key     = s3_key, 
              Body    = csv_buffer.getvalue() 
              )

print(f"✅ Archivo subido a s3://{bucket_name}/{s3_key}")






