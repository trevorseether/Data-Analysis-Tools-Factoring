# -*- coding: utf-8 -*-
"""
Created on Thu May 15 10:29:42 2025

@author: Joseph Montoya
"""
import os
import pandas as pd
# import requests
# from io import BytesIO
import numpy as np
# import boto3
from pyathena import connect
# import openpyxl
# from openpyxl import load_workbook
# from openpyxl.styles import NamedStyle
# import os

# import shutil
from datetime import datetime

import warnings
warnings.filterwarnings("ignore")
from pyathena import connect

#%%
os.chdir(r'G:\.shortcut-targets-by-id\1alT0hxGsi0dfv0NYh_LB4NrT2tKEgPK8\Cierre Factoring\Archivos')
# Credenciales de AmazonAthena
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
# Ejecutar consulta
# query = ''' select * from prod_datalake_analytics.fac_outstanding '''

# cursor = conn.cursor()
# cursor.execute(query)

# # Obtener los resultados
# resultados = cursor.fetchall()

# # Obtener los nombres de las columnas
# column_names = [desc[0] for desc in cursor.description]

# # Convertir los resultados a un DataFrame de pandas
# data_cierre_de_todo = pd.DataFrame(resultados, columns = column_names)

# #%%
# data_cierre_de_todo.to_excel('Data_Cierre_deTodo (06 06 2025).xlsx', index = False)

#%%
# Ejecutar consulta
query = """                      -- fac_bids_funds_ymonja
SELECT 
    b.auction_id,
    a.auction_code,
    b.customer_name AS funding_name,
    MAX(b.amount) AS bid_amount
FROM prod_datalake_analytics.fac_bids b
LEFT JOIN prod_datalake_analytics.fac_auctions a 
    ON a._id = b.auction_id
WHERE b.status = 'ganado' 
    AND (b.customer_name LIKE '%FONDO%' OR b.customer_name LIKE '%FACTORING%')
GROUP BY b.auction_id, a.auction_code, b.customer_name;

"""
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
bids_funds = pd.DataFrame(resultados, columns = column_names)

#%%
bids_funds.to_excel('bids_funds.xlsx', index = False)

#%%

# chunk_size = 40000
# start = 1
# end = chunk_size
# dataframes = []

# while True:
#     query = f"""
#     WITH numerada AS (
#       SELECT 
#         *, 
#         row_number() OVER (ORDER BY (SELECT NULL)) AS rn
#       FROM "prod_datalake_master"."prestamype__sentinel_batch"
#     )
#     SELECT * FROM numerada
#     WHERE rn BETWEEN {start} AND {end}
#     """
    
#     print(f"Descargando filas {start} a {end}")
#     df_chunk = pd.read_sql(query, conn)

#     if df_chunk.empty:
#         print("No hay más datos.")
#         break

#     dataframes.append(df_chunk)
#     start += chunk_size
#     end += chunk_size

# # Concatenar todo
# sentinel_batch = pd.concat(dataframes, ignore_index=True)

# #%%
# sentinel_batch.to_csv('sentinel_batch script.csv',                        
#                       index    = False,
#                       encoding = 'utf-8-sig')



