# -*- coding: utf-8 -*-
"""
Created on Wed Sep 24 13:11:02 2025

@author: Joseph Montoya
"""
# =============================================================================
# GENERAR SCHEMA Y PARQUET PARA AMAZON ATHENA, PARA CARGA EN SANDBOX
# =============================================================================

import pandas as pd
# pip install pyarrow

ubicacion = r'C:/Users/Joseph Montoya/Downloads/Ejecutivos-BI de líneas.xlsx'


df = pd.read_excel(io = ubicacion,
                             sheet_name = 'Ejecutivo',
                             dtype = str)

#%%
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
################################################################################
# Hora actual en Perú (UTC-5)
now = datetime.now(ZoneInfo("America/Lima"))

# Guardar directamente el objeto datetime
df["_timestamp"] = now - timedelta(hours=5)

#%% 
# pip install pyarrow
df.to_parquet(r'C:/Users/Joseph Montoya/Desktop/solicitud antonella/fac_line_executives.parquet',
                 index = False,
                 )

#%%

# el nombre del csv, también debe ser el nombre de la carpeta
nombre_parquet = 'fac_line_executives'
nombre_carpeta = nombre_parquet

df = pd.read_parquet(r'C:/Users/Joseph Montoya/Desktop/solicitud antonella/fac_line_executives.parquet')

#%%
nombre_parquet = 'fac_line_executives'
nombre_carpeta = nombre_parquet

mapping_types = {
    'object'          : 'string',
    'float64'         : 'double',
    'int64'           : 'integer',
    'int32'           : 'integer',
    'bool'            : 'boolean',
    'datetime64[ns]'  : 'date',
    'datetime64[us, America/Lima]' : 'TIMESTAMP'
}

temp_schema = ""
for col, dtype in df.dtypes.astype(str).to_dict().items(): 
    formated_type = mapping_types[dtype]
    temp_schema += f"""    `{col}` {formated_type},
"""

create_table_query = f"""
CREATE EXTERNAL TABLE prod_datalake_sandbox.ba__{nombre_parquet}
(
{temp_schema}
    
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    's3://prod-datalake-sandbox-730335218320/{nombre_carpeta}/'
TBLPROPERTIES (
  'parquet.compression'='snappy'
)
"""
create_table_query = create_table_query.replace("`_timestamp` string", "`_timestamp` TIMESTAMP")
create_table_query = create_table_query.replace("`_timestamp` TIMESTAMP,", "`_timestamp` TIMESTAMP")

print(create_table_query)



