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

df = pd.read_excel('datos.xlsx')

#%% 
# pip install pyarrow
df.to_parquet(r'C:/Users/Joseph Montoya/Desktop/ejemplo1/ejemplo1.parquet',
                 index = False,
                 )

#%%

# el nombre del csv, también debe ser el nombre de la carpeta
nombre_parquet = 'ejemplo1'
nombre_carpeta = nombre_parquet

df = pd.read_parquet(r'C:/Users/Joseph Montoya/Desktop/ejemplo1/ejemplo1/ejemplo1.parquet')

from datetime import datetime
now = datetime.utcnow()
athena_timestamp = now.strftime("%Y-%m-%d %H:%M:%S.000")

df['_timestamp'] = athena_timestamp

mapping_types = {
    'object'          : 'string',
    'float64'         : 'double',
    'int64'           : 'integer',
    'bool'            : 'boolean',
    'datetime64[ns]'  : 'date'
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

print(create_table_query)

df_pagos.head()
