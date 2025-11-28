# -*- coding: utf-8 -*-
"""
Created on Thu Aug 14 15:43:33 2025

@author: Joseph Montoya
"""

import pandas as pd

codmes = '2025-10-31'

#%%

df = pd.read_excel(r'G:/.shortcut-targets-by-id/1wzewbtJQv6Fr_f0uKnZrRg-jPtPM9D8a/BUSINESS ANALYTICS/FACTORING/COMISIONES/Ejecutivos Factoring/fac_ejecutivos.xlsx',
                   sheet_name = 'Ejecutivos',
                   dtype = str)
df

df.to_csv(r"G:/.shortcut-targets-by-id/1wzewbtJQv6Fr_f0uKnZrRg-jPtPM9D8a/BUSINESS ANALYTICS/FACTORING/COMISIONES/Ejecutivos Factoring/fac_ejecutivos.csv", 
          sep = ",", 
          index     = False, 
          encoding  = "utf-8-sig")

#%%%
# el nombre del csv, también debe ser el nombre de la carpeta


df = pd.read_csv(r'G:/.shortcut-targets-by-id/1wzewbtJQv6Fr_f0uKnZrRg-jPtPM9D8a/BUSINESS ANALYTICS/Lending/portafolio_lending/cosecha_lending.csv',
                 delimiter = ',')

nombre_csv = 'cosecha_lending' ############################################################################################

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
CREATE EXTERNAL TABLE prod_datalake_master.ba__{nombre_csv}
(
{temp_schema}
    `_timestamp` TIMESTAMP
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    's3://prod-datalake-master-730335218320/manual/ba/{nombre_csv}.parquet'
TBLPROPERTIES (
  'parquet.compression'='snappy'
)
"""
print(create_table_query)


