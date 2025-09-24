# -*- coding: utf-8 -*-
"""
Created on Thu Aug 14 15:43:33 2025

@author: Joseph Montoya
"""

import pandas as pd

df = pd.read_excel(r'C:/Users/Joseph Montoya/Desktop/pruebas/parchamiento de fac outstanding/fac outst.xlsx')

df.to_csv("C:/Users/Joseph Montoya/Desktop/pruebas/parchamiento de fac outstanding/fac outst.csv", 
          sep = ",", 
          index     = False, 
          encoding  = "utf-8-sig")

#%%%
# el nombre del csv, también debe ser el nombre de la carpeta
nombre_csv = 'tabla_datos'

df = pd.read_csv(r'C:/Users/Joseph Montoya/Desktop/fac_outs/ba__fac_madres_hijas/ba__fac_madres_hijas.csv',
                 delimiter = ',')




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
    's3://prod-datalake-master-730335218320/manual/ba/nombre_csv.parquet'
TBLPROPERTIES (
  'parquet.compression'='snappy'
)
"""
print(create_table_query)



