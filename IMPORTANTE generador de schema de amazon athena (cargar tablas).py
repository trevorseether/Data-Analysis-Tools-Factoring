# -*- coding: utf-8 -*-
"""
Created on Thu Aug 14 15:43:33 2025

@author: Joseph Montoya
"""

import pandas as pd

df = pd.read_excel(r'C:/Users/Joseph Montoya/Desktop/ejemplo1/ejemplo1.xlsx')
df.to_csv("C:/Users/Joseph Montoya/Desktop/ejemplo1/ejemplo1.csv", sep=",", 
          index=False, 
          encoding="utf-8-sig")

df = pd.read_csv(r'C:/Users/Joseph Montoya/Desktop/ejemplo1/ejemplo1.csv',
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
CREATE EXTERNAL TABLE prod_datalake_master.ba__ejemplo1
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
    's3://prod-datalake-master-730335218320/manual/ba/ejemplo1.parquet'
TBLPROPERTIES (
  'parquet.compression'='snappy'
)
"""
print(create_table_query)



