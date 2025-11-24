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

df = pd.read_csv('C:/Users/Joseph Montoya/Desktop/fac_monthly_portfolio/fac_monthly_portfolio.csv',
                     dtype = {'Codigo_subasta': str,
                              'Cierre_fecha': str,
                              'codmes': str,
                              'Producto'	: str ,
                            'Ruc_cliente'	: str ,
                            'Nombre_cliente'	: str ,
                            'Ruc_proveedor'	: str ,
                            'Nombre_proveedor'	: str ,
                            'Fecha_desembolso'	: str ,
                            'Moneda'	: str ,
                            'Monto_neto_total_pendiente'	: float ,
                            'Monto_financiado'	: float ,
                            'Plazo'	: float ,
                            'tasa_empresario'	: float ,
                            'Monto_adelanto'	: float ,
                            'Ejecutivo'	: str ,
                            'Monto_total_pagado'	: float ,
                            'Capital_pagado'	: float ,
                            'Interes_pagado'	: float ,
                            'Ultima_fecha_pago'	: str ,
                            'Fecha_vencimiento'	: str ,
                            'Q_desembolso'	: float ,
                            'm_desembolso'	: float ,
                            'Saldo_capital'	: float ,
                            'Saldo_monto'	: float ,
                            'status'	: str ,
                            'Dias_atraso'	: float ,
                            'm_desembolso_soles'	: float ,
                            'Saldo_capital_soles'	: float ,
                            'Monto_financiado_soles'	: float ,
                            'Tipo_cambio'	: float ,
                            'codmes_desembolso'	: int ,
                            'PAR1_ms': float ,
                            'PAR15_ms'	: float ,
                            'PAR30_ms'	: float ,
                            'PAR60_ms'	: float ,
                            'PAR90_ms'	: float ,
                            'PAR120_ms'	: float ,
                            'PAR180_ms'	: float ,
                            'PAR360_ms'	: float ,
                            'FLAG_ORIGEN_OPERACION'	: str ,
                            'new_clients'	: int ,
                            'new_providers'	: int ,
                            'Fuente'	: str ,
                            'Comercial'	: str ,
                            'tipificacion_operativa'	: str ,
                            'confirmaron_todas_las_facturas_en_cavali_'	: str ,
                            'correo_de_confirmacion'	: str ,
                            'comision_estructuracion'	: float ,
                            'comision_estructuracion_solarizado'	: float ,
                            'tasa_all_in'	: float ,
                            'utilidad_soles'	: float ,
                            'fecha'	: str ,
                            'day'	: int ,
                            'month'	: int ,
                            'year'	: int ,
                            'day_name'	: str ,
                            'week'	: int ,
                            'quarter_name'	: str ,
                            'semester'	: int ,
                            'day_of_week'	: int ,
                            'day_of_year'	: int ,
                            'is_weekend'	: str ,
                            'is_holiday'	: str ,
                            'fiscal_year'	: int ,
                            'fiscal_month'	: int ,
                            'fiscal_quarter'	: str ,
                            'year_month'	: str ,
                            'Riesgo'	: str ,
                            'financiado_soles_gestora'	: float ,
                            'financiado_soles_fp'	: float ,
                            'financiado_soles_crowd'	: float ,
                            'saldo_gestora'	: float ,
                            'saldo_fp'	: float ,
                            'saldo_crowd': float ,
                            'grupo_economico'	: str ,
                            'actividad_economica'	: str ,
                            })

#%%
df['Cierre_fecha'] = pd.to_datetime(df['Cierre_fecha']).dt.date
df['Fecha_desembolso'] = pd.to_datetime(df['Fecha_desembolso']).dt.date
df['Ultima_fecha_pago'] = pd.to_datetime(df['Ultima_fecha_pago']).dt.date
df['Fecha_vencimiento'] = pd.to_datetime(df['Fecha_vencimiento']).dt.date
df['fecha'] = pd.to_datetime(df['fecha']).dt.date

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
df.to_parquet(r'C:/Users/Joseph Montoya/Desktop/fac_monthly_portfolio/fac_monthly_portfolio.parquet',
                 index = False,
                 )

#%%

# el nombre del csv, también debe ser el nombre de la carpeta
nombre_parquet = 'fac_monthly_portfolio'
nombre_carpeta = nombre_parquet

df = pd.read_parquet(r'C:/Users/Joseph Montoya/Desktop/fac_monthly_portfolio/fac_monthly_portfolio.csv')

#%%
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
create_table_query = create_table_query.replace("`_timestamp` TIMESTAMP.", "`_timestamp` TIMESTAMP")

print(create_table_query)



