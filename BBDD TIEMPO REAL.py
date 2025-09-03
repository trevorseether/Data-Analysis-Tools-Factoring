# -*- coding: utf-8 -*-
"""
Created on Mon Apr 14 09:38:51 2025

@author: Joseph Montoya
"""

# =============================================================================
#    AUTOMATIZACIÓN AVANCE COMERCIAL
# =============================================================================

# pip install boto3 pyathena 
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

from datetime import datetime, timedelta

ayer = datetime.now() - timedelta(days=1)
ayer_str = ayer.strftime('%Y-%m-%d')
nombre_backup    = f'BBDD TIEMPO REAL {ayer_str}.xlsx'

#%% ESTABLECER PARÁMETROS INICIALES
# Poner True si se desea guardar BACKUP

guardar_backup   = True # True o False
ubicacion_backup = r'C:\Users\Joseph Montoya\Desktop\comparación resultados'

fechas_stock = [ 
                '2024-04',
                '2024-05',
                '2024-06',
                '2024-07',
                '2024-08',
                '2024-09',
                '2024-10',
                '2024-11',
                '2024-12',
                '2025-01',
                '2025-02',
                '2025-03',
                '2025-04',
                '2025-05',
                '2025-06'
                ]

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

#%% NOMBRE DEL EJECUTIVO COMERCIAL
# se requiere para acelerar esta parte, tener los datos en el lake, si es posible incluirlo en la misma query
ejecutivo = pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vSPwEielTCHa3tqAGK9ABAjT5EObngCLXvq10wGTHfnq60HZzajUygfeUHJ8hJGSXMznOD527UYi_AK/pub?gid=1973811406&single=true&output=csv',
                          # sheet_name  = 'Base Comisiones', # desactivado porque el archivo es un csv
                          dtype       = str,
                          usecols     = ['cod_ejecutivo' , 'Ejecutivo_txt'])

ejecutivo.dropna(subset=['cod_ejecutivo'], inplace = True, how = 'all') #eliminando las filas vacías
ejecutivo['cod_ejecutivo'] = ejecutivo['cod_ejecutivo'].astype(int)
ejecutivo.drop_duplicates(subset='cod_ejecutivo', inplace = True)

#%%
# Ejecutar consulta
query = """
SELECT * FROM "prod_datalake_master"."base_tiempo_real_1"
"""
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
df = pd.DataFrame(resultados, columns = column_names)

df = df[  df['code']  !=  'kiaurrpz'  ] # eliminación puntual de crédito cláramente mal llenado

df_respaldo = df.copy()

df = df_respaldo.copy()
###############################################################################
# df_filtrado = df[ df['code'].str.lower()  == 'mmpxnnaz']
###############################################################################
# df = df[ df['flag_es_offline']  == 'si']

print(df.shape)

#%% RENAME
df.rename(columns = {'code'                   : 'Código de Subasta',
                     'company_name'           : 'Proveedor',
                     'company_ruc'            : 'RUC Proveedor',
                     'user_third_party_name'  : 'Cliente',
                     'user_third_party_ruc'   : 'RUC Cliente',
                     'monto_financiado'       : 'Monto Financiado',
                     'monto_de_adelanto'      : 'Monto Adelanto',
                  'Moneda_Monto_Financiado'   : 'Moneda',
           'tasa_de_financiamiento_asignada'  : 'Tasa de Financiamiento',
                     'comision_total'         : 'Comisión de Estructuración',
                     'product'                : 'Tipo de Producto',
                     'fecha_de_cierre'        : 'Fecha de Cierre de Subasta',
                     'fecha_esperada_pago'    : 'Fecha de Pago Proyectada',
                     'tipo_de_pago'           : 'Tipo de Pago',
                     'Tipo de Proveedor'      : 'Tipo de Proveedor',
                     'Tipo de Cliente'        : 'Tipo de Cliente',
                    'Porpietario_del_negocio' : 'Ejecutivo (cod)',
                           'comision_sin_igv' : 'comision sin igv'
                     }, inplace = True)

df['RUC Cliente'] = df['RUC Cliente'].astype(float).astype('Int64').astype(str) # CORRECCIÓN DEL FORMATO
# df['Código de Subasta'] = df['Código de Subasta'].str.lower()
df['comision sin igv'] = df['comision sin igv'].round(2)
print(df.shape)
#%% ORDENAMIENTO
columnas_orden = ['Código de Subasta', 'Proveedor', 'RUC Proveedor', 'Cliente',
                  'RUC Cliente', 'Monto Financiado', 'Monto Adelanto', 'Moneda',
                  'Tasa de Financiamiento', 'Comisión de Estructuración', 'Tipo de Producto',
                  'Fecha de Cierre de Subasta', 'Fecha de Pago Proyectada',
                  'Tipo de Pago', 'Tipo de Proveedor', 'Tipo de Cliente', 'Ejecutivo (cod)', 'comision sin igv']
df = df[columnas_orden]

df['Tipo de Producto'] = df['Tipo de Producto'].str.lower()

print(df.shape)
#%% Invertir Proveedor y Cliente para Confirming
df['Proveedor aux']     = df['Proveedor'].copy()
df['RUC Proveedor aux'] = df['RUC Proveedor'].copy()
df['Cliente aux']       = df['Cliente'].copy()
df['RUC Cliente aux']   = df['RUC Cliente'].copy()

casos_raros = ['V.C.H.1.16.12.2024','TURBO.10.12.2024','SURE11.10.2024','LUCARBAL.11.12.2024','LINDE29.03.23','LASINO20.12.22','JJA.31.12.2024',
               'IVISA.04.04.2025','HENRRY04.12.2024','GEOFIELD05.12.2024','GEMCO11.12.2024','CERRO.DOR08.11.24','ARIS01.12.22',
               ]

df['Proveedor']      = np.where((df['Tipo de Producto'] == 'confirming') | (df['Código de Subasta'].isin( casos_raros)), df['Cliente aux'],       df['Proveedor'])
df['RUC Proveedor']  = np.where((df['Tipo de Producto'] == 'confirming') | (df['Código de Subasta'].isin( casos_raros)), df['RUC Cliente aux'],   df['RUC Proveedor'])
df['Cliente']        = np.where((df['Tipo de Producto'] == 'confirming') | (df['Código de Subasta'].isin( casos_raros)), df['Proveedor aux'],     df['Cliente'])
df['RUC Cliente']    = np.where((df['Tipo de Producto'] == 'confirming') | (df['Código de Subasta'].isin( casos_raros)), df['RUC Proveedor aux'], df['RUC Cliente'])

del df['Proveedor aux']
del df['RUC Proveedor aux']
del df['Cliente aux']
del df['RUC Cliente aux']
print(df.shape)
#%% EJECUTIVO
df.loc[df['Código de Subasta'] == 'HSsgng0q', 'Ejecutivo (cod)'] = 291494314
df = df.merge(ejecutivo,
              left_on  = 'Ejecutivo (cod)',
              right_on = 'cod_ejecutivo',
              how      = 'left')
nulos1 = df[   pd.isna(df['Ejecutivo_txt'])   ]
if nulos1.shape[0] > 0:
    print('alerta, hay casos nulos, falta el ejecutivo para los siguientes códigos:')
    print(nulos1['Ejecutivo (cod)'].unique())

df.rename(columns = {'Ejecutivo (cod)' : 'Ejecutivo'}, inplace = True)

df['Ejecutivo'] = df['Ejecutivo_txt']
del df['cod_ejecutivo']
del df['Ejecutivo_txt']

###############################################################################
q_e ='''  ---revisar el uso de esta query

SELECT 
    concat(assigned_name, ' ', assigned_last_name) AS "EJECUTIVO SEGÚN fac_requests",
    code                                           AS "Código de Subasta"
FROM prod_datalake_analytics.fac_requests 
--where assigned_name is not null

'''

cursor = conn.cursor()
cursor.execute(q_e)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
ejecutivo_query = pd.DataFrame(resultados, columns = column_names)

df = df.merge(ejecutivo_query,
              on = "Código de Subasta",
              how = 'left')
print(df.shape)
#%% Fecha de Cierre Final
####### query principal ###############################################################
query2 = '''   ------- query para cambio, REALIZAR COMPARACIONES
SELECT 
    max(a."date") AS "Fecha de Cierre Final",
    b.code        AS "Código de Subasta"

FROM prod_datalake_analytics.fac_client_payment_payments as a
left join prod_datalake_analytics.fac_client_payments as c  
on a.client_payment_id = c._id
left join prod_datalake_analytics.fac_requests  as b
on c.request_id = b._id

where b.status = 'closed'
and c.status = 'finalizado'
    group by b.code
'''
cursor = conn.cursor()
cursor.execute(query2)
# Obtener los resultados
resultados = cursor.fetchall()
# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]
# Convertir los resultados a un DataFrame de pandas
fecha_cierre = pd.DataFrame(resultados, columns = column_names)
fecha_cierre["Fecha de Cierre Final"] = fecha_cierre["Fecha de Cierre Final"].dt.normalize()

df = df.merge(fecha_cierre,
              on  = "Código de Subasta",
              how = 'left')

###### complementando información de cierre final de la operación (momento en que se pague la subasta)
query = """
SELECT 
    hs_object_id, 
    subject, 
    tipo_de_pago, 
    closed_date AS "FECHA_PAGO_HUB_TICKETS", --<<<<<<<<<<
    hs_pipeline_stage, 
    hs_pipeline
FROM prod_datalake_master.hubspot__ticket    --- según cobranzas, esta es la fecha en la que el deudor termina de pagar
WHERE hs_pipeline = 26417284  --- cobranzas factoring/confirming

"""
cursor = conn.cursor()
cursor.execute(query)
# Obtener los resultados
resultados = cursor.fetchall()
# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]
# Convertir los resultados a un DataFrame de pandas
fecha_cierre = pd.DataFrame(resultados, columns = column_names)
fecha_cierre["FECHA_PAGO_HUB_TICKETS"] = fecha_cierre["FECHA_PAGO_HUB_TICKETS"].dt.normalize()
fecha_cierre = fecha_cierre[ ~fecha_cierre['subject'].isin(['PRUEBA', 'PRUEBA - 1', 'PRUEBA123', 'PRUEBA123 - 1']) ]
# # fecha_cierre['subject'] = fecha_cierre['subject'].str.lower()
dups_cierre_hub_ticket = fecha_cierre[ fecha_cierre.duplicated(subset=['subject']) ]
if dups_cierre_hub_ticket.shape[0] > 0:
    print('alerta duplicados en fecha de pago de hubspot tickets')
fecha_cierre.drop_duplicates(subset='subject', inplace = True)

df = df.merge(fecha_cierre[['subject', 'FECHA_PAGO_HUB_TICKETS']],
              left_on  = 'Código de Subasta',
              right_on = 'subject',
              how = 'left')
del df['subject']

def ajuste_fecha_cierre_final(df):
    if pd.isna(df["Fecha de Cierre Final"]):
        return df['FECHA_PAGO_HUB_TICKETS']
    else:
        return df["Fecha de Cierre Final"]
df["Fecha de Cierre Final"] = df.apply(ajuste_fecha_cierre_final, axis = 1)
print(df.shape)
#%% Fecha de Desembolso Proveedor
# FECHA_FOTO1 = pd.read_excel(r'C:/SCRIPTS JOSEPH/tablas auxiliares/fechas desembolsos fotos.xlsx',
#                             dtype = str)
# FECHA_FOTO1['fecha'] = pd.to_datetime(FECHA_FOTO1['fecha'], dayfirst = True)
# # FECHA_FOTO1.rename(columns = {'Código de Subasta' : 'cs_ff1'}, inplace = True)
########################################################################################
query = """ --- TABLA PRIORITARIA,información que se llena desde diciembre 2024
SELECT 
    code AS "Código de Subasta",
    interest_proforma_disbursement_date AS "fecha"

FROM prod_datalake_analytics.fac_requests
    WHERE STATUS = 'closed'
    AND interest_proforma_disbursement_date IS NOT NULL
"""
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()
# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]
# Convertir los resultados a un DataFrame de pandas
FECHA_FOTO1 = pd.DataFrame(resultados, columns = column_names)

FECHA_FOTO1['fecha'] = pd.to_datetime(FECHA_FOTO1['fecha'])
########################################################################################FECHA
# FECHA_FOTO1['Código de Subasta'] = FECHA_FOTO1['Código de Subasta'].str.lower()
# fecha de desembolso real desde el lake
query = """ --- para llenar vacías
select 
    codigo_de_subasta                 AS "Código de Subasta",
    fecha_de_desembolso__factoring_   AS "Fecha Mínima de Desembolso"
    
from prod_datalake_master.hubspot__deal
where codigo_de_subasta is not null
and dealstage not in (14026016 , 14026018)
-- no se deben considerar ni rechazados ni cancelados(subasta desierta) porque no tienen fecha de desembolso y generan duplicidad

"""
# SELECT 
#     c_digo_de_subasta,
#     MIN(fecha_de_desembolso_registrado) AS "Fecha Mínima de Desembolso"
# FROM 
#     prod_datalake_master.hubspot__pagos_facturas
# WHERE 
#     hs_pipeline_stage = 89540624
#     AND fecha_de_desembolso_registrado IS NOT NULL
#     AND fecha_de_desembolso_registrado > DATE('2024-08-31')
# GROUP BY 
#     c_digo_de_subasta
# ORDER BY 
#     c_digo_de_subasta
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()
# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]
# Convertir los resultados a un DataFrame de pandas
desem = pd.DataFrame(resultados, columns = column_names)
desem.drop_duplicates(subset = "Código de Subasta", inplace = True)
# desem['Código de Subasta'] = desem['Código de Subasta'].str.lower()

## fechas fotos ##
query = '''SELECT 
    codigo_de_subasta AS "Código de Subasta",
    fecha_de_desembolso_proveedor AS "fecha fac_auctions_disbursement"

FROM prod_datalake_master.factoring__fac_auctions_disbursement_date
'''
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()
# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]
# Convertir los resultados a un DataFrame de pandas
desem_auctions_disbursement = pd.DataFrame(resultados, columns = column_names)

alerta_disbursement = desem_auctions_disbursement[pd.isna(desem_auctions_disbursement['fecha fac_auctions_disbursement'])]
if alerta_disbursement.shape[0] > 0:
    print('alerta, avisar a Yovani que arregle la tabla factoring__fac_auctions_disbursement_date')
# UNIÓN
df = df.merge(FECHA_FOTO1,
              on  = 'Código de Subasta',
              how = 'left')
df = df.merge(desem,
              on  = 'Código de Subasta',
              how = 'left')
df = df.merge(desem_auctions_disbursement,
              on = 'Código de Subasta',
              how = 'left')

def f_d_p(df):
    if not pd.isna(df['fecha']):
        return df['fecha']
    if not pd.isna(df['Fecha Mínima de Desembolso']):
        return df['Fecha Mínima de Desembolso']
    # if not pd.isna(df['fecha fac_auctions_disbursement']):
    #     return df['fecha fac_auctions_disbursement']
df['Fecha de Desembolso Proveedor'] = df.apply(f_d_p, axis = 1)

sin_fecha_desembolso = df[pd.isna(df['Fecha de Desembolso Proveedor'])]
if sin_fecha_desembolso.shape[0] > 0:
    print("alerta, hay casos sin fecha de desembolso")

del df['fecha']
del df['Fecha Mínima de Desembolso']
del df['fecha fac_auctions_disbursement']
# def nulos_f_d_p(df):  # reemplazando los nulos por la fecha de cierre
#     if pd.isna(df['Fecha de Desembolso Proveedor']):
#         return df['Fecha de Cierre Final']
#     else:
#         return df['Fecha de Desembolso Proveedor']
# df['Fecha de Desembolso Proveedor'] = df.apply(nulos_f_d_p, axis = 1 )

df_vacio = df[ pd.isna(df[ 'Fecha de Desembolso Proveedor' ])]
print(df.shape)
#%% Periodo Fecha de Desembolso
df['Periodo Fecha de Desembolso'] = pd.to_datetime(df['Fecha de Desembolso Proveedor']).dt.to_period('M').dt.to_timestamp()

#%% Periodo Fecha Cierre de Subasta
def p_f_c_s(row):
    if pd.isna(row['Fecha de Cierre de Subasta']):
        return row['Periodo Fecha de Desembolso']
    else:
        fecha = pd.to_datetime(row['Fecha de Cierre de Subasta'])
        return fecha.replace(day = 1) 

df['Periodo Fecha Cierre de Subasta'] = df.apply(p_f_c_s, axis = 1)
print(df.shape)
#%% BG "Periodo Fecha de cierre de Comisiones" columna que se calcula anticipadamente pero que habrá que mandarla hacia adelante
# f_quicksight = pd.read_excel(r'C:/SCRIPTS JOSEPH/tablas auxiliares/fechas quicksight.xlsx',
#                              dtype = str)
###############################################################################
query = '''
    SELECT
        codigo_de_subasta   AS "Código de Subasta",
        fecha_quicksight    AS "fecha quicksight"
    FROM prod_datalake_master.factoring__fac_auxiliar_quicksight_dates
'''
cursor = conn.cursor()
cursor.execute(query)
# Obtener los resultados
resultados = cursor.fetchall()
# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]
# Convertir los resultados a un DataFrame de pandas
f_quicksight = pd.DataFrame(resultados, columns = column_names)

###############################################################################
# f_quicksight['Código de Subasta'] = f_quicksight['Código de Subasta'].str.lower()
f_quicksight['fecha quicksight datetime'] = pd.to_datetime(f_quicksight['fecha quicksight'])
f_quicksight = f_quicksight[['Código de Subasta', 'fecha quicksight datetime']]

df = df.merge(f_quicksight,
              on  = 'Código de Subasta',
              how = 'left')

desem['m_per_desem'] = pd.to_datetime(desem['Fecha Mínima de Desembolso']).dt.to_period('M').dt.to_timestamp()

df = df.merge(desem[['Código de Subasta', 'm_per_desem']],
              on  = 'Código de Subasta',
              how = 'left'  )

# Convertir la columna a datetime
df['Fecha de Desembolso Proveedor'] = pd.to_datetime(df['Fecha de Desembolso Proveedor'], errors='coerce')

# Ahora puedes usar .dt.normalize() para quitar la parte de la hora
df['Fecha de Desembolso Proveedor'] = df['Fecha de Desembolso Proveedor'].dt.normalize()

# def p_f_c_c(df):
#     if not pd.isna(df['fecha quicksight datetime']):
#         return df['fecha quicksight datetime']
#     else:
#         return df['m_per_desem']

# def p_f_c_c(df):
#     if not pd.isna(df['fecha quicksight datetime']):
#         return df['fecha quicksight datetime']
#     if pd.isna(df['fecha quicksight datetime']) and not pd.isna(df['m_per_desem']):
#         return df['m_per_desem']
#     if pd.isna(df['fecha quicksight datetime']) and pd.isna(df['m_per_desem']):
#         return df['Fecha de Desembolso Proveedor'].dt.to_period('M').to_timestamp()

def p_f_c_c(row):
    if not pd.isna(row['fecha quicksight datetime']):
        return row['fecha quicksight datetime']
    elif not pd.isna(row['m_per_desem']):
        return row['m_per_desem']
    elif not pd.isna(row['Fecha de Desembolso Proveedor']):
        # Asegurarse de que sea datetime antes de aplicar .to_period
        fecha = pd.to_datetime(row['Fecha de Desembolso Proveedor'], errors='coerce')
        if pd.isna(fecha):
            return pd.NaT
        return fecha.to_period('M').to_timestamp()
    else:
        return pd.NaT
df['Periodo Fecha de cierre de Comisiones (anticipado)'] = df.apply(p_f_c_c, axis = 1)

del df['m_per_desem']
del df['fecha quicksight datetime']

alerta_fecha_cierre = df [ pd.isna(df['Periodo Fecha de cierre de Comisiones (anticipado)']) ]
if alerta_fecha_cierre.shape[0]>0:
    print('alerta, fecha de cierre vacía')
    
print(df.shape)
#%% Tipo de Cambio
# query = """
# SELECT *
# FROM "prod_datalake_master"."prestamype__montly_exchange_rate_usd"
# order by codmes desc
# """
# cursor = conn.cursor()
# cursor.execute(query)

# # Obtener los resultados
# resultados = cursor.fetchall()

# # Obtener los nombres de las columnas
# column_names = [desc[0] for desc in cursor.description]

# # Convertir los resultados a un DataFrame de pandas
# tc = pd.DataFrame(resultados, columns = column_names)

# tc['codmes'] = tc['codmes'].astype(int)
# tc = tc.sort_values(by='codmes', ascending=False)

#### versión 2 ################################################################
query = '''
    SELECT
        pk,
        tc_date,
        tc_contable AS "exchange_rate"
    FROM "prod_datalake_master"."prestamype__tc_contable"
    order by pk desc
'''
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
tc = pd.DataFrame(resultados, columns = column_names)
tc.drop_duplicates(subset = 'tc_date', inplace = True)
tc = tc[['tc_date', 'exchange_rate']]
tc = tc[    tc['exchange_rate'] > 0  ]

# Asegúrate de que 'tc_date' sea de tipo datetime
tc['tc_date'] = pd.to_datetime(tc['tc_date'])
# Agrupar por año y mes, y obtener la fecha máxima de cada grupo
tc = tc.loc[tc.groupby([tc['tc_date'].dt.year, tc['tc_date'].dt.month])['tc_date'].idxmax()]
tc['codmes'] = tc['tc_date'].dt.year * 100 + tc['tc_date'].dt.month

tc = tc.sort_values(by = 'codmes', ascending = False)
###############################################################################

# validación de que ya exista el tipo de cambio actual:
camb_actual = tc.head(3)
print('Últimas fechas de T.C.:')
print(camb_actual.assign(exchange_rate=camb_actual['exchange_rate'].astype(str))[['codmes', 'exchange_rate']])

# depende de la columna "Periodo Fecha de cierre de Comisiones"
df['Periodo YYYYMM'] = df['Periodo Fecha de cierre de Comisiones (anticipado)'].dt.year * 100 + df['Periodo Fecha de cierre de Comisiones (anticipado)'].dt.month

df = df.merge(tc[['codmes', 'exchange_rate']],
              left_on  = 'Periodo YYYYMM',
              right_on = 'codmes',
              how      = 'left')

del df['Periodo YYYYMM']
del df['codmes']

df.rename(columns = {'exchange_rate' : 'Tipo de Cambio'}, inplace = True)

print(df.shape)
#%% Interes proyectado (REQUIERE Tasa Original)

# LECTURA DE SHEET "Cambio de tasa - seguimiento OFICIAL" EL CUAL LO LLENAN OPERACIONES
sheet_id = "1-rjxRNSqi5gkn6GNBz5wjyQEc4MnvQF2PBQmbC97aWc"
sheet_name = "Hoja1"  # El nombre exacto de la pestaña
# URL para exportar como CSV
url = fr"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
# Leer directamente en un DataFrame
df_tasa_real = pd.read_csv(url, sep=',')
df_tasa_real = df_tasa_real[['CODIGOFINAL', 'TASA REAL EMPRESARIO']]
df_tasa_real['TASA REAL EMPRESARIO'] = (
    df_tasa_real['TASA REAL EMPRESARIO']
    .str.replace('%', '', regex=False)  # Quita el símbolo %
    .str.replace(',', '.', regex=False)  # Cambia coma por punto
    .astype(float)  # Convierte a float
) / 100
df_tasa_real['CODIGOFINAL'] = df_tasa_real['CODIGOFINAL'].str.strip()

df_aux = df[['Código de Subasta', 'Periodo Fecha de cierre de Comisiones (anticipado)']]

# esta parte se activa para filtrar solo aquellos cuya fecha de cierre es mayor a diciembre 2024
# df_aux_copia = df_aux.copy()
# df_aux = df_aux[  df_aux['Periodo Fecha de cierre de Comisiones (anticipado)'] >= pd.Timestamp('2024-12-01')  ]

df_tasa_real = df_tasa_real.merge(df_aux[['Código de Subasta']],
                                  left_on  = 'CODIGOFINAL',
                                  right_on = 'Código de Subasta',
                                  how = 'inner')
del df_aux
dupli = df_tasa_real[df_tasa_real.duplicated(subset=['CODIGOFINAL'])]
if dupli.shape[0] > 0:
    print('Alerta, duplicados en TASA REAL EMPRESARIO')
    print('revisar: Cambio de tasa - seguimiento OFICIAL')
# df_tasa_real['CODIGOFINAL'] = df_tasa_real['CODIGOFINAL'].str.lower()

df = df.merge(df_tasa_real[['CODIGOFINAL', 'TASA REAL EMPRESARIO']],
              left_on  = 'Código de Subasta',
              right_on = 'CODIGOFINAL',
              how      = 'left')
del df['CODIGOFINAL']
###############################################################################
# incluir tasa real empresario en la Tasa de Financiamiento
def tasa_de_fi(df):
    if not pd.isna(df['TASA REAL EMPRESARIO']):
        return df['TASA REAL EMPRESARIO']
    else:
        return df['Tasa de Financiamiento']
df['Tasa de Financiamiento'] = df.apply(tasa_de_fi, axis = 1)
###############################################################################
def Tasa_Original(df):
    if df['Periodo Fecha de cierre de Comisiones (anticipado)'] <= pd.Timestamp('2024-12-01'):
        if not pd.isna(df['Tasa de Financiamiento']):
            return df['Tasa de Financiamiento']
        if pd.isna(df['Tasa de Financiamiento']):
            return df['TASA REAL EMPRESARIO']
    else:
        return df['Tasa de Financiamiento']
df['Tasa Original (anticipado)'] = df.apply(Tasa_Original, axis = 1)

def Tasa_Original2(df): # doble aseguramiento de que no haya nulos :u
    if pd.isna(df['Tasa Original (anticipado)']):
        return df['Tasa de Financiamiento']
    else:
        return df['Tasa Original (anticipado)']
df['Tasa Original (anticipado)'] = df.apply(Tasa_Original2, axis = 1)
del df['TASA REAL EMPRESARIO']
###############################################################################<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
df['Fecha de Desembolso Proveedor'] = pd.to_datetime(df['Fecha de Desembolso Proveedor'],format='%YYYY-%mm-%dd')
df['Fecha de Pago Proyectada'] = pd.to_datetime(df['Fecha de Pago Proyectada'], errors='coerce')
df['Fecha de Cierre de Subasta'] = pd.to_datetime(df['Fecha de Cierre de Subasta'], errors='coerce')
# def int_pro(df):
#     if df['Fecha de Desembolso Proveedor'] >= pd.Timestamp('2024-10-01'):
        
#         dias = ((df['Fecha de Pago Proyectada'] - df['Fecha de Desembolso Proveedor']).days) / 30
#         #POSIBLEMENTE HAY QUE AÑADIR ELIMINACIÓN DE FILAS SI EL RESULTADO ES NEGATIVO
#         return dias

def int_pro(df):
    dias1 = ((df['Fecha de Pago Proyectada'] - df['Fecha de Desembolso Proveedor']).days) / 30
    dias2 = ((df['Fecha de Pago Proyectada'] - df['Fecha de Cierre de Subasta']).days) / 30
    tasa = 1 + df['Tasa Original (anticipado)']
    monto_fin = df['Monto Financiado']
    #POSIBLEMENTE HAY QUE AÑADIR ELIMINACIÓN DE FILAS SI EL RESULTADO ES NEGATIVO
    
    if df['Fecha de Desembolso Proveedor'] >= pd.Timestamp('2024-10-01'):
        
        return ((tasa ** dias1)-1) * monto_fin
    else:
        return ((tasa ** dias2)-1) * monto_fin
df['Interes proyectado'] = df.apply(int_pro , axis = 1)
df['Interes proyectado'] = df['Interes proyectado'].round(2)

print(df.shape)
#%% Descuento
# desc = pd.read_excel(r'C:/SCRIPTS JOSEPH/tablas auxiliares/descuentos foto.xlsx',)
# desc['Código de Subasta'] = desc['Código de Subasta'].str.lower()
# df['Código de Subasta'] = df['Código de Subasta'].str.lower()

query = '''
    SELECT
        codigo_de_subasta           AS "Código de Subasta",
        descuento_persona_natural   AS "Descuento Persona Natural"
    FROM prod_datalake_master.factoring__fac_auxiliar_percentage_discount
'''
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
desc = pd.DataFrame(resultados, columns = column_names)
desc["Descuento Persona Natural"] = desc["Descuento Persona Natural"].astype(float)
desc["Código de Subasta"] = desc["Código de Subasta"].str.strip()

df = df.merge(desc,
              on = 'Código de Subasta',
              how = 'left')
df['Descuento'] = df['Descuento Persona Natural']
df['Descuento'] = df['Descuento'].fillna(0)
del df['Descuento Persona Natural']

print(df.shape)
#%% Interés Neto
df['Interes Neto'] = df['Interes proyectado'] * (1 - df['Descuento'])
df['Interes Neto'] = df['Interes Neto'].round(2)

#%% Resultado de la Operación
df['Comisión de Estructuración'] = df['Comisión de Estructuración'].fillna(0)
df['Resultado de la Operación'] = df['Interes Neto'] + df['Comisión de Estructuración']
df['Resultado de la Operación'] = df['Resultado de la Operación'].round(2)

#%% Utilidad
query = """
select
    codigo_de_subasta AS "Código de Subasta",
    "0.95%" AS "0.95% aux"
from prod_datalake_master.factoring__fac_daily_operations
where _timestamp = (select distinct max(_timestamp) from prod_datalake_master.factoring__fac_daily_operations)

""" # posiblemente, esta parte habrá que cambiarla en el futuro, se toma una foto de la actualización anterior
cursor = conn.cursor()
cursor.execute(query)
# Obtener los resultados
resultados = cursor.fetchall()
# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]
# Convertir los resultados a un DataFrame de pandas
df_95 = pd.DataFrame(resultados, columns = column_names)

def limpiar_y_convertir(valor):
    if pd.isna(valor):
        return None
    try:
        # Elimina el símbolo '%' si existe y luego convierte a float
        valor_limpio = str(valor).replace('%', '').strip()
        return float(valor_limpio)
    except ValueError:
        # Si no se puede convertir, regresa None
        return None
df_95["0.95% aux"] = df_95["0.95% aux"].apply(limpiar_y_convertir)
df_95["0.95% aux"] = df_95["0.95% aux"] / 100
df_95 = df_95[   df_95["0.95% aux"] < 5   ]
# df_95["Código de Subasta"] = df_95["Código de Subasta"].str.lower()
df = df.merge(df_95,
              on  = 'Código de Subasta',
              how = 'left')
##### Plazo (anticipado) #########################################################
# def plazo(df):
#     if pd.isna(df['Fecha de Cierre de Subasta']):
#         return ((df['Fecha de Pago Proyectada'] - df['Fecha de Desembolso Proveedor']).days)
#     else:
#         return ((df['Fecha de Pago Proyectada'] - df['Fecha de Cierre de Subasta']).days)
def plazo(df):
    return ((df['Fecha de Pago Proyectada'] - df['Fecha de Desembolso Proveedor']).days)
df['Plazo (anticipado)'] = df.apply(plazo, axis = 1)

costo_variable = 0.0095
# def _095(df):
#     # if not pd.isna(df['0.95% aux']):
#     #     return df['0.95% aux']
#     if pd.isna(df['Fecha de Cierre de Subasta']):
#         dias1 = ((df['Fecha de Pago Proyectada'] - df['Fecha de Desembolso Proveedor']).days) / 30
#         return ( (costo_variable + 1) ** (dias1) ) - 1
#     else:
#         return ( (costo_variable + 1) ** (df['Plazo (anticipado)']/30 ) ) - 1
def _095(df):
    return ( (costo_variable + 1) ** (df['Plazo (anticipado)']/30 ) ) - 1
    
df['0.95% (anticipado)'] = df.apply(_095, axis = 1)
df['0.95% (anticipado)'] = df['0.95% (anticipado)'].round(2)
###############################################################################
df['Utilidad'] = df['Resultado de la Operación'] - ( (df['0.95% (anticipado)'] * df['Monto Financiado']) * (1 - df['Descuento']))
df['Utilidad'] = df['Utilidad'].round(2)

# del df['0.95% aux']

df_123 = df[ df.duplicated(subset = ['Código de Subasta'])]
print(df.shape)
#%% Utilidad en soles
def uti_soles (df):
    if df['Moneda'] == 'PEN':
        return df['Utilidad']
    else:
        return df['Utilidad'] * df['Tipo de Cambio']
df['Utilidad en soles'] = df.apply(uti_soles, axis = 1)
df['Utilidad en soles'] = df['Utilidad en soles'].round(2)

print(df.shape)
#%% Monto Financiado en Soles
def monto_fi_so(df):
    if df['Moneda'] == 'PEN':
        return df['Monto Financiado']
    else:
        return df['Monto Financiado'] * df['Tipo de Cambio']
df['Monto Financiado en Soles'] = df.apply(monto_fi_so, axis = 1)
df['Monto Financiado en Soles'] = df['Monto Financiado en Soles'].round(2)

#%% Plazo
df['Plazo'] = df['Plazo (anticipado)']
del df['Plazo (anticipado)']

#%% Diferencia Fechas Cierre Subasta vs Fecha de Desembolso
df['Diferencia Fechas Cierre Subasta vs Fecha de Desembolso'] = \
(df['Fecha de Desembolso Proveedor'] - df['Fecha de Cierre de Subasta']).dt.days

#%% generador de columnas de Stocks
# fechas_stock = [ 
#                 '2024-03',
#                 '2024-04',
#                 '2024-05',
#                 '2024-06',
#                 '2024-07',
#                 '2024-08',
#                 '2024-09',
#                 '2024-10',
#                 '2024-11',
#                 '2024-12',
#                 '2025-01',
#                 '2025-02',
#                 '2025-03',
#                 '2025-04'
#                 ]

# Eliminar columnas para poder ejecutar esto varias veces
columnas_a_eliminar = [col for col in df.columns if "Stock " in col]
df = df.drop(columns=columnas_a_eliminar)

def mes_siguiente_func(fecha_str):
    # Convertir el string 'YYYY-MM' en datetime
    fecha = datetime.strptime(fecha_str, "%Y-%m")
    
    # Si es diciembre, pasamos a enero del año siguiente
    if fecha.month == 12:
        siguiente = datetime(year=fecha.year + 1, month=1, day=1)
    else:
        siguiente = datetime(year=fecha.year, month=fecha.month + 1, day=1)
    return pd.Timestamp(siguiente)

def eomonth(fecha_str):
    fecha = pd.to_datetime(fecha_str)
    return fecha + pd.offsets.MonthEnd(0)

def calculo_stock(row, mes_actual, mes_siguiente, eom_mes_act):
    col_L = row['Fecha de Cierre de Subasta']
    col_R = row['Fecha de Cierre Final']
    
    if (col_L < mes_siguiente) and ((col_R > eom_mes_act) or (pd.isna(col_R))):
        return 1
    elif (col_L >= mes_actual) and (col_R < mes_siguiente) and not pd.isna(col_R):
        return 1
    else:
        return 0

# Loop principal
for i in fechas_stock:
    mes_actual = pd.Timestamp(i + "-01")  # Completamos al primer día del mes
    mes_siguiente = mes_siguiente_func(i)
    eom_mes_act = eomonth(mes_actual)
    
    df[f'Stock {i}'] = df.apply(lambda row: calculo_stock(row, mes_actual, mes_siguiente, eom_mes_act), axis = 1)

#%% Costo
df['Costo'] = (((((df['Comisión de Estructuración'] / df['Monto Financiado']) + 1 ) ** ( 30 / df['Plazo'])) - 1) + df['Tasa de Financiamiento'] ) * df['Monto Financiado']
df['Costo'] = df['Costo'].round(2)

#%% 0.95%
df['0.95%'] = df['0.95% (anticipado)']

del df['0.95% (anticipado)']

#%% Utilidad Casos Especiales
df['Utilidad Casos Especiales'] = df['Utilidad en soles']

#%% Monto Financiado Casos Especiales
df['Monto Financiado Casos Especiales'] = df['Monto Financiado en Soles']

#%% Tipo de Cliente 2
df['Tipo de Cliente 2'] = df['Tipo de Cliente']

#%% Fecha de cierre de subasta Quicksight (obsoleto)
df['Fecha de cierre de subasta Quicksight (obsoleto)'] = 'Fecha de Cierre de Subasta'

#%% Diferencia de Fecha Cierre de Subasta (Hubspot vs Quicksight)
df['Diferencia de Fecha Cierre de Subasta (Hubspot vs Quicksight)'] = 'obsoleto'

#%% Periodo Fecha de cierre de subasta quicksight
df['Periodo Fecha de cierre de subasta quicksight'] = 'obsoleto'

#%% Tipo de Cliente Factoring
df['Tipo de Producto'] = df['Tipo de Producto'].str.lower()
sin_tipo = df[pd.isna(df['Tipo de Producto'])]
if sin_tipo.shape[0] > 0:
    print('alerta, tipo de producto nulo')
    
def t_cli_fa(df):
    if df['Tipo de Producto'] == 'factoring':
        return df['Tipo de Cliente']
    else:
        return ''
df['Tipo de Cliente Factoring'] = df.apply(t_cli_fa, axis = 1)

#%% Tipo de Cliente Confirming
def t_cli_con(df):
    if df['Tipo de Producto'] == 'confirming':
        return df['Tipo de Cliente']
    else:
        return ''
df['Tipo de Cliente Confirming'] = df.apply(t_cli_con, axis = 1)

#%% Tasa All-IN
df['Tasa All-IN'] = df['Costo'] / df['Monto Financiado']

#%% Apoyo Comercial
query = '''
SELECT 
    codigo_de_subasta,
    APOYO_COMERCIAL,
    CASE
        WHEN apoyo_comercial LIKE 'Alejandra Tupai'  THEN 'Alejandra Tupia'
        ELSE apoyo_comercial
        END AS "APOYO_COMERCIAL_CORREGIDO"
FROM 
    PROD_DATALAKE_MASTER.hubspot__deal
WHERE codigo_de_subasta IS NOT NULL
and dealstage   NOT IN (14026018, 14026016)

'''
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
apoyo_comercial = pd.DataFrame(resultados, columns = column_names)
apoyo_comercial = apoyo_comercial[  ~pd.isna(apoyo_comercial["APOYO_COMERCIAL_CORREGIDO"])  ]

apoyo_duplicados = apoyo_comercial[apoyo_comercial.duplicated(subset=['codigo_de_subasta'])]
aver = apoyo_comercial[   apoyo_comercial['codigo_de_subasta'].isin(   apoyo_duplicados['codigo_de_subasta'])     ]

df =  df.merge(apoyo_comercial[['codigo_de_subasta', 'APOYO_COMERCIAL_CORREGIDO']],
               left_on  = 'Código de Subasta',
               right_on = 'codigo_de_subasta',
               how = 'left')
del df['codigo_de_subasta']

df.rename(columns = { 'APOYO_COMERCIAL_CORREGIDO' : 'Apoyo Comercial'}, inplace = True)

#%% columnas vacía
df['Stock vac1'] = ''

#%% columnas vacía
df['Stock vac2'] = ''

#%% Periodo Fecha de cierre de Comisiones
df['Periodo Fecha de cierre de Comisiones'] = df['Periodo Fecha de cierre de Comisiones (anticipado)'].copy()

del df['Periodo Fecha de cierre de Comisiones (anticipado)']

#%% columnas vacías
df['Stock vac3'] = ''
df['Stock vac4'] = ''
df['Stock vac5'] = ''

#%% Tipo de Cambio Fecha de Cierre de Subasta
# en función de Periodo Fecha Cierre de Subasta
df['Periodo YYYYMM'] = df['Periodo Fecha Cierre de Subasta'].dt.year * 100 + df['Periodo Fecha Cierre de Subasta'].dt.month

df = df.merge( tc[['codmes', 'exchange_rate']],
              left_on  = 'Periodo YYYYMM',
              right_on = 'codmes',
              how      = 'left')

del df['Periodo YYYYMM']
del df['codmes']

df.rename(columns = {'exchange_rate' : 'Tipo de Cambio Fecha de Cierre de Subasta'}, inplace = True)

#%% Monto Financiado Soles Fecha de Cierre de Subasta
def m_f_s_f_c_s(df):
    if df['Moneda'] == 'PEN':
        return df['Monto Financiado']
    else:
        return df['Monto Financiado'] * df['Tipo de Cambio Fecha de Cierre de Subasta']
df['Monto Financiado Soles Fecha de Cierre de Subasta'] = df.apply(m_f_s_f_c_s, axis = 1)
df['Monto Financiado Soles Fecha de Cierre de Subasta'] = df['Monto Financiado Soles Fecha de Cierre de Subasta'].round(2)

#%% Tasa Original
df['Tasa Original'] = df['Tasa Original (anticipado)']

del df['Tasa Original (anticipado)']

#%% CORRECCIÓN NUEVO O RECURRENTE
# esta lectura va a estar difícil xd, hay un poco de artificios de datos :v
# Tipo de Cliente
# Tipo de Proveedor
df['Tipo de Cliente (query)'] = df['Tipo de Cliente'].copy()
df['Tipo de Proveedor (query)'] = df['Tipo de Proveedor'].copy()
col_fecha_desembolso = 'Fecha de Desembolso Proveedor'
###############################################################################

min_cliente = df.pivot_table(values  = col_fecha_desembolso,
                             index   = 'RUC Cliente',
                             aggfunc = 'min'). reset_index()
min_cliente['NUEVO cliente'] = 'NUEVO'

df = df.merge(min_cliente,
              on  = ['RUC Cliente', col_fecha_desembolso],
              how = 'left')
df['n client'] = df.groupby('RUC Cliente').cumcount()
def tipo_cliente(df):
    if (df['NUEVO cliente'] == 'NUEVO') and (df['n client'] == 0):
        return 'NUEVO'
    else:
        return 'RECURRENTE'
df['Tipo de Cliente'] = df.apply(tipo_cliente, axis = 1)

validacion_cliente = df[ df['Tipo de Cliente'] == 'NUEVO' ]
validacion_cliente = validacion_cliente.pivot_table( values  = 'Tipo de Cliente',
                                                     index   = 'RUC Cliente',
                                                     aggfunc = 'count' ).reset_index()
if validacion_cliente['Tipo de Cliente'].max() > 1:
    print('alerta, algo está mal con el cálculo de tipo de cliente')
###############################################################################
min_proveedor = df.pivot_table(values  = col_fecha_desembolso,
                               index   = 'RUC Proveedor',
                               aggfunc = 'min'). reset_index()
min_proveedor['NUEVO proveedor'] = 'NUEVO'

df = df.merge(min_proveedor,
              on  = ['RUC Proveedor', col_fecha_desembolso],
              how = 'left')
df['n prove'] = df.groupby('RUC Proveedor').cumcount()
def tipo_prove(df):
    if (df['NUEVO proveedor'] == 'NUEVO') and (df['n prove'] == 0):
        return 'NUEVO'
    else:
        return 'RECURRENTE'
df['Tipo de Proveedor'] = df.apply(tipo_prove, axis = 1)

validacion_proveedor = df[ df['Tipo de Proveedor'] == 'NUEVO' ]
validacion_proveedor = validacion_proveedor.pivot_table( values  = 'Tipo de Proveedor',
                                                         index   = 'RUC Proveedor',
                                                         aggfunc = 'count' ).reset_index()
if validacion_proveedor['Tipo de Proveedor'].max() > 1:
    print('alerta, algo está mal con el cálculo de tipo de proveedor')

del df['Tipo de Cliente (query)']
del df['Tipo de Proveedor (query)']
del df['NUEVO cliente']
del df['n client']
del df['NUEVO proveedor']
del df['n prove']

#%% Moviendo la columna de la comision sin IGV al final
columna_mover = 'comision sin igv'
columnas = [c for c in df.columns if c != columna_mover] + [columna_mover]
df = df[columnas]

columna_mover = "EJECUTIVO SEGÚN fac_requests"
columnas = [c for c in df.columns if c != columna_mover] + [columna_mover]
df = df[columnas]

#%%
# incluir fecha de pago confirmada por el DEUDOR (se debe usar para calcular la morosidad)
# en el hubspot está como FECHA CONFIRMADA - EXCEPCIÓN
query = '''  
SELECT 
    FR.CODE                                     AS "Código de Subasta" ,
    HT.fecha_de_pago___confirmado_por_correo    AS "Fecha de pago confirmado por Deudor"
FROM prod_datalake_analytics.fac_requests AS FR 
LEFT JOIN (     select * 
                from prod_datalake_analytics.hubspot_tickets 
                where subject is not null
                and hs_pipeline = 26417284) AS HT
ON HT.SUBJECT = FR.CODE
and FR.status = 'closed'

'''

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
fecha_pago_confirmado = pd.DataFrame(resultados, columns = column_names)

df = df.merge(fecha_pago_confirmado,
              on  = "Código de Subasta",
              how = "left")

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%|
print('fin')

#%% GENERAR BACKUP
if guardar_backup == True:
    os.chdir(r'C:\Users\Joseph Montoya\Desktop\pruebas')
    
    df.to_excel(nombre_backup, index = False)

#%% cálculo comparativo
# df_pivoteado = df.pivot_table (index   = 'Periodo Fecha de Desembolso',
#                                values  = ['Código de Subasta', 'Monto Financiado en Soles', 'Utilidad en soles'],
#                                aggfunc = ['count', 'sum', 'sum'])
df_google_sheet = pd.read_excel(r'C:/Users/Joseph Montoya/Desktop/comparación resultados/google sheet 08 05 2024.xlsx',
                                sheet_name = 'Hoja1')

# df_google_sheet['Código de Subasta'] =  df_google_sheet['Código de Subasta'].str.lower()
df_google_sheet.drop_duplicates(subset='Código de Subasta', inplace = True)

# df['Código de Subasta']    = df ['Código de Subasta'].str.lower()
df.drop_duplicates(subset='Código de Subasta', inplace = True)

# Obtener la intersección de códigos
codigos_comunes = set(df_google_sheet['Código de Subasta']).intersection(df['Código de Subasta'])

# Filtrar cada DataFrame
df_google_sheet= df_google_sheet[df_google_sheet['Código de Subasta'].isin(codigos_comunes)]
df = df[df['Código de Subasta'].isin(codigos_comunes)]
###############################################################################
os.chdir(r'C:\Users\Joseph Montoya\Desktop\pruebas')

df_google_sheet = df_google_sheet.sort_values(by = 'Código de Subasta', ascending = False)
df = df.sort_values(by = 'Código de Subasta', ascending = False)

# df_google_sheet.to_excel('g_sheet.xlsx', index = False)
# df.to_excel('df_script.xlsx', index = False)
columnas_orden[columnas_orden.index('Ejecutivo (cod)')] = 'Ejecutivo' ## <<<<<<<<<<<<<<<<<<<<<<

# df = df[columnas_orden + ['Tipo de Cliente (query)', 'Tipo de Proveedor (query)']]
# df_google_sheet = df_google_sheet[columnas_orden]
'-----------------------------------------------------------------------------'
df_google_sheet.columns = [str(col) + "-gs" for col in df_google_sheet.columns]
unidos = df.merge(df_google_sheet,
                 left_on  = 'Código de Subasta',
                 right_on = 'Código de Subasta-gs',
                 how      = 'left')
unidos = unidos[sorted(unidos.columns)]

unidos.to_excel('unidos_3.xlsx', index = False)
###############################################################################
# Asegúrate de que los valores sean numéricos
df['Monto Financiado en Soles'] = pd.to_numeric(df['Monto Financiado en Soles'], errors='coerce')
df['Utilidad en soles'] = pd.to_numeric(df['Utilidad en soles'], errors='coerce')

conteo = df.pivot_table (index = 'Periodo Fecha de Desembolso',
                               values = 'Código de Subasta',
                               aggfunc = 'count',
                               columns = 'Tipo de Producto').reset_index()
m_fin = df.pivot_table (index = 'Periodo Fecha de Desembolso',
                               values = 'Monto Financiado en Soles',
                               aggfunc = 'sum',
                               columns = 'Tipo de Producto').reset_index()

m_uti = df.pivot_table (index = 'Periodo Fecha de Desembolso',
                               values = 'Utilidad en soles',
                               aggfunc = 'sum',
                               columns = 'Tipo de Producto').reset_index()

union = conteo.merge(m_fin,
                      on = 'Periodo Fecha de Desembolso',
                      how = 'left')

union = union.merge(m_uti,
                      on = 'Periodo Fecha de Desembolso',
                      how = 'left')

os.chdir(r'C:\Users\Joseph Montoya\Desktop\comparación resultados')
union.to_excel('calculo script.xlsx', index = False)

#%%

df = df_google_sheet.copy()
df['Monto Financiado en Soles'] = pd.to_numeric(df['Monto Financiado en Soles'], errors='coerce')
df['Utilidad en soles'] = pd.to_numeric(df['Utilidad en soles'], errors='coerce')

conteo = df.pivot_table (index = 'Periodo Fecha de Desembolso',
                               values = 'Código de Subasta',
                               aggfunc = 'count',
                               columns = 'Tipo de Producto').reset_index()
m_fin = df.pivot_table (index = 'Periodo Fecha de Desembolso',
                               values = 'Monto Financiado en Soles',
                               aggfunc = 'sum',
                               columns = 'Tipo de Producto').reset_index()

m_uti = df.pivot_table (index = 'Periodo Fecha de Desembolso',
                               values = 'Utilidad en soles',
                               aggfunc = 'sum',
                               columns = 'Tipo de Producto').reset_index()

union = conteo.merge(m_fin,
                      on = 'Periodo Fecha de Desembolso',
                      how = 'left')

union = union.merge(m_uti,
                      on = 'Periodo Fecha de Desembolso',
                      how = 'left')

os.chdir(r'C:\Users\Joseph Montoya\Desktop\comparación resultados')
union.to_excel('calculo google sheet.xlsx', index = False)

#%%%%

pivot = df.pivot_table(
    index='Periodo Fecha de Desembolso',
    columns='Tipo de Producto',
    values=['Código de Subasta', 'Monto Financiado en Soles', 'Utilidad en soles'],
    aggfunc={
        'Código de Subasta': 'count',
        'Monto Financiado en Soles': 'sum',
        'Utilidad en soles': 'sum'
    }
).reset_index()




