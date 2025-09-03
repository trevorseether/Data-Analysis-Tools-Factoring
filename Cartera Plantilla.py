# -*- coding: utf-8 -*-
"""
Created on Fri May 23 09:46:23 2025

@author: Joseph Montoya
"""

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
import os

# import shutil
from datetime import datetime

import warnings
warnings.filterwarnings("ignore")

os.chdir(r'C:\Users\Joseph Montoya\Desktop\pruebas')
#%%
os.chdir(r'C:\Users\Joseph Montoya\Desktop\cartera factoring')

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
query = """

SELECT 
  CAST(
    date_trunc('month', date_add('month', 1, fecha_cierre)) - interval '1' day 
    AS date
  ) AS fecha_cierre_corregido,

  CAST(
    date_trunc('month', date_add('month', 1, transfer_date)) - interval '1' day 
    AS date
  ) AS fecha_desembolso,

  CASE 
    WHEN CAST(date_trunc('month', date_add('month', 1, fecha_cierre)) - interval '1' day AS date) = 
         CAST(date_trunc('month', date_add('month', 1, transfer_date)) - interval '1' day AS date)
    THEN 'desembolsado en el mes'
    ELSE ''
  END AS "desembolsado flag",

  *
FROM prod_datalake_analytics.fac_outstanding;

"""
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
df_outstanding = pd.DataFrame(resultados, columns = column_names)

#%%
orden_columnas = ['202012', '202101', '202102', '202103', '202104', '202105', '202106',
                  '202107', '202108', '202109', '202110', '202111', '202112', '202201',
                  '202202', '202203', '202204', '202205', '202206', '202207', '202208',
                  '202209', '202210', '202211', '202212', '202301', '202302', '202303',
                  '202304', '202305', '202306', '202307', '202308', '202309', '202310',
                  '202311', '202312', '202401', '202402', '202403', '202404', '202405',
                  '202406', '202407', '202408', '202409', '202410', '202411', '202412',
                  '202501', '202502', '202503', '202504', '202505']
orden_columnas = list(map(int, orden_columnas))

# df_outstanding = df_outstanding[ df_outstanding['actual_status'] != 'finalizado' ]

out_pivot = df_outstanding.pivot_table(columns = 'codmes',
                                       values  = 'remaining_capital_soles',
                                       aggfunc = 'sum').reset_index()

def dias_atraso_categorical(df):
    if (df['dias_atraso'] > 0)  and  (df['dias_atraso'] <= 30):
        return '1 a 30 días'
    if (df['dias_atraso'] > 30)  and  (df['dias_atraso'] <= 60):
        return '31 a 60 días'
    if (df['dias_atraso'] > 60)  and  (df['dias_atraso'] <= 90):
        return '61 a 90 días'
    if (df['dias_atraso'] > 90)  and  (df['dias_atraso'] <= 120):
        return '91 a 120 días'
    if (df['dias_atraso'] > 120):
        return '+ 120 días'
df_outstanding['dias categorical'] = df_outstanding.apply(dias_atraso_categorical, axis = 1)

def moroso(df):
    if (df['dias_atraso'] > 0):
        return 'moroso'
    else:
        return ""
df_outstanding['moroso'] = df_outstanding.apply(moroso, axis = 1)

moroso_pivot = df_outstanding[df_outstanding['moroso'] == 'moroso'].pivot_table(columns = 'codmes',
                                                                                values  = 'remaining_capital_soles',
                                                                                aggfunc = 'sum')

moroso_1_30 = df_outstanding[df_outstanding['dias categorical'] == '1 a 30 días'].pivot_table(columns = 'codmes',
                                                                                values  = 'remaining_capital_soles',
                                                                                aggfunc = 'sum')
moroso_31_60 = df_outstanding[df_outstanding['dias categorical'] == '31 a 60 días'].pivot_table(columns = 'codmes',
                                                                                values  = 'remaining_capital_soles',
                                                                                aggfunc = 'sum')
moroso_61_90 = df_outstanding[df_outstanding['dias categorical'] == '61 a 90 días'].pivot_table(columns = 'codmes',
                                                                                values  = 'remaining_capital_soles',
                                                                                aggfunc = 'sum')
moroso_91_120 = df_outstanding[df_outstanding['dias categorical'] == '91 a 120 días'].pivot_table(columns = 'codmes',
                                                                                values  = 'remaining_capital_soles',
                                                                                aggfunc = 'sum')
moroso_120 = df_outstanding[df_outstanding['dias categorical'] == '+ 120 días'].pivot_table(columns = 'codmes',
                                                                                values  = 'remaining_capital_soles',
                                                                                aggfunc = 'sum')

#%%
cartera = pd.concat([out_pivot, 
                     moroso_pivot,
                     moroso_1_30,
                     moroso_31_60,
                     moroso_61_90,
                     moroso_91_120,
                     moroso_120], ignore_index=True)
cartera = cartera.fillna(0)
cartera = cartera[orden_columnas]
###calcular provisiones aquí

nueva_fila = cartera.loc[6] / cartera.loc[0]
cartera.loc['morosidad 120'] = nueva_fila

# cartera.to_excel(r'C:\Users\Joseph Montoya\Desktop\pruebas\cartera_prueba.xlsx',
#                  index = False)

#%% CARTERA SEGÚN MONEDA
car_moneda = df_outstanding.pivot_table(columns = 'codmes',
                                        values  = 'remaining_capital_soles',
                                        index   = 'currency_auctions',
                                        aggfunc = 'sum', 
                                        margins = True, 
                                        margins_name ='suma total')
car_moneda = car_moneda[orden_columnas]
car_moneda_porcentaje = car_moneda.copy()

fila1 = car_moneda_porcentaje.loc['PEN'] / car_moneda_porcentaje.loc['suma total']
fila2 = car_moneda_porcentaje.loc['USD'] / car_moneda_porcentaje.loc['suma total']

car_moneda_porcentaje.loc['PEN'] = fila1
car_moneda_porcentaje.loc['USD'] = fila2

#columna vacía
cartera.loc['Cartera según moneda:'] = [None] * len(cartera.columns)

suma = car_moneda_porcentaje.loc['PEN'] + car_moneda_porcentaje.loc['USD']
car_moneda_porcentaje.loc['suma total'] = suma
car_moneda_porcentaje = car_moneda_porcentaje.drop(index='suma total')

cartera = pd.concat([cartera, 
                     car_moneda_porcentaje], ignore_index=True)

#%% top 1, 5, 10 y 20 deudores y proveedores
union_meses = pd.DataFrame()

# for i in orden_columnas:
#     corte_mensual = i
#     agrupamiento = df_outstanding[df_outstanding['codmes'] == corte_mensual].pivot_table(index  = 'client_ruc',
#                                                                                          values = 'remaining_capital_soles',
#                                                                                          columns = 'codmes').reset_index()
#     agrupamiento = agrupamiento.sort_values(by=[corte_mensual], ascending=[False])
    
#     union_meses = pd.concat([union_meses, agrupamiento], ignore_index=False)

for mes in orden_columnas:
    # Filtrar y agrupar sumando montos por cliente para ese mes
    df_mes = df_outstanding[df_outstanding['codmes'] == mes].groupby('client_ruc', as_index=False)['remaining_capital_soles'].sum()
    
    # Ordenar de mayor a menor
    df_mes = df_mes.sort_values(by='remaining_capital_soles', ascending=False).head(20)
    
    # Renombrar columnas para identificar el mes
    df_mes = df_mes.rename(columns={
        'client_ruc': f'client_ruc_{mes}',
        'remaining_capital_soles': f'remaining_capital_{mes}'
    }).reset_index(drop=True)
    
    if union_meses.empty:
        union_meses = df_mes
    else:
        # Concatenar horizontalmente por índice (filas)
        union_meses = pd.concat([union_meses, df_mes], axis=1)

#%%
def top_deudores_clientes(empresa):
    '''
    agrupamiento de clientes o deudores en función del top
    
    '''
    top_summary = pd.DataFrame()
    
    for mes in orden_columnas:
        # Agrupar, ordenar, y tomar top 20
        df_mes = df_outstanding[df_outstanding['codmes'] == mes].groupby(empresa, as_index=False)['remaining_capital_soles'].sum()
        df_mes = df_mes.sort_values(by='remaining_capital_soles', ascending=False).head(20).reset_index(drop=True)
    
        # Calcular sumas acumuladas para top 1, 5, 10, 20
        top_n = [1, 5, 10, 20]
        resumen_mes = {'top_n': top_n}
    
        sumas = [df_mes.head(n)['remaining_capital_soles'].sum() for n in top_n]
        resumen_mes[f'sum_{mes}'] = sumas
    
        resumen_mes_df = pd.DataFrame(resumen_mes)
    
        # Si es la primera vez, inicia el dataframe
        if top_summary.empty:
            top_summary = resumen_mes_df
        else:
            top_summary = top_summary.merge(resumen_mes_df, on='top_n')
        
    return top_summary

top_clientes    = top_deudores_clientes('client_ruc')
top_proveedores = top_deudores_clientes('provider_ruc')

#%%
cartera.to_excel('cartera.xlsx')

# top_clientes.to_excel('top_clientes.xlsx')

# top_proveedores.to_excel('top_proveedores.xlsx')

#%% desembolsados en el periodo

df_outstanding['anio'] = pd.to_datetime(df_outstanding['fecha_desembolso']).dt.year
df_outstanding['mes'] = pd.to_datetime(df_outstanding['fecha_desembolso']).dt.month

desembolsados = df_outstanding[ df_outstanding["desembolsado flag" ]== "desembolsado en el mes" ]
desembolsados = desembolsados[ desembolsados['codmes'] <= 202501]

pivot_desembolsados = desembolsados.pivot_table(values = "amount_financed_soles",
                                                index   = 'mes',
                                                columns = 'anio',
                                                aggfunc = 'sum').reset_index()


pivot_desembolsados.to_excel('desembolsados.xlsx', index = False)

###  Stock de colocaciones por moneda       ###########################
desem2 = desembolsados.pivot_table(columns = 'currency_auctions',
                                    values = 'remaining_capital',
                                    index  = 'anio',
                                    aggfunc = 'sum')
desem3 = desembolsados.pivot_table(#columns = 'currency_auctions',
                                    values = 'remaining_capital_soles',
                                    index  = 'anio',
                                    aggfunc = 'sum')

#%%% nro de clientes y deudores por periodo
hasta_enero_2025 = df_outstanding[ df_outstanding['codmes'] <= 202501]
nro_cli = hasta_enero_2025.pivot_table(values  = 'client_ruc',
                                     columns = 'anio',
                                     aggfunc = 'nunique')

nro_pro = hasta_enero_2025.pivot_table(values  = 'provider_ruc',
                                     columns = 'anio',
                                     aggfunc = 'nunique')

nro_distintos = pd.concat([nro_cli, nro_pro ], ignore_index= True)

nro_distintos.to_excel("cantidad de clientes y deudores por año.xlsx",
                       index = False)

#%% plazo promedio de cartera
df_outstanding['dias plazo'] = (df_outstanding['e_payment_date'] - df_outstanding['transfer_date']).dt.days
df_outstanding = df_outstanding[ df_outstanding['codmes'] <= 202501]
promedio_plazo = df_outstanding.pivot_table(values  = 'dias plazo',
                                            index   = 'anio',
                                            aggfunc = 'mean')
df_outstanding['transfer_date']
df_outstanding['payment_date']

df_outstanding['e_payment_date']

#%% nro de facturas (cambias proceso, sacar del fac_requests LJ offline de hubspot)
nro_facturas = df_outstanding.pivot_table(values  = 'invoice_count',
                                          index   = 'anio',
                                          aggfunc = 'sum')

#%% tasa promedio de originación
tasa_promedio_cartera = df_outstanding.pivot_table(values  = 'assigned_financing_rate',
                                                   index   = 'anio',
                                                   aggfunc = 'mean')
tasa_promedio_desembolsos = desembolsados.pivot_table(values  = 'assigned_financing_rate',
                                                   index   = 'anio',
                                                   aggfunc = 'mean')

#%%
top_10k = pd.read_excel(r'G:/.shortcut-targets-by-id/1alT0hxGsi0dfv0NYh_LB4NrT2tKEgPK8/Cierre Factoring/Archivos/TOP 10 MIL - 2023.xlsx')
top_10k['RUC'] = top_10k['RUC'].astype(str)

df_outstanding = df_outstanding.merge(top_10k[['RUC', 'Razón Social', 'Ranking 2023']],
                                      left_on  = 'client_ruc',
                                      right_on = 'RUC',
                                      how      = 'left')

df_outstanding['Ranking 2023'] = df_outstanding['Ranking 2023'].fillna(0)

def etiqueta_ranking(df):
    if (df['Ranking 2023'] > 0) and (df['Ranking 2023'] <= 500):
        return "a. 1 a 500"
    if (df['Ranking 2023'] > 500) and (df['Ranking 2023'] <= 1000):
        return "b"
    if (df['Ranking 2023'] > 1000) and (df['Ranking 2023'] <= 2000):
        return "c"
    if (df['Ranking 2023'] > 2000) and (df['Ranking 2023'] <= 5000):
        return "d"
    if (df['Ranking 2023'] > 5000) and (df['Ranking 2023'] <= 10000):
        return "e"
    if (df['Ranking 2023'] == 0):
        return "f"
df_outstanding['ranking txt'] = df_outstanding.apply(etiqueta_ranking, axis = 1)

fecha = 202501
concentracion_cartera_tops = df_outstanding[ df_outstanding['codmes'] == fecha ]

ranking_pivot = concentracion_cartera_tops.pivot_table(values = 'remaining_capital_soles',
                                                       index = 'ranking txt',
                                                       aggfunc = 'sum')

#%% empresas enero 2025
top_deudores_202501 = concentracion_cartera_tops.pivot_table(values  = 'remaining_capital_soles',
                                                             index   = 'client_name' ,
                                                             aggfunc = 'sum')

top_deudores_202501 = top_deudores_202501.sort_values(by='remaining_capital_soles', ascending=False).head(60)

###############################################################################
top_proveedores_202501 = concentracion_cartera_tops.pivot_table(values  = 'remaining_capital_soles',
                                                             index   = 'provider_name' ,
                                                             aggfunc = 'sum')

top_proveedores_202501 = top_proveedores_202501.sort_values(by='remaining_capital_soles', ascending=False).head(58)

#%% cartera por calificacion sbs

query = f'''select 
tipo_documento, nro_documento, nombres_razonsocial, paterno_nombrecomercial, materno,   calificacion_sbs_microf
from "prod_datalake_master"."prestamype__sentinel_batch"
where periodo_rcc = '{fecha}'
'''
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
calificacion = pd.DataFrame(resultados, columns = column_names)
calificacion.drop_duplicates(subset='nro_documento', inplace = True)

concentracion_cartera_tops = concentracion_cartera_tops.merge(calificacion,
                                                              left_on  = 'client_ruc',
                                                              right_on = 'nro_documento',
                                                              how      = 'left')

concentracion_cartera_tops['calificacion_sbs_microf'] = concentracion_cartera_tops['calificacion_sbs_microf'].fillna('SIN CALIFICACION')

cartera_segun_calif = concentracion_cartera_tops.pivot_table(values = 'remaining_capital_soles',
                                                             index = 'calificacion_sbs_microf',
                                                             aggfunc = 'sum')
#%% sectores / grupos económicos:
query = '''select
sectores_economicos, ruc, grupo_economico

from prod_datalake_master.hubspot__company  
where ruc is not null 
'''
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
sector_grupo = pd.DataFrame(resultados, columns = column_names)

sector_grupo['ruc'] = sector_grupo['ruc'].astype(str).str.replace('.0', "", regex = False)

concentracion_cartera_tops = concentracion_cartera_tops.merge(sector_grupo,
                                                              left_on  = 'client_ruc',
                                                              right_on = 'ruc',
                                                              how      = 'left')

concentracion_cartera_tops['grupo_economico'] = concentracion_cartera_tops['grupo_economico'].fillna("Sin Grupo")
concentracion_cartera_tops['grupo_economico'] = concentracion_cartera_tops['grupo_economico'].replace('NO TIENE', "Sin Grupo")

pivot_sector = concentracion_cartera_tops.pivot_table(values  = 'remaining_capital_soles',
                                                      index   = 'sectores_economicos',
                                                      aggfunc = 'sum')

pivot_grupo = concentracion_cartera_tops.pivot_table(values   = 'remaining_capital_soles',
                                                      index   = 'grupo_economico',
                                                      aggfunc = 'sum').sort_values(by = 'remaining_capital_soles', 
                                                      ascending = False).\
                                                     head(15)

#%% el mismo agregamiento pero por proveedor
sector_grupo.columns = [f"{col}-pro" for col in sector_grupo.columns]
concentracion_cartera_tops = concentracion_cartera_tops.merge(sector_grupo,
                                                              left_on  = 'provider_ruc',
                                                              right_on = 'ruc-pro',
                                                              how      = 'left')
concentracion_cartera_tops['grupo_economico-pro'] = concentracion_cartera_tops['grupo_economico-pro'].fillna("Sin Grupo")
concentracion_cartera_tops['grupo_economico-pro'] = concentracion_cartera_tops['grupo_economico-pro'].replace('NO TIENE', "Sin Grupo")

pivot_sector_pro = concentracion_cartera_tops.pivot_table(values  = 'remaining_capital_soles',
                                                      index   = 'sectores_economicos-pro',
                                                      aggfunc = 'sum')

pivot_grupo_pro = concentracion_cartera_tops.pivot_table(values   = 'remaining_capital_soles',
                                                      index   = 'grupo_economico-pro',
                                                      aggfunc = 'sum')

#%% RECAUDACIÓN
query = '''
SELECT
  CAST(
    date_trunc('month', date_add('month', 1, a."date")) - interval '1' day 
    AS date
  ) AS "MES PAGO"
    , a.currency
    , a."date"      AS "FECHA PAGO"
    , b.code        AS "Código de Subasta"
    , A.distribution_provider_amount_payment_client   as "cobranza"
    , a.distribution_provider_capital_with_INTeres    as "cobranza por validar"
    --, a.*

FROM prod_datalake_analytics.fac_client_payment_payments as a
left join prod_datalake_analytics.fac_client_payments as c  
on a.client_payment_id = c._id
left join prod_datalake_analytics.fac_requests  as b
on c.request_id = b._id

where b.status = 'closed'

'''
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
cobranza = pd.DataFrame(resultados, columns = column_names)
cobranza["MES PAGO"] = pd.to_datetime(cobranza["MES PAGO"])
###############################################################################
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
tc['fin_de_mes'] = tc['tc_date'] + pd.offsets.MonthEnd(0)

#%%
cobranza = cobranza.merge(tc[['fin_de_mes', 'exchange_rate']],
                          left_on  = "MES PAGO",
                          right_on = 'fin_de_mes',
                          how      = 'left')

#%%
def pago_sol(df):
    columna_cobranza = "cobranza"  # "cobranza" "cobranza por validar"
    
    if df['currency'] == 'PEN':
        return df[columna_cobranza]
    if df['currency'] == 'USD':
        return df[columna_cobranza]*df["exchange_rate"]
cobranza['pago solarizado'] = cobranza.apply(pago_sol, axis = 1)

cob_agrupado = cobranza.pivot_table(values = 'pago solarizado',
                                    index  = 'MES PAGO',
                                    columns = 'currency',
                                    aggfunc = 'sum')




