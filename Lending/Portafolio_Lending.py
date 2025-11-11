# -*- coding: utf-8 -*-
"""
Created on Wed Oct 15 09:59:08 2025

@author: Joseph Montoya
"""

# =============================================================================
# CARTERA / COSECHA LENDING
# =============================================================================

import pandas as pd
import os
import numpy as np
# pd.options.display.date_format = "%Y-%m-%d"

import boto3
import json
import io
# import os
# from datetime import datetime

from pyathena import connect

#%%
fecha_corte = '2025-11-30'

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
#%% ops
df_ops = pd.read_excel(r'G:/Mi unidad/BD_Cobranzas.xlsm',
                       sheet_name= 'Prestamos gestionados',
                            dtype= {'Numero de documento'         : str,
                                    'RUC'                         : str,
                                    'Monto prestado'              : float,
                                    'TEM'                         : float,
                                    'TEA'                         : float,
                                    'Interes ganados'             : float,
                                    'Saldo a pagar'               : float,
                                    'Numero de cuenta del cliente': str,
                                    'Fecha de desembolso'         : str,
                                    'Fecha de finalización'       : str})
df_ops.columns = (
    df_ops.columns
      .str.strip()              # elimina espacios al inicio/fin
      .str.lower()              # convierte todo a minúsculas
      .str.replace(' ', '_')    # reemplaza espacios por guiones bajos
      .str.replace('á', 'a')    # reemplaza espacios por guiones bajos
      .str.replace('é', 'e')    # reemplaza espacios por guiones bajos
      .str.replace('í', 'i')    # reemplaza espacios por guiones bajos
      .str.replace('ó', 'o')    # reemplaza espacios por guiones bajos
      .str.replace('u', 'u')    # reemplaza espacios por guiones bajos

                 )
df_ops['codigo_de_contrato'] = df_ops['codigo_de_contrato'].str.strip()
df_ops['codigo_de_prestamo'] = df_ops['codigo_de_prestamo'].str.strip()

df_ops = df_ops.dropna(subset=['fecha_de_desembolso', 'moneda'])
df_ops['nro_de_cuotas'] = df_ops['nro_de_cuotas'].astype(int)
df_ops['dias_de_mora'] = df_ops['dias_de_mora'].fillna(0).astype(int)
###############################################################################
bd_pagos = pd.read_excel(r'G:/Mi unidad/BD_Cobranzas.xlsm',
                         sheet_name = 'BD PAGOS',
                         dtype = { 'Numero de documento'       : str,
                                   'RUC'                       : str,
                                   'Fecha de pago del cliente' : str}
                         )

bd_pagos['Monto pagado']       = bd_pagos['Monto pagado'].fillna(0).astype(float)
bd_pagos['Capital pagado']     = bd_pagos['Capital pagado'].fillna(0).astype(float)
bd_pagos['Intereses generado'] = bd_pagos['Intereses generado'].fillna(0).astype(float)
bd_pagos['Monto moratorio']    = bd_pagos['Monto moratorio'].fillna(0).astype(float)
bd_pagos['Saldo a favor']      = bd_pagos['Saldo a favor'].fillna(0).astype(float)

bd_pagos['Saldo por cancelar'] = bd_pagos['Saldo por cancelar'].fillna(0).astype(float)

bd_pagos = bd_pagos.dropna(subset=['Codigo de prestamo', 'Tipo de persona', 
                                   'Tipo de documento', 'Numero de documento'])

#%% Parseando fechas de desembolso
def parse_dates(date_str):
    '''
    Parameters
    ----------
    date_str : Es el formato que va a analizar dentro de la columna del DataFrame.

    Returns
    -------
    Si el date_str tiene una estructura compatible con los formatos preestablecidos
    para su iteración, la convertirá en un DateTime

    '''
    #formatos en los cuales se tratará de convertir a DateTime
    formatos = ['%d-%m-%Y %H:%M:%S'    ,
                '%d-%m-%Y'             ,
                '%d/%m/%Y %H:%M:%S'    ,
                '%d/%m/%Y'             ,
                '%Y-%m-%d %H:%M:%S'    ,
                '%Y%m%d', '%Y-%m-%d'   , 
                '%Y-%m-%d %H:%M:%S'    , 
                '%Y/%m/%d %H:%M:%S'    ,
                '%Y-%m-%d %H:%M:%S PM' ,
                '%Y-%m-%d %H:%M:%S AM' ,
                '%Y/%m/%d %H:%M:%S PM' ,
                '%Y/%m/%d %H:%M:%S AM'     ]

    for formato in formatos:
        try:
            return pd.to_datetime(date_str, format=formato)
        except ValueError:
            pass
    return pd.NaT

df_ops['fecha_de_desembolso']    = df_ops['fecha_de_desembolso'].apply(parse_dates)
df_ops['fecha_de_finalizacion'] = df_ops['fecha_de_finalizacion'].apply(parse_dates)

bd_pagos['Fecha de pago del cliente'] = bd_pagos['Fecha de pago del cliente'].apply(parse_dates)
bd_pagos['Fecha de pago del cliente'] = pd.to_datetime(bd_pagos['Fecha de pago del cliente'])

#%% Fecha finalización de la operación
# cálculo para validaciones sacando la máxima fecha del BD_PAGOS
fecha_fin = bd_pagos.pivot_table(values  = 'Fecha de pago del cliente',
                                 index   = 'Codigo de prestamo',
                                 aggfunc = 'max').reset_index()
df_ops = df_ops.merge(fecha_fin,
                      left_on   = 'codigo_de_prestamo',
                      right_on  = 'Codigo de prestamo',
                      how       = 'left')

df_ops['fecha_de_finalizacion'] = df_ops['fecha_de_finalizacion'].fillna('')
df_ops['fecha_de_finalizacion'] = df_ops['fecha_de_finalizacion'].astype(str).str.replace('NaT', '')

df_ops['Fecha de pago del cliente'] = df_ops['Fecha de pago del cliente'].fillna('')
df_ops['Fecha de pago del cliente'] = df_ops['Fecha de pago del cliente'].astype(str).str.replace('NaT', '')

df_ops['validación fecha de finalizacion'] = np.where(df_ops['fecha_de_finalizacion'] != df_ops['Fecha de pago del cliente'],
                                                      'alerta',
                                                      '')
alerta = df_ops[ df_ops['validación fecha de finalizacion'] == 'alerta' ]

if alerta.shape[0] > 0:
    print('alerta de casos raros')
    print(alerta)
    
df_ops['fecha_de_finalizacion'] = pd.to_datetime(df_ops['fecha_de_finalizacion'])
df_ops['Fecha de pago del cliente'] = pd.to_datetime(df_ops['Fecha de pago del cliente'])

del df_ops['Codigo de prestamo']
del df_ops['validación fecha de finalizacion']

######### parchamiento para pruebas ############################


#%%
# Crear una lista de fechas
fechas = pd.date_range(start = '2025-09-30',  # no tocar este valor
                       end = fecha_corte, freq = 'ME')

# Crear un DataFrame con las fechas
df_fechas = pd.DataFrame({'Fecha_corte': fechas})
df_fechas['Fecha_corte'] = pd.to_datetime(df_fechas['Fecha_corte'])

#%% Obtener tipo de cambio del mes
query = '''
    select * from prod_datalake_analytics.tipo_cambio_sbs_jmontoya
'''
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
tc = pd.DataFrame(resultados, columns=column_names)
tc['pk'] = pd.to_datetime(tc['pk'])
tc['mes_tc'] = pd.to_datetime(tc['mes_tc'])

tc = tc[tc['mes_tc'].isin(df_fechas['Fecha_corte'])]

df_fechas = df_fechas.merge(tc[['mes_tc', 'exchange_rate']],
                            left_on = 'Fecha_corte',
                            right_on = 'mes_tc',
                            how = 'left'
                            )

df_fechas['exchange_rate'] = df_fechas['exchange_rate'].fillna(3.500)

del df_fechas['mes_tc']

#%% Obtener ejecutivo comercial
query = '''
    SELECT 
        codigo_de_negocio___tandia_lending as "codigo_de_prestamo", 
        CASE
            WHEN hubspot_owner_id = '407980810' THEN 'Priscila Quispe (pquispe@tandia.pe)'
            ELSE hubspot_owner_id
            END AS propietario_negocio
    FROM prod_datalake_master.hubspot__deal
    WHERE pipeline = '762912077'
    AND codigo_de_negocio___tandia_lending IS NOT NULL;
    
    '''
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
ejecutivo = pd.DataFrame(resultados, columns=column_names)

df_ops = df_ops.merge(ejecutivo,
                      on  = "codigo_de_prestamo",
                      how = 'left')

#%%
# Producto cartesiano (todas las combinaciones)
df_temp = df_fechas.assign(key=1).merge(df_ops.assign(key=1), 
                                        on='key', 
                                        how='left').drop('key', axis=1)

# Filtrar solo cuando la fecha está entre desembolso y cancelación
fecha_max = df_temp['Fecha_corte'].max()
df_temp['fecha_de_finalizacion'] = pd.to_datetime(df_temp['fecha_de_finalizacion'])  # aseguramos tipo datetime
df_temp['Fecha_corte'] = pd.to_datetime(df_temp['Fecha_corte'])
df_temp['mes_finalizacion'] = df_temp['fecha_de_finalizacion'].dt.to_period('M').dt.to_timestamp('M')
df_temp['mes_desembolso'] = df_temp['fecha_de_desembolso'].dt.to_period('M').dt.to_timestamp('M')

def col_aux(df):
    if df['mes_finalizacion'] == df['Fecha_corte']:
        return 'finalizado'
    if df['fecha_de_desembolso'] > df['Fecha_corte']:
        return 'eliminar'
    if df['mes_finalizacion'] < df['Fecha_corte']:
        return 'eliminar'
    if df['fecha_de_desembolso'] <= df['Fecha_corte']:
        return 'vigente'

df_temp['aux col filtrado'] = df_temp.apply(col_aux, axis = 1)


#%% filtración, las ops solo aparecen desde que son desembolsadas hasta que son finalizadas
df_temp = df_temp[df_temp['aux col filtrado'] != 'eliminar']

#%% Agregando columnas agregadas desde el bd_pagos
cap_original = bd_pagos.pivot_table(index = 'Codigo de prestamo',
                                    values = 'Saldo por cancelar',
                                    aggfunc = 'max').reset_index()

df_o = df_temp[['Fecha_corte', 'codigo_de_prestamo']]

# esta tabla se va a reutilizar para cada vez que se quiera añadir una columna que cambia mensualmente
df_cortes_ops = df_o.copy() 

df_o = df_o.merge(bd_pagos[['Codigo de prestamo', 
                            'Monto pagado', 
                            'Capital pagado', 
                            'Intereses generado', 
                            'Monto moratorio',
                            'Saldo a favor',
                            'Fecha de pago del cliente']],
                  
                  left_on = 'codigo_de_prestamo',
                  right_on = 'Codigo de prestamo',
                  how = 'left')

df_o = df_o[df_o['Fecha de pago del cliente'] <= df_o['Fecha_corte']]
# 
pivot_pagos = df_o.pivot_table(index = ['Fecha_corte', 'codigo_de_prestamo'],
                               values = ['Monto pagado', 
                                         'Capital pagado', 
                                         'Intereses generado', 
                                         'Monto moratorio',
                                         'Saldo a favor'],
                               aggfunc = 'sum').reset_index()

# rename para ser más entendible
pivot_pagos.rename(columns = {'Intereses generado': 'Interes pagado',
                              'Monto moratorio' : 'Interes moratorio pagado'},
                   inplace = True)

#%% union de columnas al df_temp
df_temp = df_temp.merge(pivot_pagos,
                        on = ['Fecha_corte', 'codigo_de_prestamo'],
                        how = 'left')

df_temp['codigo_de_prestamo']      = df_temp['codigo_de_prestamo'].str.strip()
cap_original['Codigo de prestamo'] = cap_original['Codigo de prestamo'].str.strip()
df_temp = df_temp.merge(cap_original,
                        left_on  = 'codigo_de_prestamo',
                        right_on = 'Codigo de prestamo',
                        how      = 'left')

df_temp['Monto pagado']             = df_temp['Monto pagado'].fillna(0)
df_temp['Capital pagado']           = df_temp['Capital pagado'].fillna(0)
df_temp['Interes pagado']           = df_temp['Interes pagado'].fillna(0)
df_temp['Interes moratorio pagado'] = df_temp['Interes moratorio pagado'].fillna(0)
df_temp['Saldo a favor']            = df_temp['Saldo a favor'].fillna(0)

del df_temp['Codigo de prestamo']

#%% solarizando 
#%% cálculo de saldo capital
df_temp['Saldo Capital'] = np.where(df_temp['aux col filtrado'] == 'finalizado',
                                    0,
                                    np.maximum(df_temp['Saldo por cancelar'] - df_temp['Capital pagado'], 0))
del df_temp['Saldo por cancelar']

#%% cálculo dedías de atraso
# obtenemos la mínima fecha vigente de próximo pago

pagos_pendientes = bd_pagos[['Codigo de prestamo', 'Fecha de pago esperada original' , 'Fecha de pago del cliente']]
# pagos_pendientes = pagos_pendientes[pd.isna(pagos_pendientes['Fecha de pago del cliente'])]

# esto con el tiempo podría ponerse lento
df_minima_fecha_pago_vigente = df_fechas.assign(key = 1).merge(pagos_pendientes.assign(key = 1),
                                                               on  = 'key',
                                                               how = 'left').drop('key', axis = 1)

df_minima_fecha_pago_vigente = df_minima_fecha_pago_vigente[ ~(df_minima_fecha_pago_vigente['Fecha de pago del cliente'] < df_minima_fecha_pago_vigente['Fecha_corte'])]

prox_fecha_pago = df_minima_fecha_pago_vigente.pivot_table(index = ['Fecha_corte', 'Codigo de prestamo'],
                                                           values = 'Fecha de pago esperada original',
                                                           aggfunc = 'min').reset_index()
prox_fecha_pago.rename(columns = {'Fecha de pago esperada original' : 'fecha proximo pago'},
                       inplace = True)

# añadiendo la próxima fecha de pago al df_temp
df_temp = df_temp.merge(prox_fecha_pago,
                        left_on  = ['Fecha_corte', 'codigo_de_prestamo'],
                        right_on = ['Fecha_corte', 'Codigo de prestamo'],
                        how      = 'left')
df_temp['fecha proximo pago'] = df_temp['fecha proximo pago'].where(
    df_temp['aux col filtrado'] != 'finalizado',
    pd.NaT
)

del df_temp['Codigo de prestamo']

#%% Cálculo días de atraso
df_temp['dias atraso'] = (df_temp['Fecha_corte'] - df_temp['fecha proximo pago']).dt.days

df_temp['dias atraso'] = df_temp['dias atraso'].fillna(0)

df_temp['dias atraso'] = np.maximum(df_temp['dias atraso'],0)

#%% par 0, 15, 30, 60, 90, 120, 150, 180, 360
df_temp['par 0']   = np.where(df_temp['dias atraso'] > 0,   df_temp['Saldo Capital'], 0)
df_temp['par 15']  = np.where(df_temp['dias atraso'] > 15,  df_temp['Saldo Capital'], 0)
df_temp['par 30']  = np.where(df_temp['dias atraso'] > 30,  df_temp['Saldo Capital'], 0)
df_temp['par 60']  = np.where(df_temp['dias atraso'] > 60,  df_temp['Saldo Capital'], 0)
df_temp['par 90']  = np.where(df_temp['dias atraso'] > 90,  df_temp['Saldo Capital'], 0)
df_temp['par 120'] = np.where(df_temp['dias atraso'] > 120, df_temp['Saldo Capital'], 0)
df_temp['par 150'] = np.where(df_temp['dias atraso'] > 150, df_temp['Saldo Capital'], 0)
df_temp['par 180'] = np.where(df_temp['dias atraso'] > 180, df_temp['Saldo Capital'], 0)
df_temp['par 360'] = np.where(df_temp['dias atraso'] > 360, df_temp['Saldo Capital'], 0)

#%% q_desembolso
df_temp['q_desembolso'] = np.where(df_temp['mes_desembolso'] == df_temp['Fecha_corte'], 1, 0)

df_temp['m_desembolso'] = np.where(df_temp['mes_desembolso'] == df_temp['Fecha_corte'], df_temp['monto_prestado'], 0)

#%% ordenamiento PENDIENTE

#%% CARGA AL LAKE
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
s3_prefix = "manual/ba/portafolio_lending_v1/" # carpeta lógica en el bucket 

# ==== EXPORTAR A PARQUET EN MEMORIA ====
csv_buffer = io.StringIO()

# del df_temp['exchange_rate']
df_temp.to_csv(csv_buffer, index=False, encoding="utf-8-sig") 

# Nombre de archivo con timestamp (opcional, para histórico) 
s3_key = f"{s3_prefix}portafolio_lending_v1.csv" 

# Subir directamente desde el buffer 
s3.put_object(Bucket  = bucket_name, 
              Key     = s3_key, 
              Body    = csv_buffer.getvalue() 
              )

print(f"✅ Archivo subido a s3://{bucket_name}/{s3_key}")

df_temp.to_csv('portafolio_lending_v1.csv',
               index    = 'False',
               sep      = ',',
               encoding = 'utf-8-sig')

#%%
# =============================================================================
# =============================================================================
# #                                 COSECHA
# =============================================================================
# =============================================================================

cosecha = df_temp[df_temp['q_desembolso'] == 1]
cosecha = cosecha.sort_values(by='fecha_de_desembolso', ascending = True)

################   colocar mínima fecha de desembolso  ########################
min_fecha_desembolso = cosecha.pivot_table(values  = 'fecha_de_desembolso',
                                           index   = 'codigo_de_contrato',
                                           aggfunc = 'min').reset_index()
min_fecha_desembolso.rename(columns = {'fecha_de_desembolso': 'min_fecha_desembolso'}, inplace = True)
cosecha = cosecha.merge(min_fecha_desembolso,
                        on  = 'codigo_de_contrato',
                        how = 'left')
cosecha['fecha_de_desembolso'] = np.where(cosecha['min_fecha_desembolso'].notna(),
                                          cosecha['min_fecha_desembolso'],
                                          cosecha['fecha_de_desembolso'])
cosecha['mes_desembolso'] = cosecha['fecha_de_desembolso'].dt.to_period('M').dt.to_timestamp('M')

del cosecha['min_fecha_desembolso']

################  colocar máximo monto desembolsado  ##########################
# esto existe por que en caso de ampliación, le ponen un nuevo monto alto
max_monto_desembolsado = cosecha.pivot_table(values = 'monto_prestado',
                                           index    = 'codigo_de_contrato',
                                           aggfunc  = 'max').reset_index()
max_monto_desembolsado.rename(columns = {'monto_prestado': 'max_monto_desembolsado'}, inplace = True)
cosecha = cosecha.merge(max_monto_desembolsado,
                        on  = 'codigo_de_contrato',
                        how = 'left')
cosecha['monto_prestado'] = np.where(cosecha['max_monto_desembolsado'].notna(),
                                     cosecha['max_monto_desembolsado'],
                                     cosecha['monto_prestado'])
del cosecha['max_monto_desembolsado']

####### nos quedamos solo con las operaciones validadas para cosecha ##########
cosecha = cosecha.sort_values(by = 'Fecha_corte', ascending = False)
cosecha = cosecha.drop_duplicates(subset = 'codigo_de_contrato')

# cosecha, cruce cartesiano ###################################################
cosecha = cosecha[['codigo_de_contrato', 'fecha_de_desembolso', 'mes_desembolso',
                   'moneda', 'monto_prestado', 'tipo_de_persona', 'tipo_de_documento',
                   'numero_de_documento', 'persona_o_rrll', 'ruc', 'empresa', 'correo']]

cosecha = df_fechas.assign(key = 1).merge(cosecha.assign(key = 1),
                                          on  = 'key',
                                          how = 'left').drop('key', axis = 1)

cosecha = cosecha[cosecha['mes_desembolso'] <= cosecha['Fecha_corte']]

###### añadiendo datos de cada corte mensual ##################################
filtracion = df_temp[df_temp['aux col filtrado'] != 'finalizado']

filtracion = filtracion[['Fecha_corte', 'codigo_de_contrato', 'Saldo Capital', 
                         'fecha proximo pago', 'dias atraso', 'par 0', 'par 15',
                         'par 30', 'par 60', 'par 90', 'par 120', 'par 150', 
                         'par 180', 'par 360']]

cosecha = cosecha.merge(filtracion,
                         on = ['Fecha_corte', 'codigo_de_contrato'],
                         how = 'left')

cosecha = cosecha[ cosecha['fecha_de_desembolso'] <= cosecha['Fecha_corte'] ]

cosecha['m_desembolso'] = np.where(cosecha['Fecha_corte'] <= cosecha['fecha_de_desembolso'],
                                   cosecha['monto_prestado'],
                                   0)

#%%
cosecha.to_csv('cosecha_lending_v1.csv',
               encoding = 'utf-8-sig',
               index = False,
               sep = ',')


