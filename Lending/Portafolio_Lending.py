# -*- coding: utf-8 -*-
"""
Created on Wed Oct 15 09:59:08 2025

@author: Joseph Montoya
"""

# =============================================================================
# CARTERA / COSECHA LENDING
# =============================================================================

import pandas as pd
# import os
import numpy as np
# pd.options.display.date_format = "%Y-%m-%d"

import boto3
import json
import io
# import os
# from datetime import datetime

from pyathena import connect

#%%
fecha_corte = '2025-12-31' # YYYY-MM-DD

crear_excels = True # True o False

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

bd_pagos['TOTAL DE LA CUOTA PAGADA'] = bd_pagos['TOTAL DE LA CUOTA PAGADA'].fillna(0).astype(float)
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
bd_pagos['flag_finalizado'] = (
    bd_pagos
    .groupby('Codigo de prestamo')['Status de cuota']
    .transform(lambda x: (x == 'FINALIZADO').all())
)
# obtenemos solo las ops finalizadas
bd_pagos_finalizados = bd_pagos[bd_pagos['flag_finalizado'] == True]
del bd_pagos['flag_finalizado']

fecha_fin = bd_pagos_finalizados.pivot_table( values  = 'Fecha de pago del cliente',
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
df_fechas['codmes']      = df_fechas['Fecha_corte'].dt.strftime('%Y%m')

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
                            left_on  = 'Fecha_corte',
                            right_on = 'mes_tc',
                            how      = 'left'
                            )

df_fechas['exchange_rate'] = df_fechas['exchange_rate'].fillna(3.500)

del df_fechas['mes_tc']

#%% Obtener ejecutivo comercial
query = '''
    SELECT 
        codigo_de_negocio___tandia_lending as "codigo_de_prestamo", 
        CASE
            WHEN hubspot_owner_id = '407980810' THEN 'Priscila Quispe (pquispe@tandia.pe)'
            WHEN hubspot_owner_id = '76146626'  THEN 'Priscila García (pgarcia@tandia.pe)'
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
df_temp['Fecha_corte']           = pd.to_datetime(df_temp['Fecha_corte'])
df_temp['mes_finalizacion']      = df_temp['fecha_de_finalizacion'].dt.to_period('M').dt.to_timestamp('M')
df_temp['mes_desembolso']        = df_temp['fecha_de_desembolso'].dt.to_period('M').dt.to_timestamp('M')

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

#%% solarizando monto prestado, interés ganado
df_temp['monto_prestado_soles'] = np.where(df_temp['moneda'] == 'DOLARES',
                                           round(df_temp['monto_prestado'] * df_temp['exchange_rate'],2),
                                           df_temp['monto_prestado'] )

df_temp['interes_ganados_soles'] = np.where(df_temp['moneda'] == 'DOLARES',
                                            round(df_temp['interes_ganados'] * df_temp['exchange_rate'],2),
                                            df_temp['interes_ganados'] )

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
                            'TOTAL DE LA CUOTA PAGADA', 
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
                               values = ['TOTAL DE LA CUOTA PAGADA', 
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

df_temp['TOTAL DE LA CUOTA PAGADA'] = df_temp['TOTAL DE LA CUOTA PAGADA'].fillna(0)
df_temp['Capital pagado']           = df_temp['Capital pagado'].fillna(0)
df_temp['Interes pagado']           = df_temp['Interes pagado'].fillna(0)
df_temp['Interes moratorio pagado'] = df_temp['Interes moratorio pagado'].fillna(0)
df_temp['Saldo a favor']            = df_temp['Saldo a favor'].fillna(0)

del df_temp['Codigo de prestamo']

#%% solarizando
cols_solarizar = ['Capital pagado', 'Interes pagado', 'Interes moratorio pagado',
'Saldo a favor', 'TOTAL DE LA CUOTA PAGADA', 'Saldo por cancelar']

for i in cols_solarizar:
    df_temp[i + '_soles'] = np.where(df_temp['moneda'] == 'DOLARES',
                                     round(df_temp[i] * df_temp['exchange_rate'],2),
                                     df_temp[i])
df_temp.columns

#%% cálculo de saldo capital
df_temp['Saldo Capital'] = np.where(df_temp['aux col filtrado'] == 'finalizado',
                                    0,
                                    np.maximum(df_temp['Saldo por cancelar'] - df_temp['Capital pagado'], 0))
del df_temp['Saldo por cancelar']

df_temp['Saldo Capital_soles'] = np.where(df_temp['moneda'] == 'DOLARES',
                                          round(df_temp['Saldo Capital'] * df_temp['exchange_rate'],2),
                                          df_temp['Saldo Capital'] )

#%% cálculo dedías de atraso
# obtenemos la mínima fecha vigente de próximo pago

pagos_pendientes = bd_pagos[['Codigo de prestamo', 'Fecha de pago esperada original' , 'Fecha de pago del cliente']]
# pagos_pendientes = pagos_pendientes[pd.isna(pagos_pendientes['Fecha de pago del cliente'])]

# esto con el tiempo podría ponerse lento
df_minima_fecha_pago_vigente = df_fechas.assign(key = 1).merge(pagos_pendientes.assign(key = 1),
                                                               on  = 'key',
                                                               how = 'left').drop('key', axis = 1)

df_minima_fecha_pago_vigente = df_minima_fecha_pago_vigente[ ~(df_minima_fecha_pago_vigente['Fecha de pago del cliente'] < df_minima_fecha_pago_vigente['Fecha_corte'])]

prox_fecha_pago = df_minima_fecha_pago_vigente.pivot_table(index   = ['Fecha_corte', 'Codigo de prestamo'],
                                                           values  = 'Fecha de pago esperada original',
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

#%% Rango días de atraso
def rango_dias(df_temp):
    if df_temp['dias atraso'] == 0:
        return '0. 0'
    if df_temp['dias atraso'] <= 8:
        return '01. 1 - 8'
    if df_temp['dias atraso'] <= 30:
        return '02. 9 - 30'
    if df_temp['dias atraso'] <= 60:
        return '03. 31 - 60'
    if df_temp['dias atraso'] <= 90:
        return '04. 61 - 90'
    if df_temp['dias atraso'] <= 120:
        return '05. 91 - 120'
    if df_temp['dias atraso'] <= 150:
        return '06. 121 - 150'
    if df_temp['dias atraso'] <= 240:
        return '07. 151 - 240'
    if df_temp['dias atraso'] <= 360:
        return '08. 241 - 360'
    if df_temp['dias atraso'] <= 1094:
        return '09. 361 - 1,094'
    if df_temp['dias atraso'] <= float('inf'):
        return '10. > 1,094'
df_temp['rango_dias_atraso'] = df_temp.apply(rango_dias, axis = 1)
        
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

df_temp['par 0_soles']   = np.where(df_temp['dias atraso'] > 0,   df_temp['Saldo Capital_soles'], 0)
df_temp['par 15_soles']  = np.where(df_temp['dias atraso'] > 15,  df_temp['Saldo Capital_soles'], 0)
df_temp['par 30_soles']  = np.where(df_temp['dias atraso'] > 30,  df_temp['Saldo Capital_soles'], 0)
df_temp['par 60_soles']  = np.where(df_temp['dias atraso'] > 60,  df_temp['Saldo Capital_soles'], 0)
df_temp['par 90_soles']  = np.where(df_temp['dias atraso'] > 90,  df_temp['Saldo Capital_soles'], 0)
df_temp['par 120_soles'] = np.where(df_temp['dias atraso'] > 120, df_temp['Saldo Capital_soles'], 0)
df_temp['par 150_soles'] = np.where(df_temp['dias atraso'] > 150, df_temp['Saldo Capital_soles'], 0)
df_temp['par 180_soles'] = np.where(df_temp['dias atraso'] > 180, df_temp['Saldo Capital_soles'], 0)
df_temp['par 360_soles'] = np.where(df_temp['dias atraso'] > 360, df_temp['Saldo Capital_soles'], 0)

#%% q_desembolso
df_temp['q_desembolso'] = np.where(df_temp['mes_desembolso'] == df_temp['Fecha_corte'], 1, 0)

df_temp['m_desembolso'] = np.where(df_temp['mes_desembolso'] == df_temp['Fecha_corte'], df_temp['monto_prestado'], 0)

df_temp['m_desembolso_soles'] = np.where(df_temp['mes_desembolso'] == df_temp['Fecha_corte'], df_temp['monto_prestado_soles'], 0)

#%% cálculo de provisiones
# primero que nada columna de Clasificación
dias_atraso_nulos = df_temp[ pd.isna(df_temp['dias atraso'])]
if dias_atraso_nulos.shape[0] > 0:
    print('alerta, casos sin días de atraso')

def clasificacion(df):
    if df['dias atraso'] <= 8:
        return 'Normal'
    if df['dias atraso'] <= 30:
        return 'CPP'
    if df['dias atraso'] <= 60:
        return 'Deficiente'
    if df['dias atraso'] <= 120:
        return 'Dudoso'
    else:
        return 'Pérdida'
df_temp['Clasificacion'] = df_temp.apply(clasificacion, axis = 1)

def porcentaje_provision(df):
    if df['dias atraso'] <= 8:
        return 0.01
    if df['dias atraso'] <= 30:
        return 0.05
    if df['dias atraso'] <= 60:
        return 0.25
    if df['dias atraso'] <= 120:
        return 0.60
    else:
        return 1
df_temp['% Provision'] = df_temp.apply(porcentaje_provision, axis = 1)

df_temp['Provision'] = df_temp['% Provision'] * df_temp['Saldo Capital']
df_temp['Provision'] = round( df_temp['Provision'], 2)

df_temp['Provision_soles'] = df_temp['% Provision'] * df_temp['Saldo Capital_soles']
df_temp['Provision_soles'] = round( df_temp['Provision_soles'], 2)


########## flag castigo #######################################################

df_temp['flag_castigo_>150'] = np.where(df_temp['dias atraso'] > 150,
                                       'castigo',
                                       '')

df_temp['Saldo_castigado'] = np.where(df_temp['dias atraso'] > 150,
                                       df_temp['Saldo Capital'],
                                       '')
df_temp['Saldo_castigado_soles'] = np.where(df_temp['dias atraso'] > 150,
                                       df_temp['Saldo Capital_soles'],
                                       '')

#%% ordenamient
cols_para_ordenamiento = ['Fecha_corte', 'codmes', 'exchange_rate', 'codigo_de_contrato',
       'codigo_de_prestamo', 'tipo_de_persona', 'tipo_de_cliente',
       'tipo_de_documento', 'numero_de_documento', 'persona_o_rrll', 'ruc',
       'empresa', 'correo', 'riesgo', 'tipo_de_prestamo', 'moneda',
       'nro_de_cuotas', 'monto_prestado', 'monto_prestado_soles',  'tem', 'tea', 
       'interes_ganados', 'interes_ganados_soles', 
       'saldo_a_pagar', 'fecha_de_desembolso', 'banco_de_desembolso',
       'numero_de_cuenta_del_cliente', 'gestion_del_prestamo',
       'status_actual_del_prestamo', 'fecha_de_finalizacion',
       '¿se_envio_correo_de_desmbolso?', '¿se_ingreso_al_core_bancario?',
       '¿se_envio_correo_inicial_a_contabilidad?', 'dias_de_mora',
       '¿se_ingreso_a_equifax?', 'carpeta_cliente', 'direccion',
       'numero_de_contacto', 'Fecha de pago del cliente',
       'propietario_negocio', 'mes_finalizacion', 'mes_desembolso',
       'aux col filtrado', 
       
       'Capital pagado',
       'Interes pagado', 'Interes moratorio pagado', 'Saldo a favor',
       'TOTAL DE LA CUOTA PAGADA', 'Saldo Capital', 
       
       'Capital pagado_soles', 'Interes pagado_soles',
       'Interes moratorio pagado_soles', 'Saldo a favor_soles',
       'TOTAL DE LA CUOTA PAGADA_soles', 'Saldo Capital_soles',
       
       
       'fecha proximo pago',
       'dias atraso', 'rango_dias_atraso', 
       
       'par 0', 'par 15', 'par 30',
       'par 60', 'par 90', 'par 120', 'par 150', 'par 180', 'par 360',
       
       'par 0_soles', 'par 15_soles', 'par 30_soles',
       'par 60_soles', 'par 90_soles', 'par 120_soles', 'par 150_soles', 'par 180_soles', 'par 360_soles',
       
       'q_desembolso', 'm_desembolso', 'm_desembolso_soles', 
       'Clasificacion', '% Provision',
       'Provision', 'Provision_soles', 'flag_castigo_>150', 'Saldo_castigado', 'Saldo_castigado_soles']

df_temp = df_temp[cols_para_ordenamiento]

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
s3_prefix = "manual/ba/portafolio_lending/" # carpeta lógica en el bucket 

# ==== EXPORTAR A PARQUET EN MEMORIA ====
csv_buffer = io.StringIO()

# del df_temp['exchange_rate']
df_temp.to_csv(csv_buffer, index=False, encoding="utf-8-sig") 

# Nombre de archivo con timestamp (opcional, para histórico) 
s3_key = f"{s3_prefix}portafolio_lending.csv" 

# Subir directamente desde el buffer 
s3.put_object(Bucket  = bucket_name, 
              Key     = s3_key, 
              Body    = csv_buffer.getvalue() 
              )

print(f"✅ Archivo subido a s3://{bucket_name}/{s3_key}")


if crear_excels == True:
    df_temp.to_csv(r'G:\.shortcut-targets-by-id\1wzewbtJQv6Fr_f0uKnZrRg-jPtPM9D8a\BUSINESS ANALYTICS\Lending\portafolio_lending\portafolio_lending.csv',
                   index    = False,
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
                   'moneda', 'monto_prestado', 'monto_prestado_soles', 'tipo_de_persona', 
                   'tipo_de_documento', 'numero_de_documento', 'persona_o_rrll', 'ruc', 'empresa', 'correo']]

cosecha = df_fechas.assign(key = 1).merge(cosecha.assign(key = 1),
                                          on  = 'key',
                                          how = 'left').drop('key', axis = 1)

cosecha = cosecha[cosecha['mes_desembolso'] <= cosecha['Fecha_corte']]

###### añadiendo datos de cada corte mensual ##################################
filtracion = df_temp[df_temp['aux col filtrado'] != 'finalizado']

filtracion = filtracion[['Fecha_corte', 'codigo_de_contrato', 'Saldo Capital', 'Saldo Capital_soles',
                         'fecha proximo pago', 'dias atraso', 
                         
                         'par 0', 'par 15',
                         'par 30', 'par 60', 'par 90', 'par 120', 'par 150', 
                         'par 180', 'par 360',
                         
                         'par 0_soles', 'par 15_soles', 'par 30_soles',
                         'par 60_soles', 'par 90_soles', 'par 120_soles', 'par 150_soles', 'par 180_soles', 'par 360_soles',

                         ]]

filtracion['indice_compuesto'] = filtracion['Fecha_corte'].astype(str) + filtracion['codigo_de_contrato']
filtracion = filtracion.drop_duplicates(subset = 'indice_compuesto')
del filtracion['indice_compuesto']

cosecha = cosecha.merge(filtracion,
                         on  = ['Fecha_corte', 'codigo_de_contrato'],
                         how = 'left')

cosecha = cosecha[ cosecha['fecha_de_desembolso'] <= cosecha['Fecha_corte'] ]

#%%
# ==== CONFIGURACIÓN ==== 
bucket_name = "prod-datalake-raw-730335218320" 
s3_prefix = "manual/ba/cosecha_lending/" # carpeta lógica en el bucket 

# ==== EXPORTAR A PARQUET EN MEMORIA ====
csv_buffer = io.StringIO()

cosecha.to_csv(csv_buffer, index=False, encoding="utf-8-sig") 

# Nombre de archivo con timestamp (opcional, para histórico) 
s3_key = f"{s3_prefix}cosecha_lending.csv" 

# Subir directamente desde el buffer 
s3.put_object(Bucket  = bucket_name, 
              Key     = s3_key, 
              Body    = csv_buffer.getvalue() 
              )

print(f"✅ Archivo subido a s3://{bucket_name}/{s3_key}")


if crear_excels == True:
    cosecha.to_csv(r'G:\.shortcut-targets-by-id\1wzewbtJQv6Fr_f0uKnZrRg-jPtPM9D8a\BUSINESS ANALYTICS\Lending\cosecha_lending\cosecha_lending.csv',
                   encoding = 'utf-8-sig',
                   index = False,
                   sep = ',')

#%%
print('fin')
