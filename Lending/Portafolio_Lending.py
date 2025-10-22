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

#%%
fecha_corte = '2025-10-31'

#%% ops
df_ops = pd.read_excel(r'G:/Mi unidad/BD_Cobranzas.xlsm',
                       sheet_name= 'Prestamos gestionados',
                            dtype= {'Numero de documento'         : str,
                                    'RUC'                         : str,
                                    'Nro de cuotas'               : int,
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
                '%Y-%d-%m %H:%M:%S'    ,
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

df_ops['fecha_de_desembolso']         = df_ops['fecha_de_desembolso'].apply(parse_dates)
df_ops['fecha_de_finalizacion']       = df_ops['fecha_de_finalizacion'].apply(parse_dates)
bd_pagos['Fecha de pago del cliente'] = bd_pagos['Fecha de pago del cliente'].apply(parse_dates)

bd_pagos['Fecha de pago del cliente'][12]
bd_pagos['Fecha de pago del cliente'] = pd.to_datetime(bd_pagos['Fecha de pago del cliente'])

#%% Fecha finalización de la operación
# cálculo para validaciones sacando la máxima fecha del BD_PAGOS
fecha_fin = bd_pagos.pivot_table(values  = 'Fecha de pago del cliente',
                                 index   = 'Codigo de prestamo',
                                 aggfunc = 'max').reset_index()
df_ops = df_ops.merge(fecha_fin,
                      left_on   = 'codigo_de_prestamo',
                      right_on  = 'Codigo de prestamo',
                      how = 'left')

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

#%%
# Producto cartesiano (todas las combinaciones)
df_temp = df_fechas.assign(key=1).merge(df_ops.assign(key=1), on='key', how='left').drop('key', axis=1)

# Filtrar solo cuando la fecha está entre desembolso y cancelación
fecha_max = df_temp['Fecha_corte'].max()
df_temp['fecha_de_finalizacion'] = pd.to_datetime(df_temp['fecha_de_finalizacion'])  # aseguramos tipo datetime
df_temp['Fecha_corte'] = pd.to_datetime(df_temp['Fecha_corte'])
df_temp['mes_finalizacion'] = df_temp['fecha_de_finalizacion'].dt.to_period('M').dt.to_timestamp('M')

def col_aux(df):
    if df['mes_finalizacion'] == df['Fecha_corte']:
        return 'finalizado'
    if df['mes_finalizacion'] < df['Fecha_corte']:
        return 'eliminar'
    if df['fecha_de_desembolso'] > df['Fecha_corte']:
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
#%% cálculo de saldo capital
df_temp['Saldo Capital'] = np.where(df_temp['aux col filtrado'] == 'finalizado',
                                    0,
                                    df_temp['Saldo por cancelar'] - df_temp['Capital pagado'])
del df_temp['Saldo por cancelar']

#%% cálculo dedías de atraso







