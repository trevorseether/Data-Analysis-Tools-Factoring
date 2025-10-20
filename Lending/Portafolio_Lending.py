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
                                    'Fecha de desembolso'         : str})
df_ops.columns = (
    df_ops.columns
      .str.strip()              # elimina espacios al inicio/fin
      .str.lower()              # convierte todo a minúsculas
      .str.replace(' ', '_')    # reemplaza espacios por guiones bajos
                 )
df_ops['codigo_de_contrato'] = df_ops['codigo_de_contrato'].str.strip()
df_ops['codigo_de_prestamo'] = df_ops['codigo_de_prestamo'].str.strip()

###############################################################################
bd_pagos = pd.read_excel(r'G:/Mi unidad/BD_Cobranzas.xlsm',
                         sheet_name = 'BD PAGOS',
                         dtype = {'Numero de documento': str,
                                  'RUC' : str}
                         )

#%% Fecha finalización de la operación

op_ejemplo    = ['LH-45224519959']
lista_ejemplo = [pd.Timestamp('2025-09-30')]

cancels = pd.DataFrame({
    'codigo_de_prestamo': op_ejemplo,
    'fecha_cancelacion' : lista_ejemplo
    })

cancels['fecha_cancelacion'] = pd.to_datetime(cancels['fecha_cancelacion'])
###############################################################################

df_ops = df_ops.merge(cancels,
                      on  = 'codigo_de_prestamo',
                      how = 'left')

df_ops.columns


print('pendiente finalización de la operación')

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
    formatos = ['%d/%m/%Y %H:%M:%S'    ,
                '%d/%m/%Y'             ,
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

df_ops['fecha_de_desembolso'] = df_ops['fecha_de_desembolso'].apply(parse_dates)

#%%
# Crear una lista de fechas
fechas = pd.date_range(start = '2025-09-30',  # no tocar este valor
                       end = fecha_corte, freq = 'M')

# Crear un DataFrame con las fechas
df_fechas = pd.DataFrame({'Fecha_corte': fechas})
df_fechas['Fecha_corte'] = pd.to_datetime(df_fechas['Fecha_corte'])

#%%
# Producto cartesiano (todas las combinaciones)
df_temp = df_fechas.assign(key=1).merge(df_ops.assign(key=1), on='key', how='left').drop('key', axis=1)

# Filtrar solo cuando la fecha está entre desembolso y cancelación
fecha_max = df_temp['Fecha_corte'].max()

def col_aux(df):
    if df['fecha_cancelacion'] == df['Fecha_corte']:
        return 'finalizado'
    if df['fecha_cancelacion'] < df['Fecha_corte']:
        return 'eliminar'
    if df['fecha_de_desembolso'] > df['Fecha_corte']:
        return 'eliminar'
    if df['fecha_de_desembolso'] <= df['Fecha_corte']:
        return 'vigente'

df_temp['aux col filtrado'] = df_temp.apply(col_aux, axis = 1)

#%% filtración, las ops solo aparecen desde que son desembolsadas hasta que son finalizadas
df_temp = df_temp[df_temp['aux col filtrado'] != 'eliminar']

#%%

