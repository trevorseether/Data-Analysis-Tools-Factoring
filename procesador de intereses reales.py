# -*- coding: utf-8 -*-
"""
Created on Tue Jul 22 12:24:01 2025

@author: Joseph Montoya
"""

# =============================================================================
# procesamiento de
# =============================================================================
import pandas as pd
# import numpy as np
# import boto3
from pyathena import connect
# import openpyxl
from openpyxl import load_workbook
from openpyxl.styles import NamedStyle
import os

import shutil
from datetime import datetime

import warnings
warnings.filterwarnings("ignore")

hoy_formateado = datetime.today().strftime('%d-%m-%Y')  # o '%Y-%m-%d', etc.

#%%
archivo = 'G:/Mi unidad/Pagados 122024 en adelante.xlsx'

df_online = pd.read_excel( archivo,
                           sheet_name = 'Online',
                           dtype = {'Interés Bruto pagado a Crowd (Victor E)' : str,
                                    'Monto_Financiado' : str})
df_emitidos = pd.read_excel( archivo,
                           sheet_name = 'Masivos Emitidos',
                           )
# eliminación de filas vacías
col_factura_relacionada = 'COM. VINCULADO'   #'Factura Relacionada'
col_comprobante_emitido = 'COMPROBANTE EMITIDO'  #'Comprobante Emitido'
df_emitidos = df_emitidos[~(df_emitidos[col_factura_relacionada].isna() & df_emitidos[col_comprobante_emitido].isna() & df_emitidos['RUC'].isna())]

#%%
df_online = df_online[['Subasta',
                       'Comprobante_costo_financiamiento',
                       'Fecha_Pago_real',
                       'Interés Bruto pagado a Crowd (Victor E)',
                       'Costo de Financiamiento Liquidado emp(numérico)',
                       'Interés Moratorio\n15 / 03 en adelante (numérico)']]



#%% limpieza numérica
# def convertir_a_float(valor):
#     if pd.isna(valor):
#         return None
#     valor = str(valor).replace(',', '.').replace('..', '.')
#     if valor.count('.') > 1:
#         # Si hay más de un punto, el primero es separador de miles → eliminar todos menos el último
#         partes = valor.split('.')
#         valor = ''.join(partes[:-1]) + '.' + partes[-1]
#     return float(valor)
def convertir_a_float(valor):
    import re
    if pd.isna(valor):
        return None

    # Convertir a string y normalizar
    valor = str(valor).replace(',', '.').replace('..', '.')

    # Buscar el primer número decimal en la cadena usando regex
    match = re.search(r'\d+(?:\.\d+)?', valor)
    if match:
        return float(match.group())
    return None  # Si no hay número válido, devuelve None
df_online['Interés Bruto pagado a Crowd (Victor E)(2)'] = df_online['Interés Bruto pagado a Crowd (Victor E)'].apply(convertir_a_float)
df_online['Costo de Financiamiento Liquidado emp(2)'] = df_online['Costo de Financiamiento Liquidado emp(numérico)'].apply(convertir_a_float)
df_online['Interés Moratorio\n15 / 03 en adelante(2)'] = df_online['Interés Moratorio\n15 / 03 en adelante (numérico)'].apply(convertir_a_float)

def limpiar_valor_numerico(valor):
    """
    Limpia un valor que representa un número posiblemente mal formateado.
    Soporta separadores decimales ',' y '.', texto adicional y errores.

    Parámetros:
    - valor: string o cualquier tipo (cada celda de la columna)

    Retorna:
    - float o NaN si no se puede convertir
    """
    import re
    import numpy as np

    if pd.isna(valor):
        return np.nan

    # Convertir a string y reemplazar , por .
    valor_str = str(valor).replace(",", ".")

    # Buscar un patrón de número válido (ej: -123.45)
    match = re.search(r"[+-]?\d*\.?\d+", valor_str)
    if match:
        try:
            return float(match.group())
        except ValueError:
            return np.nan
    else:
        return np.nan

df_online['Interés Bruto pagado a Crowd (Victor E)(3)'] = df_online['Interés Bruto pagado a Crowd (Victor E)(2)'].apply(limpiar_valor_numerico)
df_online['Costo de Financiamiento Liquidado emp(3)'] = df_online['Costo de Financiamiento Liquidado emp(2)'].apply(limpiar_valor_numerico)
df_online['Interés Moratorio\n15 / 03 en adelante(3)'] = df_online['Interés Moratorio\n15 / 03 en adelante(2)'].apply(limpiar_valor_numerico)

#%%
df_online = df_online[['Subasta',
                       'Comprobante_costo_financiamiento',
                       'Fecha_Pago_real',
                       'Interés Bruto pagado a Crowd (Victor E)(3)',
                       'Costo de Financiamiento Liquidado emp(3)',
                       'Interés Moratorio\n15 / 03 en adelante(3)']]

df_online = df_online.merge(df_emitidos[['COM. VINCULADO','COMPROBANTE EMITIDO', 'FECHA DE EMISIÓN']],
                            left_on = 'Comprobante_costo_financiamiento',
                            right_on = 'COM. VINCULADO',
                            how = 'left')

del df_online['COM. VINCULADO']

df_online = df_online[     ~pd.isna(df_online['Interés Bruto pagado a Crowd (Victor E)(3)'])  | ~pd.isna(df_online['Costo de Financiamiento Liquidado emp(3)'])  |  ~pd.isna(df_online['Interés Moratorio\n15 / 03 en adelante(3)']) ]

df_online.to_excel(rf'C:\Users\Joseph Montoya\Desktop\pruebas\ingresos reales {hoy_formateado}.xlsx', index = False)







