# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4 11:15:30 2026

@author: Joseph Montoya
"""

# =============================================================================
# REPORTE LIBRO CAJA
# =============================================================================

import pandas as pd
import os
import numpy as np 

#%%
ubi            = r'C:\Users\Joseph Montoya\Desktop\REPORTE LIBRO CAJA\REPORTE PG'
nombre_archivo = 'Reporte PG'
tc             = 3.5
sheets_objetivo = ['BCP SOLES', 'BCP DOLARES']
# columnas:
columnas = [
    'SD.',
    'Comprobante',
    'Emisión',
    'TD',
    'Numero',
    'Glosa',
    'Moneda',
    'Ingreso',
    'Egreso',
    'Situación' ]

HEADER_ROW = 6   # fila 7 en Excel (empieza desde 0)

dfs = []

# -----------------------------
# LOOP ARCHIVOS
# -----------------------------
for archivo in sorted(os.listdir(ubi)):

    if (
        archivo.endswith('.xlsx')
        and nombre_archivo in archivo
        and not archivo.startswith('~$')
    ):

        print(f"\nProcesando archivo: {archivo}")

        ruta = os.path.join(ubi, archivo)
        xls = pd.ExcelFile(ruta)

        sheets_validos = [
            s for s in sheets_objetivo if s in xls.sheet_names
        ]

        for sheet in sheets_validos:

            print(f"  Leyendo hoja: {sheet}")

            # ✅ leer cuenta desde B5 (fila 4, columna 1)
            cuenta = pd.read_excel(
                ruta,
                sheet_name=sheet,
                header=None,
                skiprows=4,
                nrows=1,
                usecols="B",
                engine='openpyxl'
            ).iloc[0, 0]

            # leer tabla
            df = pd.read_excel(
                ruta,
                sheet_name=sheet,
                header=HEADER_ROW,
                usecols=columnas,
                dtype=str,
                engine='openpyxl'
            )

            # columnas extra
            df['Cuenta'] = cuenta
            df['glosa_sheet_origen'] = sheet
            df['archivo_origen'] = archivo

            dfs.append(df)

# -----------------------------
# CONCAT FINAL
# -----------------------------
df_final = pd.concat(dfs, ignore_index=True)

print(f"\nFilas totales: {len(df_final)}")

#%% LIMPIEZA DE COLUMNAS
# limpieza de fecha
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
    formatos = ['%d/%m/%Y'             ,
                '%d/%m/%Y %H:%M:%S'    ,        
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

df_final['Emisión'] = df_final['Emisión'].apply(parse_dates)

df_final = df_final[ df_final['Emisión'].notna() ]

print('en función del número del excel, corregir fechas, de modo que siempre tengan el mes del documento')
#%%
# limpieza de decimales
df_final['Ingreso'] = df_final['Ingreso'].astype(float)

df_final['Egreso'] = df_final['Egreso'].astype(float)

# aquí genera un error si es que los datos están corridos

df_final['Ingreso'] = df_final['Ingreso'].fillna(0)
df_final['Egreso'] = df_final['Egreso'].fillna(0)

#%% cálculos
df_final['Mes'] = df_final['Emisión'].dt.month_name(locale='es_ES')

df_final['Ing/Egr'] = np.where(df_final['Ingreso'] != 0,
                               df_final['Ingreso'],
                               df_final['Egreso'])


df_final['Ing/Egr Solarizado'] = np.where(df_final['Moneda'] == 'US',
                                          df_final['Ing/Egr']/tc,
                                          df_final['Ing/Egr'])
ord_columns = ['SD.',
                'Comprobante',
                'Emisión',
                'TD',
                'Numero',
                'Glosa',
                'Moneda',
                'Ingreso',
                'Egreso',
                'Situación',
                'Mes',
                'glosa_sheet_origen',
                'Cuenta',
                'Ing/Egr',
                'Ing/Egr Solarizado']

df_final = df_final[ord_columns]

#%%
os.chdir(ubi)
df_final.to_excel('datos_concatenados_cs.xlsx', index = False)





