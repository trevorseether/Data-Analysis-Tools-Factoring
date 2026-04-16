# -*- coding: utf-8 -*-
"""
Created on Wed Apr 15 10:13:05 2026

@author: Joseph Montoya
"""

import pandas as pd
import os
import shutil

from datetime import datetime
hoy_formateado = datetime.today().strftime('%Y-%m-%d')

#%% variables para cada mes

codmes = 202603
fila_encabezados_pen = 4
fila_encabezados_usd = 872

#%%
# insumo nro 1
# movimientos de la cuenta recolectora
# descargado de telecredito del bcp
# Mov - EECC Soles - P2P - 2026

df_mov_eecc_pen = pd.read_excel(r'G:/Mi unidad/Mov - EECC Soles - P2P - 2026.xlsx',
                                sheet_name = str(codmes),
                                skiprows   = fila_encabezados_pen)


df_mov_eecc_usd = pd.read_excel(r'G:/Mi unidad/Mov - EECC Soles - P2P - 2026.xlsx',
                                sheet_name = str(codmes),
                                skiprows   = fila_encabezados_usd)


# eliminacion de filas vacías o sin información
df_mov_eecc_pen.dropna(subset = ['Descripción operación', 
                                 'Operación - Número',
                                 'Operación - Hora'], 
                                   inplace = True, 
                                   how     = 'all')
df_mov_eecc_pen.dropna(subset = ['Fecha'], 
                                   inplace = True, 
                                   how     = 'all')
df_mov_eecc_pen['N° De Cuota'] = df_mov_eecc_pen['N° De Cuota'].str.upper().str.replace('C', '')
df_mov_eecc_pen['N° De Cuota'] =  pd.to_numeric(df_mov_eecc_pen['N° De Cuota'], errors="coerce")
df_mov_eecc_pen['Referencia2'] = df_mov_eecc_pen['Referencia2'].str.strip().str.upper()
df_mov_eecc_pen['Fecha'] = pd.to_datetime(df_mov_eecc_pen['Fecha'], format = '%d/%m/%Y', errors = 'coerce')

print('pendiente separar dolares de los soles')

df_mov_eecc_usd.dropna(subset = ['Descripción operación', 
                                 'Operación - Número',
                                 'Operación - Hora'], 
                                   inplace = True, 
                                   how     = 'all')
df_mov_eecc_usd.dropna(subset = ['Fecha'], 
                                   inplace = True, 
                                   how     = 'all')
nombre_col_nro_cuota = 'Referencia3'
df_mov_eecc_usd[nombre_col_nro_cuota] = df_mov_eecc_usd[nombre_col_nro_cuota].str.upper().str.replace('C', '')
df_mov_eecc_usd[nombre_col_nro_cuota] =  pd.to_numeric(df_mov_eecc_usd[nombre_col_nro_cuota], errors="coerce")
df_mov_eecc_usd['Referencia2'] = df_mov_eecc_usd['Referencia2'].str.strip().str.upper()
df_mov_eecc_usd['Fecha'] = pd.to_datetime(df_mov_eecc_usd['Fecha'], format = '%d/%m/%Y', errors = 'coerce')


#%%% conexión a los 13 fondos

FIP1  = 1
FIM1  = 1
FIM2  = 1  
FIM3  = 1
FIP2  = 1
FIP3  = 1
FIP4  = 1
FIP5  = 1
FIP6  = 1
FIPD1 = 1
FIPD2 = 1
FIPD3 = r'G:/Mi unidad/Cartera fondo FIPD3.xlsx'
FIR   = 1

#%% LECTURA DE EXCELS DE CARTERA DE LOS FONDOS

FIP1  = 1
FIM1  = 1
FIM2  = 1  
FIM3  = 1
FIP2  = 1
FIP3  = 1
FIP4  = 1
FIP5  = 1
FIP6  = 1
FIPD1 = 1
FIPD2 = 1
FIPD3 = pd.read_excel(FIPD3, sheet_name = 'Pagos Préstamos', skiprows = 1)
FIR   = 1

#%% estandarizando columnas para hacer bien el merge

FIPD3['#Cuota'] = pd.to_numeric(FIPD3['#Cuota'], errors="coerce")
FIPD3['NOMBRE'] = FIPD3['NOMBRE'].str.strip().str.upper()

#%% COPIA DEL CARTERA FONDO
UBI = r'G:\.shortcut-targets-by-id\12i6ORpP5OaZcn38_m-v0DwHJG3LvLUtE\2026'
os.chdir(UBI)

ejemplo_original = r'G:\.shortcut-targets-by-id\12i6ORpP5OaZcn38_m-v0DwHJG3LvLUtE\2026\Cartera fondo FIPD3.xlsx'
destino = f"{str(codmes)} - Cartera fondo FIPD3 -automatizado- {hoy_formateado}.xlsx"

crear_excel = True
if crear_excel == True:
    # Copiar y renombrar al mismo tiempo
    shutil.copy(ejemplo_original, destino)
    print(f"✅ Archivo copiado y renombrado como '{destino}'")

#%% UNION DE DATOS PARA FIPD3
def add_suffix_by_index(df, i, j, sufijo = ''):
    cols = list(df.columns)
    # Asegurar orden correcto
    if i > j:
        i, j = j, i
    cols[i:j+1] = [str(c) + sufijo for c in cols[i:j+1]]
    df.columns = cols
    return df

df_mov_eecc_pen = add_suffix_by_index(df_mov_eecc_pen, 13, 18, " - FIP1")
df_mov_eecc_pen = add_suffix_by_index(df_mov_eecc_pen, 90, 95, " - FIPD3")

df_cols_fipd3 = df_mov_eecc_pen[['Fecha'
                                 ,'Referencia2' # cambiarlo por el codigo de contrato cuando se lo tenga
                                 ,'N° De Cuota'
                                 ,'Intereses Pagados.11 - FIP3'
                                 ,'Capital Pagado.11 - FIP3'
                                 ,'Redondeo.11 - FIP3'
                                 ,'Penalidades.11 - FIP3'
                                 ,'Saldo a Favor.11 - FIP3'
                                 ,'Intereses Compensatorios.11 - FIP3'
                                 ,'FIPD3'
                                 ]]

cartera = FIPD3[['NOMBRE',
                 '#Cuota']].copy()

cartera = cartera.merge(df_cols_fipd3,
                        left_on  = ['NOMBRE','#Cuota'],
                        right_on = ['Referencia2', 'N° De Cuota'],
                        how      = 'left')
del cartera['Referencia2']
del cartera['N° De Cuota']

lista_de_columnas = ['Fecha', 'Intereses Pagados.11 - FIP3',
                     'Capital Pagado.11 - FIP3', 'Redondeo.11 - FIP3',
                     'Penalidades.11 - FIP3', 'Saldo a Favor.11 - FIP3',
                     'Intereses Compensatorios.11 - FIP3', 'FIPD3']

cartera.dropna(subset = lista_de_columnas, 
                         inplace = True, 
                         how     = 'all')

cartera = cartera[cartera['FIPD3'] != 0]
cartera.dropna(subset = lista_de_columnas[1:8], 
                                   inplace = True, 
                                   how     = 'all')
cartera = cartera[cartera['#Cuota'].notna()]

FIPD3 = FIPD3.merge(cartera,
                    on = ['NOMBRE','#Cuota'],
                    how = 'left')
FIPD3['Fecha de Pago Real']       = FIPD3['Fecha de Pago Real']      .fillna(FIPD3[lista_de_columnas[0]])
FIPD3['Intereses Pagados']        = FIPD3['Intereses Pagados']       .fillna(FIPD3[lista_de_columnas[1]])
FIPD3['Capital Pagado']           = FIPD3['Capital Pagado']          .fillna(FIPD3[lista_de_columnas[2]])
FIPD3['Redondeo']                 = FIPD3['Redondeo']                .fillna(FIPD3[lista_de_columnas[3]])
FIPD3['Penalidades']              = FIPD3['Penalidades']             .fillna(FIPD3[lista_de_columnas[4]])
FIPD3['Saldo a Favor']            = FIPD3['Saldo a Favor']           .fillna(FIPD3[lista_de_columnas[5]])
FIPD3['Intereses Compensatorios'] = FIPD3['Intereses Compensatorios'].fillna(FIPD3[lista_de_columnas[6]])
FIPD3['Monto Total']              = FIPD3['Monto Total']             .fillna(FIPD3[lista_de_columnas[7]])

for i in lista_de_columnas:
    del FIPD3[i]

#%%
with pd.ExcelWriter(
    destino,
    engine         = "openpyxl",
    mode           = "a",           # append (archivo existente)
    if_sheet_exists= "overlay"      # no borra la hoja
) as writer:
    
    FIPD3.to_excel(
        writer,
        sheet_name = 'Pagos Préstamos',
        startrow   = 1,   # fila inicial (0-index)
        startcol   = 0,   # columna inicial (0-index)
        index      = False
    )
    
#%%



