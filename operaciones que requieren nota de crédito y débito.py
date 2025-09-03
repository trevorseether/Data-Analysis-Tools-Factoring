# -*- coding: utf-8 -*-
"""
Created on Mon May 26 14:26:35 2025

@author: Joseph Montoya
"""

# =============================================================================
#  VALIDACIÓN, FALTANTES DE NOTA DE CRÉDITO / DEBITO
# =============================================================================
import pandas as pd
import os
from datetime import datetime

import warnings
warnings.filterwarnings("ignore")

#%%
archivo = 'G:/Mi unidad/Pagados 122024 en adelante.xlsx'

df_online = pd.read_excel( archivo,
                           sheet_name = 'Online',
                           dtype = {'Interés Bruto pagado a Crowd (Victor E)' : str,
                                    'Monto_Financiado' : str})
df_emitidos = pd.read_excel( archivo,
                           sheet_name = 'Masivos Emitidos',
                           )

df_individuales_emitidos = pd.read_excel( archivo,
                                         sheet_name = 'Individuales Emitidos',
                                         dtype = {'CÓDIGO SOLICITUD' : str,
                                                  'SUBTOTAL' : str})

# eliminación de filas vacías
col_factura_relacionada = 'COM. VINCULADO'   #'Factura Relacionada'
col_comprobante_emitido = 'COMPROBANTE EMITIDO'  #'Comprobante Emitido'
df_emitidos = df_emitidos[~(df_emitidos[col_factura_relacionada].isna() & df_emitidos[col_comprobante_emitido].isna() & df_emitidos['RUC'].isna())]

# omitir los que tienen garantía negativa
df_online = df_online[ ~(df_online['GARANTIA NEGATIVA'] < 0) ]

# emitidos para facturas de interés moratorio
df_individuales_emitidos = df_individuales_emitidos[['CÓDIGO SOLICITUD', 'TIPO DE COMPROBANTE', 'SUBTOTAL']]
df_individuales_emitidos = df_individuales_emitidos[ df_individuales_emitidos['TIPO DE COMPROBANTE'].str.lower().str.contains('factura', na=False) ]
df_individuales_emitidos = df_individuales_emitidos[ ~pd.isna(df_individuales_emitidos['SUBTOTAL']) ]
df_individuales_emitidos['SUBTOTAL'] = pd.to_numeric(df_individuales_emitidos['SUBTOTAL'], errors= 'coerce')
df_individuales_emitidos = df_individuales_emitidos[ ~pd.isna(df_individuales_emitidos['SUBTOTAL']) ]
df_individuales_emitidos['SUBTOTAL'] = df_individuales_emitidos['SUBTOTAL'].round(2)
df_individuales_emitidos = df_individuales_emitidos.drop_duplicates()

#%% limpieza numérica
def convertir_a_float(valor):
    if pd.isna(valor):
        return None
    valor = str(valor).replace(',', '.').replace('..', '.')
    if valor.count('.') > 1:
        # Si hay más de un punto, el primero es separador de miles → eliminar todos menos el último
        partes = valor.split('.')
        valor = ''.join(partes[:-1]) + '.' + partes[-1]
    return float(valor)

# df_online['Interés Bruto pagado a Crowd (Victor E)'] = df_online['Interés Bruto pagado a Crowd (Victor E)'].apply(convertir_a_float)


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

# df_online['Interés Bruto pagado a Crowd (Victor E)'] = df_online['Interés Bruto pagado a Crowd (Victor E)'].apply(limpiar_valor_numerico)
# df_online['Costo de Financiamiento Cobrado emp'] = df_online['Costo de Financiamiento Cobrado emp'].apply(limpiar_valor_numerico)

# df_online['Saldo por costo de financiamiento cobrado'] = df_online['Saldo por costo de financiamiento cobrado'].apply(limpiar_valor_numerico)

def saldo_txt(df):
    if pd.isna( df['Costo_Financiamiento'] ):
        return "NO HAY COMPROBANTE"
    if pd.isna( df['Costo de Financiamiento Cobrado emp'] ):
        return "NO HAY REGISTRO DE COSTO COBRADO"
    else: 
        return ''
# df_online['flag saldo por costo1'] = df_online.apply(saldo_txt, axis = 1)

#%%
df_online['RUC PROVEEDOR'] = (
    df_online['RUC PROVEEDOR']
    .astype(str)                 # convertir a string
    .str.replace(',', '', regex=False)  # eliminar comas
    .str.replace('.00', '', regex=False)
    .str.replace('.0', '', regex=False))

#%%
# df_online['Saldo por costo de financiamiento cobrado (calculado)'] = df_online['Costo de Financiamiento Cobrado emp'] - df_online['Costo_Financiamiento']
def saldo_txt2(df):
    if pd.isna( df['Costo_Financiamiento'] ):
        return "NO HAY COMPROBANTE"
    if pd.isna( df['Costo de Financiamiento Cobrado emp'] ):
        return "NO HAY REGISTRO DE COSTO COBRADO"
    else: 
        return ''
# df_online['flag saldo por costo2'] = df_online.apply(saldo_txt2, axis = 1)

def diferencias_saldo(df):
    if round(df['Saldo por costo de financiamiento cobrado (calculado)'],2) != round(df['Saldo por costo de financiamiento cobrado'],2):
        return 'diferencia'
    else:
        return ''
# df_online['validar saldo'] = df_online.apply(diferencias_saldo, axis = 1)

#%%
df_online['Comprobante_costo_financiamiento'] = df_online['Comprobante_costo_financiamiento'].str.strip()
df_emitidos[col_factura_relacionada]          = df_emitidos[col_factura_relacionada]         .str.strip()

df = df_online.merge(df_emitidos[[col_factura_relacionada, col_comprobante_emitido]],
                     left_on  = 'Comprobante_costo_financiamiento',
                     right_on = col_factura_relacionada,
                     how = 'left')

filtrado = df[ pd.isna(df [col_comprobante_emitido])]

filtrado = filtrado[ ~pd.isna(filtrado ['Comprobante_costo_financiamiento'])]

filtrado = filtrado[ filtrado['Saldo por costo de financiamiento cobrado'] != 0]

filtrado = filtrado[ filtrado['Saldo por costo de financiamiento cobrado'] != '#VALUE!']

filtrado = filtrado[ filtrado['Saldo por costo de financiamiento cobrado'] != '#REF!']

filtrado = filtrado[ ~pd.isna(filtrado['Saldo por costo de financiamiento cobrado']) ]

filtrado = filtrado[ filtrado['Saldo por costo de financiamiento cobrado'] != 'NO HAY REGISTRO DE COSTO COBRADO']


#%%
df_final = pd.DataFrame()

df_final['Codigo de subasta']      = filtrado['Subasta']
df_final['Fecha para comprobante'] = pd.to_datetime(datetime.now()).normalize()
df_final['RUC']                    = filtrado['RUC PROVEEDOR']
df_final['Razon social']           = filtrado['RAZON SOCIAL']
df_final['Dirección']              = filtrado['DIRECCION']
df_final['Subtotal']               = filtrado['Saldo por costo de financiamiento cobrado'].abs()
df_final['IGV']                    = 0
df_final['Monto']                  = filtrado['Saldo por costo de financiamiento cobrado']
df_final['Moneda']                 = filtrado['Moneda Factura']

df_final['Descripcion (Concepto)'] = "Ajuste al descuento por operación de Factoring en referencia el Contrato Empresario."
df_final['Correo receptor']        = filtrado['CORREO']
df_final['Tipo de Comprobante']    = ''

df_final['Observación']            = "Inafecta de IGV"
df_final['Factura']                = filtrado['Comprobante_costo_financiamiento']

#%%
def tipo_comprobante(df):
    if df['Monto']<0:
        return "NOTA DE CREDITO"
    if df['Monto']>=0:
        return "NOTA DE DÉBITO"
    else:
        return "NOTA DE CREDITO"
df_final['Tipo de Comprobante'] = df_final.apply(tipo_comprobante, axis = 1)

df_final['Monto'] = df_final['Monto'].abs()

#%%
hoy_formateado = datetime.today().strftime('%d-%m-%Y')  # o '%Y-%m-%d', etc.

# df_final.to_excel(rf'C:\Users\Joseph Montoya\Desktop\notas de crédito y débito\Listado para envío de comprobantes masivo {hoy_formateado} (4).xlsx',
#                   index = False)

#%% excel con coma decimal
# Lista de columnas a convertir
columnas = ['Subtotal', 'Monto']

# Formatear cada columna como texto con coma decimal
for col in columnas:
    df_final[col] = df_final[col].apply(lambda x: f"{x:.2f}".replace('.', ',') if pd.notna(x) else "")

df_final.to_excel(rf'C:\Users\Joseph Montoya\Desktop\notas de crédito y débito\Listado para envío de comprobantes masivo {hoy_formateado}.xlsx',
                  index = False)

# =============================================================================
#%% CUANDO HAYA INTERÉS MOTARIORIO:
# =============================================================================
interes_moratorio = df_online[~pd.isna(df_online['Interés Moratorio\n15 / 03 en adelante'])]
interes_moratorio['Interés Moratorio\n15 / 03 en adelante'] = pd.to_numeric(interes_moratorio['Interés Moratorio\n15 / 03 en adelante'], errors='coerce')
interes_moratorio = interes_moratorio[interes_moratorio['Interés Moratorio\n15 / 03 en adelante'] > 0]

# quitar si ya se crearon las facturas para interés moratorio
interes_moratorio = interes_moratorio[~ interes_moratorio['Subasta'].isin(list(df_individuales_emitidos['CÓDIGO SOLICITUD'])) ]

if interes_moratorio.shape[0]>0:
    print('se crearán notas por interés moratorio')

    df_int_mo = pd.DataFrame()
    df_int_mo['Codigo de subasta']      = interes_moratorio['Subasta']
    df_int_mo['Fecha para comprobante'] = pd.to_datetime(datetime.now()).normalize()
    df_int_mo['RUC']                    = interes_moratorio['RUC PROVEEDOR']
    df_int_mo['Razon social']           = interes_moratorio['RAZON SOCIAL']
    df_int_mo['Dirección']              = interes_moratorio['DIRECCION']
    df_int_mo['Subtotal']               = interes_moratorio['Interés Moratorio\n15 / 03 en adelante'].abs()
    df_int_mo['IGV']                    = 0
    df_int_mo['Monto']                  = interes_moratorio['Interés Moratorio\n15 / 03 en adelante']
    df_int_mo['Moneda']                 = interes_moratorio['Moneda Factura']
    
    df_int_mo['Descripcion (Concepto)'] = "Interés moratorio en operación de Factoring en referencia el Contrato Empresario."
    df_int_mo['Correo receptor']        = interes_moratorio['CORREO']
    df_int_mo['Tipo de Comprobante']    = 'FACTURA'
    
    df_int_mo['Observación']            = "Inafecta de IGV"
    df_int_mo['Factura']                = interes_moratorio['Comprobante_costo_financiamiento']

#%% excel con coma decimal
# Lista de columnas a convertir
columnas = ['Subtotal', 'Monto', 'IGV']

# Formatear cada columna como texto con coma decimal
for col in columnas:
    df_int_mo[col] = df_int_mo[col].apply(lambda x: f"{x:.2f}".replace('.', ',') if pd.notna(x) else "")

df_int_mo.to_excel(rf'C:\Users\Joseph Montoya\Desktop\notas de crédito y débito\Interés Moratorio Operación Factoring {hoy_formateado}.xlsx',
                  index = False)




