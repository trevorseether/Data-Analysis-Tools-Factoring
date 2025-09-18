# -*- coding: utf-8 -*-
"""
Created on Fri Aug 15 09:47:36 2025

@author: Joseph Montoya
"""

# =============================================================================
# Documentos para emisión de notas de crédito/ débito, facturas por interés moratorio
# =============================================================================

import pandas as pd
import os
from datetime import datetime
import numpy as np
import warnings
warnings.filterwarnings("ignore")

#%%
ubi = r'C:\Users\Joseph Montoya\Desktop\notas de crédito y débito'

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

facturas_emitidas = df_emitidos[ df_emitidos['TIPO DE COMPROBANTE'] == 'FACTURA' ]
df_emitidos = df_emitidos[df_emitidos['TIPO DE COMPROBANTE'].str.contains('NOTA DE')]
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

#%% limpieza del RUC
df_online['RUC PROVEEDOR'] = (
    df_online['RUC PROVEEDOR']
    .astype(str)                 # convertir a string
    .str.replace(',', '', regex=False)  # eliminar comas
    .str.replace('.00', '', regex=False)
    .str.replace('.0', '', regex=False))

#%%
df_online['Comprobante_costo_financiamiento'] = df_online['Comprobante_costo_financiamiento'].str.strip()
df_emitidos[col_factura_relacionada]          = df_emitidos[col_factura_relacionada]         .str.strip()

df = df_online.merge(df_emitidos[[col_factura_relacionada, col_comprobante_emitido]],
                      left_on  = 'Comprobante_costo_financiamiento',
                      right_on = col_factura_relacionada,
                      how = 'left')

filtrado = df[ pd.isna(df [col_comprobante_emitido])]

filtrado = filtrado[ ~filtrado['Comprobante_costo_financiamiento'].isin(list(df_emitidos[col_factura_relacionada] )) ]

filtrado = filtrado[ ~pd.isna(filtrado ['Comprobante_costo_financiamiento'])]

'''
filtrado = df_online[ ~df_online['Subasta'].isin(list( df_emitidos['CÓDIGO OPERACIÓN'] ))]
'''

filtrado = filtrado[ filtrado['Saldo por costo de financiamiento cobrado'] != 0]

filtrado = filtrado[ filtrado['Saldo por costo de financiamiento cobrado'] != '#VALUE!']

filtrado = filtrado[ filtrado['Saldo por costo de financiamiento cobrado'] != '#VALOR!']

filtrado = filtrado[ filtrado['Saldo por costo de financiamiento cobrado'] != '#REF!']

filtrado = filtrado[ ~pd.isna(filtrado['Saldo por costo de financiamiento cobrado']) ]

filtrado = filtrado[ filtrado['Saldo por costo de financiamiento cobrado'] != 'NO HAY REGISTRO DE COSTO COBRADO']

filtrado = filtrado[ filtrado['Saldo por costo de financiamiento cobrado'] != 'NO HAY COMPROBANTE']

filtrado = filtrado[ pd.isna(filtrado['Costo de Financiamiento Liquidado emp']) ]

#%% CREACIÓN DE BASE
df_comprobantes = pd.DataFrame()

df_comprobantes['aux1'] = filtrado['Subasta']
df_comprobantes['aux2'] = filtrado['Saldo por costo de financiamiento cobrado']

df_comprobantes['Grupo'] = np.arange(1, len(df_comprobantes) + 1)

df_comprobantes['Serie'] = np.where(df_comprobantes['aux2'] >= 0,
                                    'FFD3',
                                    'FFC3')

df_comprobantes['Correlativo'] = ''

df_comprobantes['Tipo de nota'] = np.where(df_comprobantes['aux2'] >= 0,
                                           'Débito',
                                           'Crédito')

df_comprobantes['Fecha de emisión'] = pd.to_datetime(datetime.now()).normalize()
df_comprobantes["Fecha de emisión"] = (df_comprobantes["Fecha de emisión"].dt.strftime("%Y-%m-%d").astype("string"))

df_comprobantes['Motivo'] = np.where(df_comprobantes['aux2'] >= 0,
                                     'Aumento en el valor',
                                     'Disminución en el valor')

df_comprobantes['Comprobante Relacionado'] = filtrado['Comprobante_costo_financiamiento']

df_comprobantes['Fecha Comprobante Relacionado'] = ''

df_comprobantes['Tipo de doc. Cliente'] = 'RUC'

df_comprobantes['N° de doc. Cliente'] = filtrado['RUC PROVEEDOR']

df_comprobantes['Nombre cliente'] = filtrado['RAZON SOCIAL']

df_comprobantes['Correo cliente'] = ''

df_comprobantes['Moneda'] = filtrado['Moneda Operacion']

del df_comprobantes['aux1']
del df_comprobantes['aux2']

#%% HOJA ITEMS
df_items = pd.DataFrame()

df_items['aux1'] = filtrado['Subasta']

df_items['Grupo'] = np.arange(1, len(df_items) + 1)

df_items['Código del item'] = filtrado['Subasta']

df_items['Descripción del item'] = 'Ajuste al descuento por operación de Factoring en referencia el Contrato Empresario.'

df_items['Unidad del item'] = 'ZZ'

df_items['Cantidad del item'] = '1'

df_items['Precio del item'] = filtrado['Saldo por costo de financiamiento cobrado'].abs()

df_items['Impuesto'] = 'INA'
    
df_items['Gratuito'] = 'No'
    
df_items['ICBPER'] = 'No'

del df_items['aux1']

#%%
# df_comprobantes.to_excel(ubi + '\\Comprobantes.xlsx', index = False, sheet_name = 'Comprobantes')

# df_items.to_excel(ubi + '\\Items.xlsx', index = False, sheet_name = 'Items')
# pip install XlsxWriter

hoy_formateado = datetime.today().strftime('%d-%m-%Y')

with pd.ExcelWriter(ubi + f'\\Carga_masiva_Notas de credito y debito {hoy_formateado}.xlsx', engine='xlsxwriter') as writer:
    df_comprobantes.to_excel(writer, index=False, sheet_name='Comprobantes')
    df_items.to_excel(writer, index=False, sheet_name='Items')

#%%
# =============================================================================
# facturas por interés moratorio
# =============================================================================

interes_moratorio = df_online[~pd.isna(df_online['Interés Moratorio\n15 / 03 en adelante (numérico)'])]
interes_moratorio = interes_moratorio[  pd.isna(interes_moratorio['Interés Moratorio\n15 / 03 en adelante (comentarios)']) ]

interes_moratorio['Interés Moratorio\n15 / 03 en adelante (numérico)'] = pd.to_numeric(interes_moratorio['Interés Moratorio\n15 / 03 en adelante (numérico)'], errors='coerce')
interes_moratorio = interes_moratorio[interes_moratorio['Interés Moratorio\n15 / 03 en adelante (numérico)'] > 0]

# quitar si ya se crearon las facturas para interés moratorio
interes_moratorio = interes_moratorio[~ interes_moratorio['Subasta'].isin(list(df_individuales_emitidos['CÓDIGO SOLICITUD'])) ]
interes_moratorio = interes_moratorio[~ interes_moratorio['Subasta'].isin(list(facturas_emitidas['CÓDIGO OPERACIÓN'])) ]

if interes_moratorio.shape[0]>0:
    print('se crearán notas por interés moratorio')

    df_int_mo = pd.DataFrame()
    
    df_int_mo['aux1'] = interes_moratorio['Subasta']
    df_int_mo['Grupo'] = np.arange(1, len(df_int_mo) + 1)
    df_int_mo['Serie'] = ''
    df_int_mo['Correlativo'] = ''    
    df_int_mo['Fecha de emisión'] = pd.to_datetime(datetime.now()).normalize()
    df_int_mo['Fecha de emisión'] = (df_int_mo['Fecha de emisión'].dt.strftime("%Y-%m-%d").astype("string"))
    df_int_mo['Tipo de doc. cliente']   = 'RUC'
    df_int_mo['N° de doc. Cliente'] = interes_moratorio['RUC PROVEEDOR']
    df_int_mo['Nombre cliente'] = interes_moratorio['RAZON SOCIAL']
    df_int_mo['Correo cliente'] = ''
    df_int_mo['Moneda'] = interes_moratorio['Moneda Operacion']
    del df_int_mo['aux1']
    
    df_int_items = pd.DataFrame()
    df_int_items['aux1'] = interes_moratorio['Subasta']
    df_int_items['Grupo'] = np.arange(1, len(df_int_items) + 1)
    df_int_items['Código del item'] = interes_moratorio['Subasta']
    df_int_items['Descripción del item'] = 'Interés moratorio en operación de Factoring en referencia.'
    df_int_items['Unidad del item'] = 'ZZ'
    df_int_items['Cantidad del item'] = '1'
    df_int_items['Precio del item'] = interes_moratorio['Interés Moratorio\n15 / 03 en adelante (numérico)'].abs()
    df_int_items['Impuesto'] = 'INA'
    df_int_items['Gratuito'] = 'No'
    df_int_items['ICBPER'] = 'No'
    del df_int_items['aux1']

hoy_formateado = datetime.today().strftime('%d-%m-%Y')

with pd.ExcelWriter(ubi + f'\\Carga_masiva_Facturas {hoy_formateado}.xlsx', engine='xlsxwriter') as writer:
    df_int_mo.to_excel(writer, index=False, sheet_name='Comprobantes')
    df_int_items.to_excel(writer, index=False, sheet_name='Items')




