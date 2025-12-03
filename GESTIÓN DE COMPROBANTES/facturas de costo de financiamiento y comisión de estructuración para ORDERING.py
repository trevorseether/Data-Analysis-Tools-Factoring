# -*- coding: utf-8 -*-
"""
Created on Wed Dec  3 17:50:09 2025

@author: Joseph Montoya
"""

# =============================================================================
# FACTURAS PARA ORDERING
# POR COSTO DE FINANCIAMIENTO Y POR COMISIÓN DE ESTRUCTURACIÓN
# =============================================================================

import pandas as pd
import numpy as np
from datetime import datetime

#%% lectura de hoja de opps offline
archivo = 'G:/Mi unidad/Pagados 122024 en adelante.xlsx'
ubi = r'C:\Users\Joseph Montoya\Desktop\notas de crédito y débito'

df_offline = pd.read_excel( archivo,
                           sheet_name = 'Offline automatizado',
                           dtype = str)

# filtrando orderings
df_offline = df_offline[df_offline['tipo_de_producto'] == 'Ordering']

#%% FACTURAS POR COSTO DE FINANCIAMIENTO

fac_costo_fi = df_offline[~pd.isna(df_offline['Costo_Financiamiento_teorico'])]

fac_costo_fi['Costo_Financiamiento_teorico'] = pd.to_numeric(fac_costo_fi['Costo_Financiamiento_teorico'], errors='coerce')
fac_costo_fi = fac_costo_fi[fac_costo_fi['Costo_Financiamiento_teorico'] > 0]

# quitar si ya se crearon las facturas para interés moratorio
# fac_costo_fi = fac_costo_fi[~ fac_costo_fi['Subasta'].isin(list(df_individuales_emitidos['CÓDIGO SOLICITUD'])) ]
# fac_costo_fi = fac_costo_fi[~ fac_costo_fi['Subasta'].isin(list(facturas_emitidas['CÓDIGO OPERACIÓN'])) ]

# fac_costo_fi = fac_costo_fi[~ fac_costo_fi['Subasta'].isin(list(df_facturas_emitidas['CÓDIGO OPERACIÓN']))] 

if fac_costo_fi.shape[0]>0:
    print('se crearán facturas por costo de financiamiento')

    df_costo_fi = pd.DataFrame()
    
    df_costo_fi['aux1'] = fac_costo_fi['Codigo_de_Subasta']
    df_costo_fi['Grupo'] = np.arange(1, len(df_costo_fi) + 1)
    df_costo_fi['Serie'] = 'F003'
    df_costo_fi['Correlativo'] = ''    
    df_costo_fi['Fecha de emisión'] = pd.to_datetime(datetime.now()).normalize()
    df_costo_fi['Fecha de emisión'] = (df_costo_fi['Fecha de emisión'].dt.strftime("%Y-%m-%d").astype("string"))
    df_costo_fi['Tipo de doc. cliente']   = 'RUC'
    df_costo_fi['N° de doc. Cliente'] = fac_costo_fi['ruc_proveedor']
    df_costo_fi['Nombre cliente'] = fac_costo_fi['razon_social']
    df_costo_fi['Correo cliente'] = ''
    df_costo_fi['Moneda'] = fac_costo_fi['Moneda_del_Monto_Financiado']
    del df_costo_fi['aux1']
    
    df_costo_items = pd.DataFrame()
    df_costo_items['aux1'] = fac_costo_fi['Codigo_de_Subasta']
    df_costo_items['Grupo'] = np.arange(1, len(df_costo_items) + 1)
    df_costo_items['Código del item'] = fac_costo_fi['Codigo_de_Subasta']
    df_costo_items['Descripción del item'] = 'Interés compensatorio por operación de adelanto por órdenes de compra y prestación de servicios.'
    df_costo_items['Unidad del item'] = 'ZZ'
    df_costo_items['Cantidad del item'] = '1'
    df_costo_items['Precio del item'] = round(fac_costo_fi['Costo_Financiamiento_teorico'].round(2),2)
    df_costo_items['Impuesto'] = '18%'
    df_costo_items['Gratuito'] = 'No'
    df_costo_items['ICBPER'] = 'No'
    del df_costo_items['aux1']

hoy_formateado = datetime.today().strftime('%d-%m-%Y')

with pd.ExcelWriter(ubi + f'\\Carga_masiva_Facturas_costo_financiamiento_ORDERING {hoy_formateado}.xlsx', engine='xlsxwriter') as writer:
    df_costo_fi.to_excel(writer, index=False, sheet_name='Comprobantes')
    df_costo_items.to_excel(writer, index=False, sheet_name='Items')

#%% FACTURAS POR COMISIÓN DE ESTRUCTURACIÓN
COLUMNA_COMISION = 'comision_estructuracion (para todas las ops mixtas esta columna corresponde a la nota de crédito o débito)'

fac_costo_fi = df_offline[~pd.isna(df_offline[ COLUMNA_COMISION ])]

fac_costo_fi[ COLUMNA_COMISION ] = pd.to_numeric(fac_costo_fi[ COLUMNA_COMISION ], errors='coerce')
fac_costo_fi = fac_costo_fi[fac_costo_fi[ COLUMNA_COMISION ] > 0]

# quitar si ya se crearon las facturas para interés moratorio
# fac_costo_fi = fac_costo_fi[~ fac_costo_fi['Subasta'].isin(list(df_individuales_emitidos['CÓDIGO SOLICITUD'])) ]
# fac_costo_fi = fac_costo_fi[~ fac_costo_fi['Subasta'].isin(list(facturas_emitidas['CÓDIGO OPERACIÓN'])) ]

# fac_costo_fi = fac_costo_fi[~ fac_costo_fi['Subasta'].isin(list(df_facturas_emitidas['CÓDIGO OPERACIÓN']))] 

if fac_costo_fi.shape[0]>0:
    print('se crearán facturas por comisión de estructuración')

    df_costo_fi = pd.DataFrame()
    
    df_costo_fi['aux1'] = fac_costo_fi['Codigo_de_Subasta']
    df_costo_fi['Grupo'] = np.arange(1, len(df_costo_fi) + 1)
    df_costo_fi['Serie'] = 'F003'
    df_costo_fi['Correlativo'] = ''    
    df_costo_fi['Fecha de emisión'] = pd.to_datetime(datetime.now()).normalize()
    df_costo_fi['Fecha de emisión'] = (df_costo_fi['Fecha de emisión'].dt.strftime("%Y-%m-%d").astype("string"))
    df_costo_fi['Tipo de doc. cliente']   = 'RUC'
    df_costo_fi['N° de doc. Cliente'] = fac_costo_fi['ruc_proveedor']
    df_costo_fi['Nombre cliente'] = fac_costo_fi['razon_social']
    df_costo_fi['Correo cliente'] = ''
    df_costo_fi['Moneda'] = fac_costo_fi['Moneda_del_Monto_Financiado']
    del df_costo_fi['aux1']
    
    df_costo_items = pd.DataFrame()
    df_costo_items['aux1'] = fac_costo_fi['Codigo_de_Subasta']
    df_costo_items['Grupo'] = np.arange(1, len(df_costo_items) + 1)
    df_costo_items['Código del item'] = fac_costo_fi['Codigo_de_Subasta']
    df_costo_items['Descripción del item'] = 'Comisión de estructuración por operación de adelanto por órdenes de compra y prestación de servicios.'
    df_costo_items['Unidad del item'] = 'ZZ'
    df_costo_items['Cantidad del item'] = '1'
    df_costo_items['Precio del item'] = round(fac_costo_fi[ COLUMNA_COMISION ].round(2),2)
    df_costo_items['Impuesto'] = '18%'
    df_costo_items['Gratuito'] = 'No'
    df_costo_items['ICBPER'] = 'No'
    del df_costo_items['aux1']

hoy_formateado = datetime.today().strftime('%d-%m-%Y')

with pd.ExcelWriter(ubi + f'\\Carga_masiva_Facturas_comision_estructuracion_ORDERING {hoy_formateado}.xlsx', engine='xlsxwriter') as writer:
    df_costo_fi.to_excel(writer, index=False, sheet_name='Comprobantes')
    df_costo_items.to_excel(writer, index=False, sheet_name='Items')








