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
from pyathena import connect

#%% lectura de hoja de opps offline
archivo = 'G:/Mi unidad/Pagados 122024 en adelante.xlsx'
ubi = r'C:\Users\Joseph Montoya\Desktop\notas de crédito y débito'

df_offline = pd.read_excel( archivo,
                           sheet_name = 'Offline automatizado',
                           dtype = str)

# filtrando orderings
df_offline = df_offline[df_offline['tipo_de_producto'] == 'Ordering']

#%% operaciones que hay que omitir para evitar doble facturación (aquellos que ya tienen)

df_offline = df_offline[df_offline['Comprobante_costo_financiamiento_manual (en caso de ordering, automatizado)'].isna()]

#%% Conexión a Amazon Athena
import json
with open(r"C:/Users/Joseph Montoya/Desktop/credenciales actualizado.txt") as f:
    creds = json.load(f)

conn = connect(
    aws_access_key_id     = creds["AccessKeyId"],
    aws_secret_access_key = creds["SecretAccessKey"],
    aws_session_token     = creds["SessionToken"],
    s3_staging_dir        = creds["s3_staging_dir"],
    region_name           = creds["region_name"]
    )

# obtención del tipo de cambio y fecha de emision
query = """
select * from prod_datalake_master.prestamype__tc_contable
order by pk desc
limit 1
"""
cursor = conn.cursor()
cursor.execute(query)
# Obtener los resultados
resultados = cursor.fetchall()
# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]
# Convertir los resultados a un DataFrame de pandas
tc = pd.DataFrame(resultados, columns = column_names)
tc_contable = tc['tc_contable'].max()
fecha_emision = pd.to_datetime(tc['pk'].max())

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
    # df_costo_fi['Fecha de emisión'] = pd.to_datetime(datetime.now()).normalize()
    df_costo_fi['Fecha de emisión'] = fecha_emision.normalize()
    df_costo_fi['Fecha de emisión'] = (df_costo_fi['Fecha de emisión'].dt.strftime("%Y-%m-%d").astype("string"))
    df_costo_fi['Tipo de doc. cliente']   = 'RUC'
    df_costo_fi['N° de doc. Cliente'] = fac_costo_fi['ruc_proveedor']
    df_costo_fi['Nombre cliente'] = fac_costo_fi['razon_social']
    df_costo_fi['Correo cliente'] = fac_costo_fi['correo']
    df_costo_fi['Moneda'] = fac_costo_fi['Moneda_del_Monto_Financiado']
    del df_costo_fi['aux1']
    
    df_costo_items = pd.DataFrame()
    df_costo_items['aux1'] = fac_costo_fi['Codigo_de_Subasta']
    df_costo_items['Grupo'] = np.arange(1, len(df_costo_items) + 1)
    df_costo_items['Código del item'] = fac_costo_fi['Codigo_de_Subasta'] + " - CF"
    df_costo_items['Descripción del item'] = 'Interés compensatorio por operación de adelanto por órdenes de compra y prestación de servicios.'
    df_costo_items['Unidad del item'] = 'ZZ'
    df_costo_items['Cantidad del item'] = '1'
    df_costo_items['Precio del item'] = round(fac_costo_fi['Costo_Financiamiento_teorico'].round(2),2)
    df_costo_items['Impuesto'] = '18%'
    df_costo_items['Gratuito'] = 'No'
    df_costo_items['ICBPER'] = 'No'
    del df_costo_items['aux1']

hoy_formateado = datetime.today().strftime('%d-%m-%Y')

###############################################################################
# obteniendo el precio solarizado, para filtrar aquellos que deben ir en detracciones
df_costo_items['tc'] = tc_contable
df_costo_items = df_costo_items.merge(df_costo_fi[['Grupo', 'Moneda']], on = 'Grupo', how = 'left')
df_costo_items['monto solarizado aux'] = np.where(df_costo_items['Moneda'] == 'USD',
                                                  df_costo_items['Precio del item'] * tc_contable,
                                                  df_costo_items['Precio del item'])
df_costo_items['filtro detracciones'] = np.where(df_costo_items['monto solarizado aux'] >= 700,
                                                 'aplica detracciones',
                                                 '')
df_costo_fi = df_costo_fi.merge(df_costo_items[['Grupo', 'filtro detracciones']], on = 'Grupo', how = 'left')

df_cfi_detracciones = df_costo_fi[df_costo_fi['filtro detracciones'] =='aplica detracciones']
df_cit_detracciones = df_costo_items[df_costo_items['filtro detracciones'] =='aplica detracciones']

df_cfi_nd = df_costo_fi[df_costo_fi['filtro detracciones'] !='aplica detracciones']
df_cit_nd = df_costo_items[df_costo_items['filtro detracciones'] !='aplica detracciones']
###############################################################################
del df_cit_nd['tc']
del df_cit_nd['Moneda']
del df_cit_nd['monto solarizado aux']
del df_cit_nd['filtro detracciones']

del df_cfi_nd['filtro detracciones']

df_cfi_nd['Grupo'] = np.arange(1, len(df_cfi_nd) + 1)
df_cit_nd['Grupo'] = np.arange(1, len(df_cfi_nd) + 1)

with pd.ExcelWriter(ubi + f'\\Carga_masiva_Facturas_costo_financiamiento_ORDERING_sin_detraccion {str(fecha_emision)[0:10]}.xlsx', engine='xlsxwriter') as writer:
    df_cfi_nd.to_excel(writer, index=False, sheet_name='Comprobantes')
    df_cit_nd.to_excel(writer, index=False, sheet_name='Items')

###############################################################################
df_cfi_detracciones['Tipo de detracción'] = '037 - Demás servicios gravados con el IGV'
df_cfi_detracciones['Medio de pago'] = '001 - Depósito en cuenta'

df_cfi_detracciones = df_cfi_detracciones[['Grupo', 'Serie', 'Correlativo', 'Fecha de emisión',
                                           'Tipo de detracción', 'Medio de pago',
                                           'Tipo de doc. cliente', 'N° de doc. Cliente', 'Nombre cliente',
                                           'Correo cliente', 'Moneda']]
del df_cit_detracciones['tc']
del df_cit_detracciones['Moneda']
del df_cit_detracciones['monto solarizado aux']
del df_cit_detracciones['filtro detracciones']

df_cfi_detracciones['Grupo'] = np.arange(1, len(df_cfi_detracciones) + 1)
df_cit_detracciones['Grupo'] = np.arange(1, len(df_cit_detracciones) + 1)

with pd.ExcelWriter(ubi + f'\\Carga_masiva_Facturas_costo_financiamiento_ORDERING_con_detraccion {str(fecha_emision)[0:10]}.xlsx', engine='xlsxwriter') as writer:
    df_cfi_detracciones.to_excel(writer, index=False, sheet_name='Comprobantes')
    df_cit_detracciones.to_excel(writer, index=False, sheet_name='Items')

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
    # df_costo_fi['Fecha de emisión'] = pd.to_datetime(datetime.now()).normalize()
    df_costo_fi['Fecha de emisión'] = fecha_emision.normalize()
    df_costo_fi['Fecha de emisión'] = (df_costo_fi['Fecha de emisión'].dt.strftime("%Y-%m-%d").astype("string"))
    df_costo_fi['Tipo de doc. cliente']   = 'RUC'
    df_costo_fi['N° de doc. Cliente'] = fac_costo_fi['ruc_proveedor']
    df_costo_fi['Nombre cliente'] = fac_costo_fi['razon_social']
    df_costo_fi['Correo cliente'] = fac_costo_fi['correo']
    df_costo_fi['Moneda'] = fac_costo_fi['Moneda_del_Monto_Financiado']
    del df_costo_fi['aux1']
    
    df_costo_items = pd.DataFrame()
    df_costo_items['aux1'] = fac_costo_fi['Codigo_de_Subasta']
    df_costo_items['Grupo'] = np.arange(1, len(df_costo_items) + 1)
    df_costo_items['Código del item'] = fac_costo_fi['Codigo_de_Subasta'] + " - CE"
    df_costo_items['Descripción del item'] = 'Comisión de estructuración por operación de adelanto por órdenes de compra y prestación de servicios.'
    df_costo_items['Unidad del item'] = 'ZZ'
    df_costo_items['Cantidad del item'] = '1'
    df_costo_items['Precio del item'] = round(fac_costo_fi[ COLUMNA_COMISION ].round(2),2)
    df_costo_items['Impuesto'] = '18%'
    df_costo_items['Gratuito'] = 'No'
    df_costo_items['ICBPER'] = 'No'
    del df_costo_items['aux1']

hoy_formateado = datetime.today().strftime('%d-%m-%Y')

###############################################################################
# obteniendo el precio solarizado, para filtrar aquellos que deben ir en detracciones
df_costo_items['tc'] = tc_contable
df_costo_items = df_costo_items.merge(df_costo_fi[['Grupo', 'Moneda']], on = 'Grupo', how = 'left')
df_costo_items['monto solarizado aux'] = np.where(df_costo_items['Moneda'] == 'USD',
                                                  df_costo_items['Precio del item'] * tc_contable,
                                                  df_costo_items['Precio del item'])
df_costo_items['filtro detracciones'] = np.where(df_costo_items['monto solarizado aux'] >= 700,
                                                 'aplica detracciones',
                                                 '')
df_costo_fi = df_costo_fi.merge(df_costo_items[['Grupo', 'filtro detracciones']], on = 'Grupo', how = 'left')

df_cfi_detracciones = df_costo_fi[df_costo_fi['filtro detracciones'] =='aplica detracciones']
df_cit_detracciones = df_costo_items[df_costo_items['filtro detracciones'] =='aplica detracciones']

df_cfi_nd = df_costo_fi[df_costo_fi['filtro detracciones'] !='aplica detracciones']
df_cit_nd = df_costo_items[df_costo_items['filtro detracciones'] !='aplica detracciones']
###############################################################################
del df_cit_nd['tc']
del df_cit_nd['Moneda']
del df_cit_nd['monto solarizado aux']
del df_cit_nd['filtro detracciones']

del df_cfi_nd['filtro detracciones']

df_cfi_nd['Grupo'] = np.arange(1, len(df_cfi_nd) + 1)
df_cit_nd['Grupo'] = np.arange(1, len(df_cfi_nd) + 1)

with pd.ExcelWriter(ubi + f'\\Carga_masiva_Facturas_comision_estructuracion_ORDERING_sin_detraccion {str(fecha_emision)[0:10]}.xlsx', engine='xlsxwriter') as writer:
    df_cfi_nd.to_excel(writer, index=False, sheet_name='Comprobantes')
    df_cit_nd.to_excel(writer, index=False, sheet_name='Items')

###############################################################################
df_cfi_detracciones['Tipo de detracción'] = '037 - Demás servicios gravados con el IGV'
df_cfi_detracciones['Medio de pago'] = '001 - Depósito en cuenta'

df_cfi_detracciones = df_cfi_detracciones[['Grupo', 'Serie', 'Correlativo', 'Fecha de emisión',
                                           'Tipo de detracción', 'Medio de pago',
                                           'Tipo de doc. cliente', 'N° de doc. Cliente', 'Nombre cliente',
                                           'Correo cliente', 'Moneda']]
del df_cit_detracciones['tc']
del df_cit_detracciones['Moneda']
del df_cit_detracciones['monto solarizado aux']
del df_cit_detracciones['filtro detracciones']

df_cfi_detracciones['Grupo'] = np.arange(1, len(df_cfi_detracciones) + 1)
df_cit_detracciones['Grupo'] = np.arange(1, len(df_cit_detracciones) + 1)
with pd.ExcelWriter(ubi + f'\\Carga_masiva_Facturas_comision_estructuracion_ORDERING_con_detraccion {str(fecha_emision)[0:10]}.xlsx', engine='xlsxwriter') as writer:
    df_cfi_detracciones.to_excel(writer, index=False, sheet_name='Comprobantes')
    df_cit_detracciones.to_excel(writer, index=False, sheet_name='Items')

#%%
print('fin')

#%%
print('implementar que en caso de costo de financiamiento de confirming, debe ir - cf1 o -cf2, según si es interés normal o el interés por estrategia de diferencia de fechas')
