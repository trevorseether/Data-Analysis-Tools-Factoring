# -*- coding: utf-8 -*-
"""
Created on Tue Sep  2 14:04:48 2025

@author: Joseph Montoya
"""

# =============================================================================
# paso 2, EXISTING LOANS
# =============================================================================
import pandas as pd
from datetime import datetime, timezone, timedelta
import numpy as np
from dateutil.relativedelta import relativedelta
from unidecode import unidecode

import os
os.chdir(r'C:\Users\Joseph Montoya\Desktop\LoanTape_PGH\temp\202509 existing')

def sum_date(codmes,months):

    temp = datetime.strptime(codmes, '%Y%m') + relativedelta(months=months)
    return datetime.strftime(temp,'%Y%m')

#%%
cierre = '202509'
fecha_cierre = pd.to_datetime(cierre, format='%Y%m') + pd.offsets.MonthEnd(0)

from datetime import datetime, timezone, timedelta
peru_tz = timezone(timedelta(hours=-5))
today_date = datetime.now(peru_tz).strftime('%Y%m%d')

#%%
bd_operaciones  = pd.read_excel(r"G:/.shortcut-targets-by-id/103C1ITMg88pYuTOUdrxjoOtU5u15eVkj/Cierre PGH/archivos/BD_Operaciones.xlsx")


bd_operaciones['Codigo Prestamo'] = bd_operaciones['Codigo Prestamo'].str.upper()

bd_operaciones['begin_date_codmes'] = bd_operaciones['Fecha Desembolso / reestructuración/cambio de fondo'].dt.strftime('%Y%m')

backup_op=bd_operaciones.copy()
bd_operaciones=bd_operaciones[bd_operaciones['begin_date_codmes'].astype(int)<=int(cierre)]

bd_operaciones['Monto de prestamo recibido soles'] = bd_operaciones.apply(lambda x: x['Monto de prestamo recibido'] * x['cambio'] if x['Moneda'] == 'DOLARES' else x['Monto de prestamo recibido'], axis=1)
bd_operaciones['Comision Prestamype soles'] = bd_operaciones.apply(lambda x: x['Comision Prestamype'] * x['cambio'] if x['Moneda'] == 'DOLARES' else x['Comision Prestamype'], axis=1)

bd_operaciones = bd_operaciones.rename(columns = {'Codigo Prestamo':'loan_id',
                                                  'Fecha Desembolso / reestructuración/cambio de fondo':'begin_date',
                                                  'CODIGO EMPRESARIO':'customer_id',
                                                  'Comision Prestamype': 'fees',
                                                  'Comision Prestamype soles': 'fees soles',
                                                  'Tasa Mensual\nInteres':'interest_rate',
                                                  'TIPO DE PRESTAMO ':'loan_type'})

#%%
bd_empresarios = pd.read_excel(r'G:/.shortcut-targets-by-id/103C1ITMg88pYuTOUdrxjoOtU5u15eVkj/Cierre PGH/archivos/BD_Operaciones.xlsx',sheet_name='Empresarios')

bd_empresarios['Codigo cliente'] = bd_empresarios['Codigo cliente'].str.upper()

bd_empresarios['Sexo'] = bd_empresarios['Sexo'].replace('-', np.nan)
bd_empresarios['Sexo'] = bd_empresarios['Sexo'].str.strip().str.title().replace({'Masculino':'Male','Femenino':'Female'})
#%%
path = r'G:/.shortcut-targets-by-id/103C1ITMg88pYuTOUdrxjoOtU5u15eVkj/Cierre PGH/archivos/Data Cierres - Prestamos GH.xlsx'
data_cierres = pd.read_excel(path, sheet_name = 'data')

data_cierres['loan_id'] = data_cierres['loan_id'].str.upper()
data_cierres['cierre'] = data_cierres['cierre'].astype(int).astype(str)
data_cierres[data_cierres['contract_id'].isnull()]

data_cierres['cierre'].unique()
#%%
data_deals = pd.read_excel(r'G:/.shortcut-targets-by-id/103C1ITMg88pYuTOUdrxjoOtU5u15eVkj/Cierre PGH/archivos/deals.xlsx')

df_sector = data_deals[['detalle_del_sector_del_negocio', 'detalle_del_sector_del_negocio__comercio_', 'detalle_del_sector_del_negocio__construccion_', 'detalle_del_sector_del_negocio__manufactura_', 'detalle_del_sector_del_negocio__servicios_']]
columnas_sector = df_sector.apply(lambda x: x.last_valid_index(), axis=1).fillna('detalle_del_sector_del_negocio'); columnas_sector
detalles = [data_deals.loc[i,columnas_sector[i]] for i in range(data_deals.shape[0])]


# Serie en minúsculas y sin tildes
detalles = [unidecode(i).lower() if isinstance(i, str) else i for i in detalles]        # La función unidecode() quita las tildes

detalles = pd.Series(detalles).str.replace('0','no_hay_informacion').str.replace('sin detalle','no_hay_informacion').fillna('no_hay_informacion')


# Columna detalles
data_deals['detalles'] = detalles


for i in range(len(detalles)):
  if 'no_hay_informacion' in detalles[i]:
    data_deals.loc[i,'customer_sector'] = 'no_hay_informacion'
  elif ('comerciante' in detalles[i]) | ('comercio' in detalles[i]):
    data_deals.loc[i,'customer_sector'] = 'comercio'
  elif 'manufactura' in detalles[i]:
    data_deals.loc[i,'customer_sector'] = 'manufactura'
  elif 'construccion' in detalles[i]:
    data_deals.loc[i,'customer_sector'] = 'construccion'
  elif 'dependiente' in detalles[i]:
    data_deals.loc[i,'customer_sector'] = 'dependiente'
  elif 'extraccion de petroleo y minerales' in detalles[i]:
    data_deals.loc[i,'customer_sector'] = 'extraccion de petroleo y minerales'
  elif 'otros servicios' in detalles[i]:
    data_deals.loc[i,'customer_sector'] = 'otros_servicios'
  elif 'agricultura' in detalles[i]:
    data_deals.loc[i,'customer_sector'] = 'agricultura (produccion agricola)'
  elif 'electricidad y agua' in detalles[i]:
    data_deals.loc[i,'customer_sector'] = 'electricidad y agua'
  elif 'administracion publica y defensa' in detalles[i]:
    data_deals.loc[i,'customer_sector'] = 'administracion publica y defensa'
    
data_deals[data_deals['customer_sector'].isnull()][['detalle_del_sector_del_negocio', 'detalle_del_sector_del_negocio__comercio_',
                                                    'detalle_del_sector_del_negocio__construccion_', 'detalle_del_sector_del_negocio__manufactura_',
                                                    'detalle_del_sector_del_negocio__servicios_','detalles','customer_sector']]#.isnull().sum()


# tipo_de_propiedad o collateral_description en inglés
data_deals['tipo_de_inmueble_principal'] = data_deals['tipo_de_inmueble_principal'].replace('departamento','flat').replace('local','shop').replace('casa','house').replace('terreno','terrain').replace('aires','airs').replace('edificio','edifice')
data_deals['tipo_de_inmueble_principal'].unique()

# Reemplazar "," por "" en tasacion_aprobada_dolares
data_deals['tasacion_aprobada_dolares'] = data_deals['tasacion_aprobada_dolares'].replace(",","")


# motivos de prestamo sin tildes
data_deals['motivo_del_prestamo'] = data_deals['motivo_del_prestamo'].fillna('No hay informacion').apply(unidecode)
data_deals['motivo_principal_del_prestamo'] = data_deals['motivo_principal_del_prestamo'].fillna('No hay informacion').apply(unidecode)

#%%
path = r'G:/.shortcut-targets-by-id/103C1ITMg88pYuTOUdrxjoOtU5u15eVkj/Cierre PGH/archivos/tipo_de_cambio.xlsx'
tipo_de_cambio = pd.read_excel(path, sheet_name = 'tipo_de_cambio')
tipo_de_cambio.codmes=tipo_de_cambio.codmes.astype(str).str.replace('\.0', '', regex=True)
data_cambios = tipo_de_cambio.copy()
data_cambios.sort_values('codmes',ascending=False).head()

#%%
# BD PAGOS
bd_pagos_read = pd.read_excel(rf'C:/Users/Joseph Montoya/Desktop/LoanTape_PGH/temp/temp_current_loans_{cierre}.xlsx')
bd_pagos = bd_pagos_read.copy()
bd_pagos['Codigo Operación'] = bd_pagos['Codigo Operación'].str.upper()

#%%

bd_pagos = bd_pagos.reset_index(drop=True)
bd_pagos = bd_pagos.drop_duplicates()

#COLUMNA SALDO POR CANCELAR ACTUALIZADO POR AMORTIZACION
bd_pagos['Saldo por cancelar'] = bd_pagos['Saldo por cancelar'].replace('-',0).replace('Reestructurado',0)
bd_pagos['Saldo por cancelar amortizado'] = bd_pagos['Saldo por cancelar esperado actualizada'].combine_first(bd_pagos['Saldo por cancelar'])

#COLUMNA intereses ACTUALIZADO POR AMORTIZACION
bd_pagos['Interes esperado fraccionado original'] = bd_pagos['Interes esperado fraccionado original'].replace('-',0).replace('                      ',0).fillna(0).astype(float)
bd_pagos['Interes esperado original'] = pd.to_numeric(
    bd_pagos['Interes esperado original']
    .astype(str)
    .str.replace(r'[^0-9.\-]', '', regex=True),
    errors='coerce' 
).fillna(0.0)
#bd_pagos['Interes esperado original'] = bd_pagos['Interes esperado original'].replace('-',0).replace('Reestructurado',0).replace('Reestructuración',0).replace('-\xa0',0).fillna(0).astype(float)
bd_pagos['Interes esperado original'] = bd_pagos['Interes esperado original'] + bd_pagos['Interes esperado fraccionado original'] 

#bd_pagos['Interes fraccionado actualizado'] = bd_pagos['Interes fraccionado actualizado'].replace('-',0).replace('Reestructurado',0).fillna(0).astype(float)
bd_pagos['Interes esperado actualizado'] = bd_pagos['Interes esperado actualizado'].replace('-',0).replace('Reestructurado',0).fillna(0).astype(float)

bd_pagos['Interes esperado amortizado'] = bd_pagos['Interes esperado actualizado'].combine_first(bd_pagos['Interes esperado original'])


#COLUMNA capital ACTUALIZADO POR AMORTIZACION
bd_pagos['Amortización esperada fraccionado original'] = bd_pagos['Amortización esperada fraccionado original'].replace('-',0).replace('|',0).fillna(0).astype(float)
bd_pagos['Amortización esperada original'] = pd.to_numeric(
    bd_pagos['Amortización esperada original']
    .astype(str)
    .str.replace(r'[^0-9.\-]', '', regex=True),
    errors='coerce' 
).fillna(0.0)
#bd_pagos['Amortización esperada original'] = bd_pagos['Amortización esperada original'].replace('-',0).replace('Reestructurado',0).fillna(0).astype(float)
bd_pagos['Amortización esperada original'] = bd_pagos['Amortización esperada original'] + bd_pagos['Amortización esperada fraccionado original']

bd_pagos['Amortización esperada actualizado']=bd_pagos['Amortización esperada actualizado'].replace('-',0).replace('Reestructurado',0).fillna(0).astype(float)

bd_pagos['Amortización esperada amortizado'] = bd_pagos['Amortización esperada actualizado'].combine_first(bd_pagos['Amortización esperada original'])

bd_pagos['Fecha de pago esperada original'] = bd_pagos['Fecha de pago esperada original'].replace('-', np.nan)

bd_pagos['TIPO DE PRESTAMO '] = bd_pagos['TIPO DE PRESTAMO '].replace(0,np.nan)

# Columnas en soles
temp_loan_id = pd.DataFrame({'Codigo Prestamo':bd_operaciones['loan_id'].unique()})
bd_operaciones['Codigo Prestamo']=bd_operaciones['loan_id']
temp_merge = pd.merge(temp_loan_id, bd_operaciones[['Codigo Prestamo','Moneda','begin_date_codmes']], on='Codigo Prestamo', how='left')
temp_merge = pd.merge(temp_merge, data_cambios, left_on='begin_date_codmes', right_on='codmes', how='left')
bd_pagos = pd.merge(bd_pagos, temp_merge, left_on='Codigo Operación', right_on='Codigo Prestamo', how='left')

bd_pagos['Cuota esperada mensual'] = bd_pagos['Cuota esperada mensual'].replace(",","").apply(lambda x: 0 if isinstance(x, str) else x).fillna(0)                               ### str.replace(",","")
bd_pagos['Saldo por cancelar'] = bd_pagos['Saldo por cancelar'].replace(",","").apply(lambda x: 0 if isinstance(x, str) else x).fillna(0)
bd_pagos['Amortización esperada original'] = bd_pagos['Amortización esperada original'].replace(",","").apply(lambda x: 0 if isinstance(x, str) else x).fillna(0)
bd_pagos['Interes esperado original'] = bd_pagos['Interes esperado original'].replace(",","").apply(lambda x: 0 if isinstance(x, str) else x).fillna(0)

bd_pagos['Saldo por cancelar amortizado'] = bd_pagos['Saldo por cancelar amortizado'].replace(",","").apply(lambda x: 0 if isinstance(x, str) else x).fillna(0)
bd_pagos['Amortización esperada amortizado'] = bd_pagos['Amortización esperada amortizado'].replace(",","").apply(lambda x: 0 if isinstance(x, str) else x).fillna(0)
bd_pagos['Interes esperado amortizado'] = bd_pagos['Interes esperado amortizado'].replace(",","").apply(lambda x: 0 if isinstance(x, str) else x).fillna(0)


bd_pagos['Cuota esperada mensual soles'] = bd_pagos.apply(lambda x: x['Cuota esperada mensual'] * x['cambio'] if x['Moneda_y'] == 'DOLARES' else x['Cuota esperada mensual'], axis=1)
bd_pagos['Saldo por cancelar soles'] = bd_pagos.apply(lambda x: x['Saldo por cancelar'] * x['cambio'] if x['Moneda_y'] == 'DOLARES' else x['Saldo por cancelar'], axis=1)
bd_pagos['Amortización esperada original soles'] = bd_pagos.apply(lambda x: x['Amortización esperada original'] * x['cambio'] if x['Moneda_y'] == 'DOLARES' else x['Amortización esperada original'], axis=1)
bd_pagos['Interes esperado original soles'] = bd_pagos.apply(lambda x: x['Interes esperado original'] * x['cambio'] if x['Moneda_y'] == 'DOLARES' else x['Interes esperado original'], axis=1)

bd_pagos['Saldo por cancelar amortizado soles'] = bd_pagos.apply(lambda x: x['Saldo por cancelar amortizado'] * x['cambio'] if x['Moneda_y'] == 'DOLARES' else x['Saldo por cancelar amortizado'], axis=1)
bd_pagos['Amortización esperada amortizado soles'] = bd_pagos.apply(lambda x: x['Amortización esperada amortizado'] * x['cambio'] if x['Moneda_y'] == 'DOLARES' else x['Amortización esperada amortizado'], axis=1)
bd_pagos['Interes esperado amortizado soles'] = bd_pagos.apply(lambda x: x['Interes esperado amortizado'] * x['cambio'] if x['Moneda_y'] == 'DOLARES' else x['Interes esperado amortizado'], axis=1)


bd_pagos['Fecha de pago del cliente'] = bd_pagos['Fecha de pago del cliente'].apply(lambda x: np.nan if isinstance(x,str) else x)
bd_pagos['Monto total pagado al crédito'] = bd_pagos['Monto total pagado al crédito'].apply(lambda x: np.nan if isinstance(x,str) else x)
bd_pagos['Capital pagado'] = bd_pagos['Capital pagado'].apply(lambda x: np.nan if isinstance(x,str) else x)
bd_pagos['Interes pagado'] = bd_pagos['Interes pagado'].apply(lambda x: np.nan if isinstance(x,str) else x)
bd_pagos['Penalidades'] = bd_pagos['Penalidades'].apply(lambda x: np.nan if isinstance(x,str) else x)
bd_pagos['Monto ampliado, renovado o sustituido'] = bd_pagos['Monto ampliado, renovado o sustituido'].apply(lambda x: np.nan if isinstance(x,str) else x)

bd_pagos['Monto total pagado al crédito soles'] = bd_pagos.apply(lambda x: x['Monto total pagado al crédito'] * x['cambio'] if x['Moneda_y'] == 'DOLARES' else x['Monto total pagado al crédito'], axis=1)
bd_pagos['Capital pagado soles'] = bd_pagos.apply(lambda x: x['Capital pagado'] * x['cambio'] if x['Moneda_y'] == 'DOLARES' else x['Capital pagado'], axis=1)
bd_pagos['Interes pagado soles'] = bd_pagos.apply(lambda x: x['Interes pagado'] * x['cambio'] if x['Moneda_y'] == 'DOLARES' else x['Interes pagado'], axis=1)
bd_pagos['Penalidades soles'] = bd_pagos.apply(lambda x: x['Penalidades'] * x['cambio'] if x['Moneda_y'] == 'DOLARES' else x['Penalidades'], axis=1)
bd_pagos['Monto ampliado, renovado o sustituido soles'] = bd_pagos.apply(lambda x: x['Monto ampliado, renovado o sustituido'] * x['cambio'] if x['Moneda_y'] == 'DOLARES' else x['Monto ampliado, renovado o sustituido'], axis=1)

bd_pagos = bd_pagos.dropna(subset = 'Codigo Contrato')
bd_pagos = bd_pagos.dropna(subset = 'Fecha de pago esperada original')


bd_pagos = bd_pagos.rename(columns = {'Codigo Operación':'loan_id',
                                      'Fecha de pago esperada original':'original_maturity_date'})

# Columna interest_outstanding (suma de "Intereses esperado original" en cuotas con atraso hasta la fecha de cierre)                                             ########
temp_bd = bd_pagos.query("`original_maturity_date` < @fecha_cierre")[['loan_id','Interes esperado original soles','Fecha de pago del cliente']]
temp_bd['Interes esperado original soles'] = temp_bd['Interes esperado original soles'].apply(lambda x: 0 if isinstance(x, str) else x)
temp_groupby = temp_bd[temp_bd['Fecha de pago del cliente'].isnull()].groupby('loan_id')['Interes esperado original soles'].sum().reset_index()


bd_pagos.shape #179863 177019
#%%
#prestamo con cuotas iguales
# caso P00389E00360Y00146NORP2P reestructurado antiguo, cambio manual en el cronograma
bd_pagos[bd_pagos.duplicated(subset=['loan_id', 'NRO CUOTAS'], keep=False)]['loan_id'].unique()
#%%
#completando Saldo por cancelar de ultima cuota
# Paso 1: Obtener la última cuota por código de préstamo
bd_copy=bd_pagos.sort_values(by=['loan_id','NRO CUOTAS']).copy()
bd_copy=bd_copy.reset_index(drop=True)
ultima_cuota = bd_copy.loc[bd_copy.groupby('loan_id')['NRO CUOTAS'].idxmax()]

# Paso 2: Calcular la diferencia
ultima_cuota['diferencia'] = ultima_cuota['Saldo por cancelar'] - ultima_cuota['Amortización esperada original']

# Paso 3: Filtrar donde la diferencia sea mayor a 1
errores = ultima_cuota[(ultima_cuota['diferencia'] > 50)]#&(ultima_cuota['TIPO DE PRESTAMO']=='CUOTA FIJA')]#&(ultima_cuota['diferencia'] > 5)

# Resultado
xd=errores[['loan_id', 'NRO CUOTAS', 'Saldo por cancelar', 'Amortización esperada original', 'diferencia','TIPO DE PRESTAMO']].copy()

xd #['TIPO DE PRESTAMO'].unique()
# copiar a diego Farro, paola, victor, steven, cesar , dirigido a josmel y diego farro
#%%
val=backup_op['Codigo Prestamo'].to_list()
bd_pagos[~bd_pagos['loan_id'].isin(val)]['loan_id'].unique()
#%%
saldos=pd.read_excel(r'C:/Users/Joseph Montoya/Desktop/LoanTape_PGH/saldos_casos.xlsx')
saldos.head()
#%%
backup_pagos = bd_pagos.copy()
#%%
#corregir saldos de casos de amortizaciones antiguas
bd_pagos=pd.merge(bd_pagos,saldos[['loan_id','SALDO FINAL']], how = 'left', on= 'loan_id')
idx_ultimas_saldo = bd_pagos[~bd_pagos['SALDO FINAL'].isnull()].groupby('loan_id')['NRO CUOTAS'].idxmax()
bd_pagos.loc[idx_ultimas_saldo, 'Saldo por cancelar soles'] = bd_pagos['SALDO FINAL']

#%%
#correccion de cuotas
# Paso 1: Obtener el índice de la última cuota por préstamo
idx_ultimas = bd_pagos.groupby('loan_id')['NRO CUOTAS'].idxmax()

# Paso 2: Calcular la diferencia en esas filas
diferencias = bd_pagos.loc[idx_ultimas, 'Saldo por cancelar soles'] - bd_pagos.loc[idx_ultimas, 'Amortización esperada original soles']

# Paso 3: Filtrar las que tengan diferencia > 50
idx_a_corregir = idx_ultimas[(diferencias > 50).values]

# Paso 4: Sumar la diferencia al Amortización esperada original, solo en esas últimas cuotas
bd_pagos.loc[idx_a_corregir, 'Amortización esperada original soles'] += diferencias[diferencias > 50]
bd_pagos.loc[idx_a_corregir, 'Cuota esperada mensual soles'] += diferencias[diferencias > 50]

#bd_pagos[bd_pagos['loan_id']=='R0000459'][['loan_id','NRO CUOTAS','Amortización esperada original soles','Saldo por cancelar soles','Cuota esperada mensual soles']]
#%%
#validar porcentaje de recaudacion
comparacion = bd_pagos.groupby(['loan_id', 'TIPO DE PRESTAMO ']).agg({
    'Cuota esperada mensual soles': 'sum',
    'Monto total pagado al crédito soles': 'sum'
}).reset_index()
comparacion[(comparacion['Monto total pagado al crédito soles']/comparacion['Cuota esperada mensual soles'])>2] 
#35 casos antes del 2022 no existia campos de amortizacion, se chancaba el crono original
#%%
(comparacion['Monto total pagado al crédito soles'].sum()/comparacion['Cuota esperada mensual soles'].sum())
#%%
# =============================================================================
# proceso
# =============================================================================
#%%
data_loan = bd_operaciones[['loan_id','customer_id','loan_type','begin_date','begin_date_codmes','Prestamo de destino donde se renovó',
                            'Prestamo reestructurado de destino','fees soles','Codigo Contrato','Moneda','Monto de prestamo recibido soles',
                            'interest_rate'
                            ]].copy()


data_merged = pd.merge(data_loan, data_cierres.query("`cierre` == @cierre")[['loan_id','capital_soles','diasatraso','status']],
                       on = 'loan_id', how = 'left')

data_merged['principal_outstanding'] = data_merged['capital_soles']
data_merged['days_past_due'] = data_merged['diasatraso']
data_merged['principal_outstanding'] = data_merged['principal_outstanding'].replace(0, np.nan)

bd_empresarios = bd_empresarios.rename(columns = {'Codigo cliente':'customer_id',
                                                  'Sexo':'customer_gender'})
bd_pagos = bd_pagos.rename(columns = {'Codigo Operación':'loan_id',
                                      'Fecha de pago esperada original':'original_maturity_date'})
data_deals = data_deals.rename(columns = {'codigo_de_contrato':'Codigo Contrato'})


# Completar 'asset_product' de data_merged con 'TIPO DE PRESTAMO ' de bd_operaciones
dic_status = {'SOLO INTERESES': 'BULLET W/ PERIODIC INTEREST',
              'CUOTA MIXTA': 'PARTIAL AMORTIZING',
              'CREDITO PUENTE': 'PARTIAL AMORTIZING',
              'AMORTIZACION LIBRE': 'PARTIAL AMORTIZING',
              'CUOTA FIJA': 'AMORTIZING'}

data_merged['asset_product'] = data_merged['loan_type'].replace(dic_status)

# Completar 'original_maturity_date' de data_merged con la última fecha de pago esperada original de bd_pagos
data_merged = pd.merge(data_merged,bd_pagos[['loan_id', 'original_maturity_date']].sort_values(['loan_id', 'original_maturity_date']).drop_duplicates(subset='loan_id', keep='last'), on='loan_id', how='left')
data_merged['original_maturity_date'] = pd.to_datetime(data_merged['original_maturity_date'])

# Actualizar 'customer_gender' de data_merged con bd_empresarios (Solo menor valor de Prioridad)
data_merged = pd.merge(data_merged, bd_empresarios[['customer_id','Prioridad','customer_gender']].sort_values(['customer_id','Prioridad']).drop_duplicates(subset='customer_id', keep='first'),
                      on='customer_id', how='left')


# Completar renewed_id
data_merged['renewed_id'] =data_merged['Prestamo de destino donde se renovó']

# Completar restructured_id
data_merged['restructured_id'] = data_merged['Prestamo reestructurado de destino']

# campo status
data_merged['status']=data_merged['status'].replace('VIGENTE','CURRENT').replace('FINALIZADO','CLOSED').fillna('CLOSED')

data_merged.loc[data_merged['restructured_id'].notna(), 'status' ] = 'RESTRUCTURED'
data_merged.loc[data_merged['renewed_id'].notna(), 'status' ] = 'RENEWED'

# Completar closure_date
id_finalizado = data_merged.query("`status` != 'CURRENT'")['loan_id']
data_merged = pd.merge(data_merged, bd_pagos[['loan_id','Fecha de pago del cliente']].query("`loan_id` in @id_finalizado").query("`Fecha de pago del cliente` < @fecha_cierre").drop_duplicates(subset = 'loan_id', keep='last'), on='loan_id', how='left')
data_merged['closure_date'] = pd.to_datetime(data_merged['Fecha de pago del cliente'])


# Completar Columnas
data_merged['branch'] = 'Prestamype'
data_merged['product'] = 'LOAN WITH GUARANTEE'
data_merged['interest_period'] = 'MONTHLY'

# Actualizar days_past_due    ~   Completar con vacíos los 'status' CLOSED
data_merged.loc[data_merged['status'].isin(['CLOSED','RENEWED','RESTRUCTURED']), 'days_past_due'] = np.nan

# Actualizar principal_outstanding   ~   Completar con vacíos los 'status' CLOSED
data_merged['principal_outstanding'] = data_merged['capital_soles']
data_merged.loc[data_merged['status'] == 'CLOSED', 'principal_outstanding'] = np.nan

# Completar Fees
data_merged['fees'] = data_merged['fees soles']

# Completar customer_sector

data_merged = pd.merge(data_merged, 
                       data_deals[['Codigo Contrato','customer_sector','tipo_de_inmueble_principal','tasacion_aprobada_dolares',
                                   'motivo_prestamo','motivo_del_prestamo']]
                       , on='Codigo Contrato', how='left')
data_merged['customer_sector'] = data_merged['customer_sector'].fillna('no_hay_informacion')

# Compeltar collateral_description
data_merged['collateral_description'] = data_merged['tipo_de_inmueble_principal']

# Completar collateral_value
data_merged['collateral_value'] = data_merged['tasacion_aprobada_dolares'].replace(0,np.nan)

# Completar loan_purpose:
data_merged['motivo_prestamo'] = data_merged['motivo_prestamo'].combine_first(data_merged['motivo_del_prestamo']).str.rstrip('.')
data_merged['loan_purpose'] = data_merged['motivo_prestamo'].fillna('no_hay_informacion')

# Columna interest_outstanding (suma de "Intereses esprado original" en cuotas con atraso hasta la fecha de cierre)                                             ########
temp_bd = bd_pagos[['loan_id','Interes esperado amortizado soles','Fecha de pago del cliente']].copy()
temp_bd.loc[temp_bd['Fecha de pago del cliente']>fecha_cierre,'Fecha de pago del cliente'] = np.nan

temp_groupby = temp_bd[temp_bd['Fecha de pago del cliente'].isnull()].groupby('loan_id')['Interes esperado amortizado soles'].sum().reset_index()
temp_merge = pd.merge(data_merged, temp_groupby, on='loan_id', how='left')
data_merged.loc[data_merged['status'] == 'CURRENT','interest_outstanding'] = temp_merge['Interes esperado amortizado soles']
data_merged.loc[data_merged['status'] != 'CURRENT','interest_outstanding'] = np.nan

# Columna (suma de "Amortización esperada original" en cuotas con atraso hasta la fecha de cierre)                                                              #########
temp_bd = bd_pagos.query("`original_maturity_date` < @fecha_cierre")[['loan_id','Amortización esperada amortizado soles','Fecha de pago del cliente']]
temp_bd['Amortización esperada amortizado soles'] = temp_bd['Amortización esperada amortizado soles'].apply(lambda x: 0 if isinstance(x, str) else x)
temp_groupby = temp_bd[temp_bd['Fecha de pago del cliente'].isnull()].groupby('loan_id')['Amortización esperada amortizado soles'].sum().reset_index()
temp_merge = pd.merge(data_merged, temp_groupby, on='loan_id', how='left')
data_merged.loc[data_merged['status'] == 'CURRENT','amortización_2'] = temp_merge['Amortización esperada amortizado soles']
data_merged.loc[data_merged['status'] != 'CURRENT','amortización_2'] = np.nan

# Completar total_loan_amount
temp_merge = pd.merge(bd_pagos, bd_operaciones, on='loan_id', how='left')
bd_pagos['TIPO DE PRESTAMO '] = temp_merge['TIPO DE PRESTAMO '].combine_first(temp_merge['loan_type'])

temp_groupby = bd_pagos.sort_values(['loan_id','NRO CUOTAS']).groupby('loan_id').agg({'TIPO DE PRESTAMO ': 'last','Cuota esperada mensual soles': 'sum', 'Saldo por cancelar soles': 'last', 'Amortización esperada original soles': 'last'}).reset_index()
temp_groupby['total_loan_calculado'] = temp_groupby['Cuota esperada mensual soles'] + temp_groupby['Saldo por cancelar soles'] - temp_groupby['Amortización esperada original soles']

temp_merge = pd.merge(data_merged, temp_groupby, on='loan_id', how='left')
temp_merge['total_loan_amount2'] = np.where(data_merged['loan_type'].isin(['AMORTIZACION LIBRE', 'CUOTA MIXTA', 'SOLO INTERESES', 'CREDITO PUENTE']), temp_merge['total_loan_calculado'], temp_merge['Cuota esperada mensual soles'])

data_merged.loc[data_merged['Moneda'] == 'DOLARES','total_loan_amount'] = temp_merge['total_loan_amount2']
data_merged['total_loan_amount'] = temp_merge['total_loan_amount2']

data_merged['currency'] = np.where(data_merged['Moneda'] == 'DOLARES',
                                   'USD',
                                   'PEN')
data_merged = data_merged.drop('Moneda',axis=1)

# Actualizar principal_amount
temp_merge['principal_amount'] = temp_merge['Monto de prestamo recibido soles'] + temp_merge['fees soles']
data_merged['principal_amount'] = temp_merge['principal_amount']


# Columna begin_date_codmes
data_merged['begin_date_codmes'] = data_merged['begin_date_codmes'].astype(int)


data_merged #4089 
#%%
bd_pagos[bd_pagos['loan_id']=='P00161E00006Y00064RENP2P'][['loan_id','Interes esperado amortizado soles','Interes esperado original soles','Fecha de pago del cliente']]
#%%
temp_bd = bd_pagos[['loan_id','Interes esperado amortizado soles','Fecha de pago del cliente']].copy()
temp_bd.loc[temp_bd['Fecha de pago del cliente']>fecha_cierre,'Fecha de pago del cliente'] = np.nan

temp_groupby = temp_bd[temp_bd['Fecha de pago del cliente'].isnull()].groupby('loan_id')['Interes esperado amortizado soles'].sum().reset_index()
temp_groupby[temp_groupby['loan_id']=='P03122E02905Y00281RYAFIP4']
temp_groupby
#%%
data_merged[data_merged['loan_id']=='P00161E00006Y00064RENP2P']
#%%
bd_pagos[bd_pagos['loan_id'].isin(errores)]['Moneda_x'].unique()
#%%
# error en  R0000456 en bd_operaciones monto desembolsado
# R0000456 caso de amortizacion antiguo
# P02741E01000Y00152RENP2P no debe tener monto renovado
# R0000260 PRECANCELADO DEBE ESTAR EN AMORTIZACION Y NO EN MONTO RENOVADO

errores = data_merged[(data_merged['total_loan_amount']-data_merged['principal_amount'])<0]['loan_id'].to_list() ##75
sum_cuotas=bd_pagos[bd_pagos['loan_id'].isin(errores)]
sum_cuotas= pd.merge(sum_cuotas,bd_operaciones,on='loan_id',how='left')
summm = sum_cuotas.groupby(['loan_id','Moneda_x']).agg({'Cuota esperada mensual soles': 'first', 'Código de cuota': 'count','Monto de prestamo recibido soles':'max'}).reset_index()
summm['new_total']=summm['Cuota esperada mensual soles']*summm['Código de cuota']+summm['Monto de prestamo recibido soles']
summm
#%%
""" RENOVACIONES POSIBLES QUE NO ESTAN ENLAZADAS A SU DESTINO
cliente GABRIELA LOURDES CATRACCHIA tiene dos codigos de empresario
E02954 NO TIENE RENOVACION
loan_id
P00021E00011Y00009NORP2P
P02016E01639Y00117NORP2P
P02741E01000Y00152RENP2P
P00031E00022Y00006NORP2P
R0000085
R0000260
R0000318
P00942E00608Y00144RENP2P
R0000319
P01097E00390Y00247NORP2P
P01151E00397Y00116NORP2P
P01518E01223Y00429NORP2P
P01870E01523Y00117NORP2P
P01970E01005Y00541RYAP2P
P02092E01701Y00575NORP2P
P02220E01781Y00607NORP2P
P02379E01937Y00656NORP2P
P01261E01001Y00334NORP2P
P01524E01225Y00371NORP2P
P01531E01226Y00033NORP2P
R0000416
P01560E01234Y00432NORP2P
P01599E01286Y00420NORP2P
P01808E01458Y00148NORP2P
P01832E01487Y00315NORP2P
P01961E01079Y00540RYAP2P
P02149E01730Y00490NORP2P
P02393E01466Y00281NORFIM2
P02416E01949Y00172NORP2P
P02438E02015Y00096NORP2P
P02557E02068Y00281NORFIM1
P02557E02068Y00281NORFIM2
P02610E02137Y00635NORP2P
P02624E00563Y00704NORP2P
P02784E02780Y00281NORFIP2
P02951E02916Y00281NORBID
P03005E02954Y00281NORFIP4
P03075E03036Y00281NORFIM2
"""
#%%
data_merged['customer_birth_year']=np.nan
data_merged['maturity_date']=np.nan
data_merged['downpayment']=np.nan
data_merged['principal_remaining']=np.nan
data_merged['fee_outstanding']=np.nan
data_merged['penalty_outstanding']=np.nan
data_merged['credit_situation'] = np.nan
data_merged.sort_values('Codigo Contrato',inplace=True)


df=data_merged[['loan_id',
'customer_id',
'customer_birth_year',
'customer_gender',
'customer_sector',
'branch',
'status',
'credit_situation',
'product',
'asset_product',
'currency',
'loan_purpose',
'begin_date',
'maturity_date',
'original_maturity_date',
'closure_date',
'principal_amount',
'total_loan_amount',
'interest_rate',
'interest_period',
'downpayment',
'fees',
'principal_remaining',
'principal_outstanding',
'interest_outstanding',
'fee_outstanding',
'penalty_outstanding',
'days_past_due',
'collateral_description',
'collateral_value',
'restructured_id',
'renewed_id']].copy()

#%%
print(df.shape)
df = df[df['loan_id'].isin(bd_pagos_read['Codigo Operación'].unique())]
print(df.shape)
#%%
df.to_excel(f'loans_file_{today_date}_current_loans.xlsx',index = False)
#%%
# =============================================================================
# payments
# =============================================================================
#%%
path = r'G:/.shortcut-targets-by-id/1HElohd7NvuWykuve6q6H6n4jYXrJW67Q/loan tape/202209_Loan Tape_actualizado.xlsx'
data_pay = pd.read_excel(path, sheet_name = 'Payments File')
print(data_pay.shape)
data_pay = data_pay.drop(columns=['IZQUIERDA', 'loan_id anterior'])
data_pay.columns
data_pay['Monto renovado'] = data_pay['Monto renovado'].replace(0,np.nan)
id_corregir = {'P01002E00684Y00203AMP':'P01002E00684Y00203RYA',
               'P01169E00619Y00123AMP':'P01169E00619Y00123RYA',
               'P01179E00592Y00140AMP':'P01179E00592Y00140RYA',
               'P01234E00681Y00196AMP':'P01234E00681Y00196RYA',
               'P01288E00477Y00273AMP':'P01288E00477Y00273RYA',
               'P01307E01043Y00227AMP':'P01307E01043Y00227RYA',
               'P01368E00760Y00332AMP':'P01368E00760Y00332RYA',
               'P01399E00670Y00254AMP':'P01399E00670Y00254RYA',
               'P01452E01175Y00402NOR':'P01452E01175Y00211NOR',
               'P01469E01178Y00404NOR':'P01469E01178Y00399NOR',
               'P01480E00893Y00411AMP':'P01480E00893Y00411RYA'}

data_pay['loan_id'] = data_pay['loan_id'].map(id_corregir).fillna(data_pay['loan_id'])
data_pay['loan_id'] = data_pay['loan_id'].str.upper()
#%%
data_pay2 = data_pay.copy()
ids = data_merged['loan_id'].drop_duplicates()

bd_pagos_temp = bd_pagos.query("`loan_id` in @ids").copy()
bd_pagos_temp = bd_pagos_temp.rename(columns={'Fecha de pago del cliente':'date','Monto total pagado al crédito soles':'amount',
                                              'Capital pagado soles':'principal_amount', 'Interes pagado soles':'interest_amount',
                                              'Monto ampliado, renovado o sustituido soles':'Monto renovado','Penalidades soles':'penalty_amount',
                                              'Código de cuota':'cuota'
                                              })
bd_pagos_temp = bd_pagos_temp[['loan_id','date','amount','principal_amount', 'interest_amount','penalty_amount','Monto renovado']]

bd_pagos_temp.iloc[:, 2:] = bd_pagos_temp.iloc[:, 2:].replace(0, np.nan)
bd_pagos_temp.shape #145016
#%%
# casos: 'loan_id' con fechas, pero lo demás vacío en todas las fechas
casos = []
bd_temp = bd_pagos_temp[~bd_pagos_temp['date'].isnull()]
ids_temp = bd_temp['loan_id'].unique()


for id in ids_temp:
  a = bd_temp.query("`loan_id` == @id").iloc[:,2:]
  if a[~a.isnull().all(axis=1)].shape[0] < a.shape[0]:
     casos.append(id)

print(len(casos))#; casos
#%%
# ... Se eliminan los registros que tienen fechas repetidas todas sin valores (si hay una con valor, se deja todo)
indices_temp = []

for id in casos:
  a = bd_pagos_temp.query("`loan_id` == @id")
  fecha_temp = a[a.iloc[:,2:].isnull().all(axis=1)]['date'].unique()[0]
  if a.query("`date` == @fecha_temp").iloc[:,2:].isnull().all(axis=1).value_counts().get(False,0) > 0:
    idx_temp = a.query("`date` == @fecha_temp").iloc[:,2:].isnull().all(axis=1)
    indices_temp.extend(idx_temp[idx_temp == True].index.tolist())

indices_temp

bd_pagos_temp = bd_pagos_temp.drop(indices_temp)

bd_pagos_temp = bd_pagos_temp.query("`date` <= @fecha_cierre")

ids = data_merged['loan_id'].to_list() 

temp_data = bd_pagos_temp[bd_pagos_temp['loan_id'].isin(ids)]

bd_pagos_temp.shape
#%%
bd_pagos_temp=bd_pagos_temp.dropna(subset=bd_pagos_temp.columns[2:], how='all')
bd_pagos_temp.dropna(subset='amount', how='all')
#%%
bd_pagos_temp = bd_pagos_temp.copy()
bd_pagos_temp['payment_id'] = np.nan
bd_pagos_temp['fee_amount'] = np.nan
bd_pagos_temp['payment_mode'] = np.nan
bd_pagos_temp['payment_source'] = np.nan
bd_pagos_temp['payment_source_payment_id'] = np.nan
#%%
df_payments= bd_pagos_temp[['loan_id', 'payment_id', 'date', 'amount', 'principal_amount',
       'interest_amount', 'fee_amount', 'penalty_amount', 'payment_mode',
       'payment_source', 'payment_source_payment_id', 'Monto renovado']]

#%%
print(df_payments.shape)
df_payments = df_payments[df_payments['loan_id'].isin(bd_pagos_read['Codigo Operación'].unique())]
print(df_payments.shape)

#%%
df_payments.to_excel(f'payments_{today_date}_current_loans.xlsx',index=False)

#%%
# =============================================================================
# REPAYMENTS
# =============================================================================
#%%
repayments_id = pd.merge(data_loan['loan_id'], bd_operaciones.query("@fecha_cierre >= `begin_date`")['loan_id'], on='loan_id', how='outer')

data_repayments = pd.merge(repayments_id, bd_pagos[['loan_id','original_maturity_date','Cuota esperada mensual soles','Amortización esperada original soles','Interes esperado original soles','Saldo por cancelar soles']], on='loan_id', how='left')
data_repayments = data_repayments.rename(columns={'original_maturity_date':'due_date','Cuota esperada mensual soles':'amount',
                                                  'Amortización esperada original soles':'principal_amount','Interes esperado original soles':'interest_amount'})
#data_repayments.iloc[:,2:5] = data_repayments.iloc[:,2:5].apply(lambda x: round(x), axis=1)

data_repayments
#%%
# data_repayments[data_repayments['loan_id'] == 'P02730E02242Y00063NORP2P']
data_loan[data_loan['loan_id'] == 'P02730E02242Y00063NORP2P']
#%%
data_repayments = data_repayments.dropna(subset=['due_date'])
#%%
print(data_repayments.shape)
data_repayments = data_repayments[data_repayments['loan_id'].isin(bd_pagos_read['Codigo Operación'].unique())]
print(data_repayments.shape)
#%%
data_repayments.to_excel(f'repayments_{today_date}_current_loans.xlsx',index=False)
#%%
# =============================================================================
# INDIVIDUAL
# =============================================================================
#%%
data_individual = pd.read_excel(path, sheet_name = 'Individual Loan Checks')
print(data_individual.shape)
data_individual = data_individual.drop(columns=['IZQUIERDA', 'loan_id anterior'])
data_individual.columns
#%%
individual_merged = data_merged[['loan_id','begin_date','product','original_maturity_date','principal_amount','interest_rate','principal_outstanding','days_past_due','status']]
                             
individual_merged = individual_merged.rename(columns={'original_maturity_date':'maturity_date','principal_outstanding':'Principal remaining','days_past_due':'DPD'})


# Actualizar columnas
data_pay3 = pd.merge(bd_pagos_temp, bd_pagos.groupby('loan_id').agg({'NRO CUOTAS':'max'}).reset_index(), on='loan_id', how='left')
temp_groupby = data_pay3.groupby('loan_id').agg({'amount':'sum', 'NRO CUOTAS':'count'}).reset_index()

temp_merge_group = pd.merge(individual_merged, temp_groupby, on='loan_id', how='left')

# individual_merged['Total amount paid to date'] = temp_merge_group['amount']
# individual_merged['Number of payments made'] = temp_merge_group['NRO CUOTAS']
individual_merged = temp_merge_group.rename(columns={'amount':'Total amount paid to date','NRO CUOTAS':'Number of payments made'})


print(individual_merged.shape)
print(individual_merged.isnull().sum())
#

individual_merged[individual_merged.duplicated(subset='loan_id', keep=False)]

individual_merged
#%%
df_individual= individual_merged[['loan_id', 'begin_date', 'maturity_date', 'principal_amount', 'product',
       'interest_rate', 'status', 'Total amount paid to date',
       'Principal remaining', 'Number of payments made', 'DPD']]

df_individual.rename(columns = {'maturity_date': 'original_maturity_date'}, inplace = True)
#%%
print(df_individual.shape)
df_individual = df_individual[df_individual['loan_id'].isin(bd_pagos_read['Codigo Operación'].unique())]
print(df_individual.shape)
#%%
df_individual.to_excel(f'individual_{today_date}_current_loans.xlsx',index=False)
#%%
# =============================================================================
# AGRE
# =============================================================================
#%%
df_aggregate = pd.read_excel(path, sheet_name = 'Aggregate Checks')
print(df_aggregate.shape)
#%%
df_aggregate.iloc[:,1] = [len(data_merged['loan_id'].unique() ),
                          len(data_merged['customer_id'].unique() ),
                          len(data_merged[data_merged['status'] != 'CURRENT']),
                          len(data_merged[data_merged['days_past_due']>90] ),
                          data_pay2['amount'].sum(),
                          data_merged['principal_amount'].sum(),
                          individual_merged['Principal remaining'].sum(),
                          data_merged['interest_outstanding'].sum(),
                          0,
                          0]
#%%
[len(data_merged['loan_id'].unique() ),
                          len(data_merged['customer_id'].unique() ),
                          len(data_merged[data_merged['status'] != 'CURRENT']),
                          len(data_merged[data_merged['days_past_due']>90] ),
                          bd_pagos_temp['amount'].sum(),
                          data_merged['principal_amount'].sum(),
                          individual_merged['Principal remaining'].sum(),
                          data_merged['interest_outstanding'].sum(),
                          0,
                          0]
#%%
# OK 1 4129
bd_operaciones.query("@fecha_cierre >= `begin_date`").shape[0]
#%%




