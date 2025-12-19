# -*- coding: utf-8 -*-
"""
Created on Tue Aug 26 15:13:42 2025

@author: Joseph Montoya
"""

# =============================================================================
# COMISIÓN DE ESTRUCTURACIÓN
# =============================================================================

import pandas as pd
from pyathena import connect
import numpy as np
from datetime import datetime
hoy_formateado = datetime.today().strftime('%Y-%m-%d')

import warnings
warnings.filterwarnings("ignore")

from datetime import datetime, timezone, timedelta
peru_tz = timezone(timedelta(hours=-5))
today_date = datetime.now(peru_tz).strftime('%Y-%m-%d')
today_date = pd.Timestamp(today_date)
first_day_month = today_date.replace(day=1)

#%% Credenciales de AmazonAthena
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

#%%
fecha_corte = '2025-11' # YYYY-MM
ubi = r'C:\Users\Joseph Montoya\Desktop\reporte semanal contabilidad\2025\11'

#%% lecturas
# DIRECTORIO FIJO, NO CAMBIAR
gest_compro = pd.read_excel(r'G:/Mi unidad/Gestión de Comprobantes Factoring.xlsx',
                            sheet_name = 'Emitir Conf')

#%% QUERY PRINCIPAL

query = F''' 
select case
		when proforma_strategy_name = 'factoring-v1-new' then 'Estrategias' else ''
	end as Flag_Estrategias,
	product as producto,
	a.code as Subasta,
	--subasta
	a.status as Status,
	proforma_simulation_currency as Moneda,
	--moneda
	DATE(interest_proforma_disbursement_date) as Fecha_desembolso,
	--fecha desembolso registrado admin (registrado manualmente- es el real)
	DATE(
		date_add('hour', -5, CAST(a.closed_at AS timestamp))
	) as Fecha_venta,
	--fecha de venta
	DATE(proforma_client_payment_date_expected) as Fecha_esperada_pago,
	--Fecha de pago proveedor
	case
		when product = 'confirming' then proforma_simulation_financing else proforma_simulation_financing_total
	end as Monto_Financiado,
	--Monto Financiado
	case
		when product = 'confirming' then invoice_net_amount else proforma_simulation_net
	end as Monto_neto,
	--Monto Neto Factura
	case
		when product = 'confirming' then proforma_simulation_commission else proforma_simulation_financing_commission
	end as comision_sin_igv,
	--Comision sin IGV
	case
		when product = 'confirming' then proforma_simulation_commission_igv else proforma_simulation_financing_commission_igv
	end as igv_comision,
	--IGV comisión
	case
		when product = 'confirming' then proforma_simulation_commission + proforma_simulation_commission_igv else proforma_simulation_financing_commission + proforma_simulation_financing_commission_igv
	end as comision_total,
	--comisión con IGV
	b.code as Comprobante_Comision
from prod_datalake_analytics.fac_requests a
	left join (
		select *
		from prod_datalake_analytics.view_prestamype_fac_cpe
		where concept in ('commission-factoring', 'commission-confirming')
	) b on a._id = b.request_id
where status in('closed')
	and interest_proforma_disbursement_date is null
	and substring(
		cast(
			DATE(
				date_add('hour', -5, CAST(a.closed_at AS timestamp))
			) as varchar
		),
		1,
		7
	) = '{fecha_corte}'
union all
select case
		when proforma_strategy_name = 'factoring-v1-new' then 'Estrategias' else ''
	end as Flag_Estrategias,
	product as producto,
	a.code as Subasta,
	--subasta
	a.status as Status,
	proforma_simulation_currency as Moneda,
	--moneda
	DATE(interest_proforma_disbursement_date) as Fecha_desembolso,
	--fecha desembolso registrado admin
	DATE(
		date_add('hour', -5, CAST(a.closed_at AS timestamp))
	) as Fecha_venta,
	--fecha de venta
	DATE(interest_proforma_client_payment_date_expected) as Fecha_esperada_pago,
	--Fecha de pago proveedor
	case
		when product = 'confirming' then proforma_simulation_financing else interest_proforma_simulation_financing_total
	end as Monto_Financiado,
	--Monto Financiado
	case
		when product = 'confirming' then invoice_net_amount else interest_proforma_simulation_net
	end as Monto_neto,
	--Monto Neto Factura
	case
		when product = 'confirming' then proforma_simulation_commission else interest_proforma_simulation_financing_commission
	end as comision_sin_igv,
	--Comision sin IGV
	case
		when product = 'confirming' then proforma_simulation_commission_igv else interest_proforma_simulation_financing_commission_igv
	end as igv_comision,
	--IGV comisión
	case
		when product = 'confirming' then proforma_simulation_commission + proforma_simulation_commission_igv else interest_proforma_simulation_financing_commission + interest_proforma_simulation_financing_commission_igv
	end as comision_total,
	--comisión con IGV
	b.code as Comprobante_Comision
from prod_datalake_analytics.fac_requests a
	left join (
		select *
		from prod_datalake_analytics.view_prestamype_fac_cpe
		where concept in ('commission-factoring', 'commission-confirming')
	) b on a._id = b.request_id
where status in('closed')
	and interest_proforma_disbursement_date is not null
	and substring(
		cast(
			DATE(
				date_add('hour', -5, CAST(a.closed_at AS timestamp))
			) as varchar
		),
		1,
		7
	) = '{fecha_corte}'
'''

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
df = pd.DataFrame(resultados, columns = column_names)

#%% si comprobante es vacío, y producto es confirming, entonces 1
df['Flag_por cobrar'] = np.where(pd.isna(df['Comprobante_Comision']) & (df['producto'] == 'confirming'),
                                 1,
                                 0)

#%%
print('consultar con Diego si los factoring, pueden no tener factura')
fact_sin_fact = df[(df['producto'] != 'confirming')  &  pd.isna(df['Comprobante_Comision']) ]
if fact_sin_fact.shape[0] > 0:
    fact_sin_fact.to_excel(ubi + rf'\casos raros pregunar {fecha_corte} {hoy_formateado}.xlsx',
                            index = False)

#%% FILTRACIÓN
df_cobrar_1 = df[ df['Flag_por cobrar'] == 1]

#%% merge con gestión de comprobantes

df_cobrar_1['sub'] = df_cobrar_1['Subasta'].str.lower().str.strip()
gest_compro['sub'] = gest_compro['Codigo de subasta'].str.lower().str.strip()

df_cobrar_1 = df_cobrar_1.merge(gest_compro[[  'sub', 
                                               'Fecha para comprobante (emisión)',
                                               'Comprobante Emitido']],
                                on  = 'sub',
                                how = 'left')
del df_cobrar_1['sub']
del df_cobrar_1['Comprobante_Comision']
del df_cobrar_1['Flag_por cobrar']
df_cobrar_1['Flag_por cobrar'] = np.where(pd.isna(df_cobrar_1['Comprobante Emitido']) & (df_cobrar_1['producto'] == 'confirming'),
                                 1,
                                 0)

# columna del mes de envío
df_cobrar_1['Fecha envío'] = first_day_month
df_cobrar_1 = df_cobrar_1[['Fecha envío'] + [c for c in df_cobrar_1.columns if c != 'Fecha envío']]

#%% alerta para consultar a tesorería
consultar = df[ (df['producto'] == 'factoring') & pd.isna(df['Comprobante_Comision'])]

#%% CREACIÓN DE EXCELS
df.to_excel(            ubi + rf'\Comi estructuración {fecha_corte} {hoy_formateado}.xlsx',
                        index = False)
df_cobrar_1.to_excel(   ubi + rf'\Casos confirming {fecha_corte} {hoy_formateado}.xlsx',
                        index = False)

if consultar.shape[0] > 0:
    print('casos para consultar a tesorería')
    consultar.to_excel( ubi + rf'\Consultar a tesorería {fecha_corte} {hoy_formateado}.xlsx',
                        index = False)

