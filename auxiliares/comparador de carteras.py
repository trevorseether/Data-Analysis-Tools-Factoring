# -*- coding: utf-8 -*-
"""
Created on Wed Aug 13 10:30:51 2025

@author: Joseph Montoya
"""
# =============================================================================
# COMPARADOR ENTRE QUERYS DE VICTOR Y JOSEPH
# =============================================================================

import pandas as pd
import requests
from io import BytesIO
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

#%% QUERY FAC_OUTSTANDING
query = '''

SELECT 
*
FROM prod_datalake_analytics.fac_outst_unidos_f_desembolso_jmontoya

'''
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
outstanding_joseph = pd.DataFrame(resultados, columns = column_names)
outstanding_joseph = outstanding_joseph[   outstanding_joseph['FLAG_ORIGEN_OPERACION'] != 'compra interna']
print('outstanding creado')

#%% QUERY desembolsados victor
query = '''

with online as (
	select code,
		product,
		proforma_simulation_currency,
		case
			when proforma_simulation_currency = 'PEN' then coalesce(
				proforma_simulation_financing_total,
				proforma_simulation_financing
			) else coalesce(
				proforma_simulation_financing_total,
				proforma_simulation_financing
			) * exchange_rate
		end as Monto_financiado_solarizado,
		status,
		date(
			coalesce(
				interest_proforma_disbursement_date,
				fecha_de_desembolso__factoring_
			)
		) as fecha_desembolso,
		d.*
	from prod_datalake_analytics.fac_requests a
		left join (
			select distinct dealname,
				fecha_de_desembolso__factoring_
			from prod_datalake_master.hubspot__deal
			where pipeline = '14026011'
				and dealstage not in ('14026016', '14026018')
		) b on upper(trim(a.code)) = upper(trim(b.dealname))
		left join "prod_datalake_master"."cambio_sbs" c on date_add(
			'day',
			-1,
			date_trunc(
				'month',
				date_add(
					'month',
					1,
					date(
						coalesce(
							interest_proforma_disbursement_date,
							fecha_de_desembolso__factoring_
						)
					)
				)
			)
		) = c.Fecha_Maxima
		left join prod_datalake_analytics.transversal__dimension_tiempo d on date(
			coalesce(
				interest_proforma_disbursement_date,
				fecha_de_desembolso__factoring_
			)
		) = d.fecha
	where status in ('closed', 'ongoing', 'confirmed', 'pending')
		and date(
			coalesce(
				interest_proforma_disbursement_date,
				fecha_de_desembolso__factoring_
			)
		) is not null
),
offline as (
	select dealname,
		tipo_de_producto,
		moneda_del_monto_financiado,
		case
			when moneda_del_monto_financiado = 'PEN' then monto_financiado else monto_financiado * exchange_rate
		end as Monto_financiado_solarizado,
		'offline' as status,
		date(fecha_de_desembolso__factoring_) as fecha_desembolso,
		d.*
	from prod_datalake_master.hubspot__deal a
		left join "prod_datalake_master"."cambio_sbs" c on date_add(
			'day',
			-1,
			date_trunc(
				'month',
				date_add(
					'month',
					1,
					date(fecha_de_desembolso__factoring_)
				)
			)
		) = c.Fecha_Maxima
		left join prod_datalake_analytics.transversal__dimension_tiempo d on date(fecha_de_desembolso__factoring_) = d.fecha
	where pipeline = '14026011'
		and dealstage not in ('14026016', '14026018', '1105313628')
		and (
			tipo_de_operacion = 'Offline'
			or length(dealname) > 10
		)
		and fecha_de_desembolso__factoring_ is not null
),
operaciones as (
	select *
	from online
	union all
	select *
	from offline
)



 select * from operaciones
 
 
'''
cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
ops_victor = pd.DataFrame(resultados, columns = column_names)
print('ops vic creado')

#%% comparación 

ops_estan_en_outsts_pero_no_en_victor = outstanding_joseph[ ~ outstanding_joseph['code'].isin(list(ops_victor['code']))]
ops_estan_en_outsts_pero_no_en_victor.drop_duplicates(subset=['code'], keep='first', inplace=True)


ops_estan_en_vict_pero_no_outst = ops_victor[ ~ ops_victor['code'].isin(list(outstanding_joseph['code']))]

