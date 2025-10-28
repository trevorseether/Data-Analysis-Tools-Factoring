# -*- coding: utf-8 -*-
"""
Created on Mon Aug 25 10:22:40 2025

@author: Joseph Montoya
"""

# =============================================================================
# LOAN TAPE FACTORING
# =============================================================================
import pandas as pd
from pyathena import connect
import json
import os
import shutil # para copiar archivos en windows
import numpy as np
import boto3
import io

import warnings
warnings.filterwarnings("ignore")

from datetime import datetime
hoy_formateado = datetime.today().strftime('%Y-%m-%d')

#%%
ubi = r'C:\Users\Joseph Montoya\Desktop\loans tape\2025 09'

cierre = 202509

crear_excel = False # True o False para crear excel

upload_s3 = True # True o False para cargar a Amazon Athena

#%%
os.chdir(ubi)

#%% Credenciales de AmazonAthena
txt_credenciales_athena = r"C:/Users/Joseph Montoya/Desktop/credenciales actualizado.txt" # no cambiar

with open(txt_credenciales_athena) as f:
    creds = json.load(f)

conn = connect(
    aws_access_key_id     = creds["AccessKeyId"],
    aws_secret_access_key = creds["SecretAccessKey"],
    aws_session_token     = creds["SessionToken"],
    s3_staging_dir        = creds["s3_staging_dir"],
    region_name           = creds["region_name"]
    
    )

#%%

query = f''' 
---   factoring_tape_jalfaro

WITH ranked_rows AS (
	SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY code
			ORDER BY codmes DESC
		) AS rn
	FROM prod_datalake_master.ba__fac_outstanding_monthly_snapshot
	where cast(codmes as int) <= {cierre}
), comision_exito as (
    
    select 
        fa.code,
        round(sum(coalesce(fcpi.interes_detail_success_commission,0)),2) as comision_exito,
        round(sum(coalesce(fcpi.interes_detail_withholding_tax,0)),2) as retencion,
        max(fcpi.currency_type_short_name) as moneda_inversion,
        max(client_payment_payments_date) as client_payment_payments_date,
        max(year(client_payment_payments_date) * 100 + month(client_payment_payments_date)) AS yyyymm

      from prod_datalake_analytics.fac_client_payment_investors as fcpi
    left join (select * from prod_datalake_analytics.fac_auctions where status != 'canceled') as fa
    on fa._id = fcpi.auction_id
    
    group by fa.code

--select * from from prod_datalake_analytics.tipo_cambio_jmontoya
), comision_exito_sol AS (      

    select 
        ce.*,
        tcjm.exchange_rate,
        case
            when moneda_inversion = 'USD' then round( tcjm.exchange_rate * comision_exito, 2 )
            else comision_exito
            end as comision_exito_soles,
        case
            when moneda_inversion = 'USD' then round( tcjm.exchange_rate * retencion, 2 )
            else retencion
            end as retencion_soles

    from comision_exito as ce

    left join prod_datalake_analytics.tipo_cambio_jmontoya as tcjm 
    on ce.yyyymm = tcjm.tc_codmes

)

SELECT a.code as loan_id,
	a.client_ruc as customer_id,
	a.provider_ruc as provider_id,
	'' as customer_birth_year,
	'' as customer_gender,
	f.business_industry as customer_sector,
	'Prestamype' as branch,
	CASE
		WHEN a.actual_status = 'vigente' THEN 'CURRENT' else 'CLOSED'
	END as status,
	'FACTORING' as product,
	a.product_type as asset_product,
	'' as loan_purpose,
	cast(a.transfer_date as date) as begin_date,
	'' as maturity_date,
	--date_format(a.e_payment_date, '%Y-%m-%d') as original_maturity_date,
    CAST(a.e_payment_date AS date) AS original_maturity_date,
	cast(a.payment_date as date) as closure_date,
	date_diff('day', cast(a.transfer_date as date), cast(a.payment_date as date)) as "Fecha Pago - Fecha Desembolso",
	date_diff('day', cast(a.transfer_date as date), cast(a.e_payment_date  as date)) as "Fecha vencimiento proveedor - Fecha Desembolso",
	g.proforma_simulation_currency as currency,
	a.amount_financed_soles as principal_amount,
	CASE
		WHEN currency_auctions = 'USD' THEN a.total_net_amount_pending_payment * a.exchange_rate 
		WHEN currency_auctions = 'USD' THEN a.total_net_amount_pending_payment * a.exchange_rate 
		ELSE a.total_net_amount_pending_payment
	END AS total_loan_amount,
	'.=SI(J2="confirming";X2+AB2;X2+Y2+AB2)' as total_loan_amount_func,
	
	CASE WHEN g.proforma_financing_interest_rate IS NOT NULL THEN g.proforma_financing_interest_rate
	ELSE g.proforma_simulation_financing_cost_rate END as interest_rate,
	
	'=+AT2' as "Monto Factura",
	'=+S2'  as "Monto Financiado",
	'=X2*((1+V2)^(P2/30)-1)' as "Intereses",
	(CASE WHEN (g.product = 'factoring') THEN g.proforma_simulation_financing_advance ELSE g.invoice_net_amount END) as "Monto Adelantado",
	'=X2-Z2' AS "FEE Estructutación",
	'.=SI(J2="confirming";0;T2-Z2-AA2-Y2)' as "GARANTIA",
	'=Y2' AS "Interes o Cost Financiamiento",
	'.=SI(J2="confirming";0;+AB2-AC2)' as "Importe a Devo Pro",
	ce.comision_exito_soles as "FEE Comi Éxito",
	ce.retencion_soles AS "FEE Retención",
	'' AS " ",
	
	'MONTHLY' as interest_period,
	'' AS downpayment,
	CASE 
	    WHEN g.proforma_simulation_currency = 'USD' and (g.product = 'factoring')
	        then (g.proforma_simulation_financing_commission + g.proforma_simulation_financing_commission_igv)*  a.exchange_rate
	    WHEN g.proforma_simulation_currency = 'USD' and (g.product != 'factoring')
	        then ((g.proforma_simulation_commission + g.proforma_simulation_commission_igv))*  a.exchange_rate
	    WHEN g.proforma_simulation_currency = 'PEN' and (g.product = 'factoring')
	        then (g.proforma_simulation_financing_commission + g.proforma_simulation_financing_commission_igv)
	    WHEN g.proforma_simulation_currency = 'PEN' and (g.product != 'factoring')
	        then ((g.proforma_simulation_commission + g.proforma_simulation_commission_igv))
	        END AS fees,
	        
	CASE
		WHEN proforma_end_simulation_warranty IS null
		AND currency_auctions = 'USD' THEN proforma_start_simulation_warranty * a.exchange_rate
		WHEN proforma_end_simulation_warranty IS null
		AND currency_auctions = 'PEN' THEN proforma_start_simulation_warranty
		WHEN currency_auctions = 'USD' THEN proforma_start_simulation_warranty * a.exchange_rate ELSE proforma_end_simulation_warranty
	END AS warranty,
	--currency_auctions as currency,
	'' as principal_remaining,
	CASE WHEN a.actual_status = 'vigente' THEN b.remaining_capital_soles 
	ELSE NULL END as principal_outstanding,
	CASE
	    WHEN a.actual_status != 'vigente' THEN NULL
		WHEN b.remaining_capital_soles IS null OR b.remaining_capital_soles = 0 THEN NULL
		WHEN proforma_end_simulation_financing_cost_value IS null AND currency_auctions = 'USD' THEN proforma_start_simulation_financing_cost_value * a.exchange_rate
		WHEN proforma_end_simulation_financing_cost_value IS null AND currency_auctions = 'PEN' THEN proforma_start_simulation_financing_cost_value
		WHEN currency_auctions = 'USD' THEN proforma_start_simulation_financing_cost_value * a.exchange_rate 
		ELSE proforma_end_simulation_financing_cost_value
	END as interest_outstanding,
	'' as fee_outstanding,
	'' as penalty_outstanding,
	b.dias_atraso as days_past_due,
	'invoice' as collateral_description,
	CASE
		WHEN currency_request = 'USD' THEN a.amount_of_invoices * a.exchange_rate ELSE a.amount_of_invoices
	END AS collateral_value,
	CASE
		WHEN currency_request = 'USD' THEN g.invoice_nominal_amount * a.exchange_rate ELSE g.invoice_nominal_amount
	END AS collateral_value2,
	CASE
		WHEN currency_request = 'USD' THEN g.invoice_net_amount * a.exchange_rate ELSE g.invoice_net_amount
	END AS collateral_value3,
	CASE
		WHEN proforma_end_simulation_financing_cost_value IS null
		AND currency_auctions = 'USD' THEN proforma_start_simulation_financing_cost_value * a.exchange_rate
		WHEN proforma_end_simulation_financing_cost_value IS null
		AND currency_auctions = 'PEN' THEN proforma_start_simulation_financing_cost_value
		WHEN currency_auctions = 'USD' THEN proforma_start_simulation_financing_cost_value * a.exchange_rate ELSE proforma_end_simulation_financing_cost_value
	END AS interest_amount -- para hoja Repayment Schedules File
FROM ranked_rows a
	LEFT JOIN (
		SELECT code,
			remaining_capital_soles,
			dias_atraso
		FROM prod_datalake_master.ba__fac_outstanding_monthly_snapshot
-- 		WHERE codmes = '202412'
		WHERE codmes = {cierre}
	) b ON a.code = b.code
	LEFT JOIN prod_datalake_analytics.fac_auctions c ON a.code = c.code
	LEFT JOIN prod_datalake_analytics.view_fac_user_third_parties d ON a.client_id = d._id
	LEFT JOIN prod_datalake_analytics.view_fac_third_parties f ON d.id_client = f._id
	LEFT JOIN prod_datalake_analytics.fac_requests g ON a.code = g.code
	
	LEFT JOIN comision_exito_sol as ce on ce.code = c.code
WHERE rn = 1 --limit 10 16,557
	AND (
		(
			c.status = 'finished'
			AND c.type_sale = 'auction'
		)
		OR (
			c.status = 'confirmed'
			AND c.type_sale = 'direct'
		)
	)
    -- and a.code = '8GA97sqH'
ORDER BY a.codmes;


'''

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
df = pd.DataFrame(resultados, columns = column_names)

#%% casteo de fechas
df['begin_date']             = pd.to_datetime(df['begin_date'], format='%Y-%m-%d')
df['original_maturity_date'] = pd.to_datetime(df['original_maturity_date'], format='%Y-%m-%d')

#%% calculo inverso de la fecha de pago
import math

def calcular_dias(row):
    # Aquí todo bien, los 3 están llenos (no vacíos), pero sale nulo cuando el interest_amount es negativo
    dias = (30 * math.log((row['interest_amount'] / row['principal_amount']) + 1)) / math.log(1 + row['interest_rate'])
    return dias

df['dias calc'] = df.apply(calcular_dias, axis=1)
df['dias calc r0'] = df['dias calc'].round(0)

raro  = df[  df['dias calc r0'] < 0]
raro2 = df[  pd.isna(df['dias calc r0']) ]

def ajuste_closure_date(df):
    if not pd.isna(df['closure_date']):
        return df["begin_date"] + pd.to_timedelta(df["dias calc r0"], unit="D")
    else:
        return df['closure_date']
df['closure_date'] = df.apply(ajuste_closure_date, axis = 1)

def ajuste_original_maturity_date(df):
    if pd.isna(df['closure_date']):
        return df["begin_date"] + pd.to_timedelta(df["dias calc r0"], unit="D")
    else:
        return df['original_maturity_date']
df['original_maturity_date'] = df.apply(ajuste_original_maturity_date, axis = 1)

###############################################################################

df["Fecha Pago - Fecha Desembolso"] = (df["closure_date"] - df["begin_date"]).dt.days

df["Fecha vencimiento proveedor - Fecha Desembolso"] = (df["original_maturity_date"] - df["begin_date"]).dt.days

df["Fecha Pago - Fecha Desembolso"] = np.where(df["Fecha Pago - Fecha Desembolso"].isnull(),
                                               df["Fecha vencimiento proveedor - Fecha Desembolso"],
                                               df["Fecha Pago - Fecha Desembolso"])
del df['dias calc']
del df['dias calc r0']
#%% parcheo puntual
df['GARANTIA'] = """'=SI(J2="confirming";0;T2-Z2-AA2-Y2)"""
df['Importe a Devo Pro'] = """'=SI(J2="confirming";0;+AB2-AC2)"""

#%% porcentaje de adelanto
df['Monto Adelantado'] = df['Monto Adelantado'].astype(float).fillna(0)
df['principal_amount'] = df['principal_amount'].astype(float).fillna(0)
df['percentage of advance'] = df['Monto Adelantado'] / df['principal_amount']

#%% ajustes sacados del script de Yovani
df['collateral_value'] = df['collateral_value2']
del df['collateral_value2']
del df['collateral_value3']

###############################################################################
# Lo conviertes en string y luego a datetime, asumiendo el día 1 del mes
fecha = pd.to_datetime(str(cierre), format="%Y%m")

# Ahora obtienes el último día del mes
ultimo_dia = fecha + pd.offsets.MonthEnd(0)

df[['closure_date',
    'original_maturity_date', 
    'begin_date']] = df[['closure_date',
                         'original_maturity_date',
                         'begin_date']].apply(pd.to_datetime)

# df['days_past_due'] = ((df['closure_date'] - df['original_maturity_date']).dt.days).apply(lambda x: x if x >= 0 else 0).astype(int)

###############################################################################
# Lo conviertes en string y luego a datetime, asumiendo el día 1 del mes
fecha_u = pd.to_datetime(str(cierre), format="%Y%m") + pd.offsets.MonthEnd(0)

df['days_past_due'] = np.where(df['status'] == 'CLOSED',
                               0,
                               ((fecha_u - df['original_maturity_date']).dt.days).apply(lambda x: x if x >= 0 else 0).astype(int))
###############################################################################
#%%%% CREACIÓN DE Repayment Schedules File
repayments = df[['loan_id',
                 'original_maturity_date',
                 'total_loan_amount',
                 'principal_amount',
                 'interest_amount',
                 'warranty',
                 'closure_date']]
                               
repayments.columns = ['loan_id',
                      'due_date',
                      'amount',
                      'principal_amount',
                      'interest_amount',
                      'warranty_amount',
                      'paid_date']

# repayments.to_excel('./loan_schedules_202506.xlsx', index=False)

#%%
# df.to_excel(ubi + fr'\Loans File {hoy_formateado}.xlsx', index = False,
#             sheet_name= 'Loans File')

#%%
# =============================================================================
# query 2
# =============================================================================
query = f'''
with client_payments_1 as (
	SELECT *,
		CASE
			WHEN currency != distribution_provider_currency_distribution
			AND distribution_provider_currency_distribution = 'USD' THEN 'pen2usd'
			WHEN currency != distribution_provider_currency_distribution
			AND distribution_provider_currency_distribution = 'PEN' THEN 'usd2pen' ELSE 'mantain'
		END AS amount_paid_exchange_flag,
		CASE
			WHEN distribution_provider_currency_amount_bussinesman != distribution_provider_currency_distribution
			AND distribution_provider_currency_distribution = 'USD' THEN 'pen2usd'
			WHEN distribution_provider_currency_amount_bussinesman != distribution_provider_currency_distribution
			AND distribution_provider_currency_distribution = 'PEN' THEN 'usd2pen' ELSE 'mantain'
		END AS guarantee_exchange_flag,
		CASE
			WHEN coalesce(distribution_provider_amount_bussinesman, 0) > 0 THEN distribution_provider_amount_bussinesman
			WHEN coalesce(pay_order_businessman_amount, 0) > 0
			AND coalesce(distribution_provider_igv, 0) > 0 THEN 0
			WHEN coalesce(pay_order_businessman_amount, 0) > 0
			AND coalesce(distribution_provider_igv, 0) = 0 THEN pay_order_businessman_amount ELSE 0
		END AS guarantee_paid
	FROM prod_datalake_analytics.fac_client_payment_payments
),
client_payments_2 as (
	SELECT a.guarantee_exchange_flag,
		CASE
			WHEN cardinality(a.distribution) = 0 THEN 'new' ELSE 'old'
		END AS flag,
		a.created_at,
		b.auction_code,
		b.auction_currency,
		b.status,
		a.payment_type,
		a._id as payment_id,
		a.client_payment_id,
		CASE
			WHEN amount_paid_exchange_flag = 'usd2pen' THEN coalesce(
				round(a.amount * a.distribution_provider_rate, 2),
				0
			)
			WHEN amount_paid_exchange_flag = 'pen2usd' THEN coalesce(
				round(a.amount / a.distribution_provider_rate, 2),
				0
			) ELSE a.amount
		END AS original_amount_paid,
		a.distribution_provider_currency_distribution as currency_distribution,
		a.distribution_provider_amount_payment_client as client_amount_paid,
		CASE
			WHEN coalesce(pay_order_businessman_amount, 0) > 0
			AND coalesce(distribution_provider_igv, 0) > 0 THEN coalesce(a.distribution_provider_interes_payment, 0) + coalesce(distribution_provider_igv, 0)
			WHEN coalesce(pay_order_businessman_amount, 0) = 0
			AND coalesce(distribution_provider_igv, 0) > 0 THEN coalesce(a.distribution_provider_interes_payment, 0) + coalesce(distribution_provider_igv, 0)
			WHEN coalesce(a.distribution_provider_interes_payment, 0) = 0
			AND coalesce(
				a.distribution_provider_amount_capital_payment,
				0
			) = 0
			AND coalesce(guarantee_paid = 0) THEN a.distribution_provider_interes ELSE coalesce(a.distribution_provider_interes_payment, 0)
		END as interest_paid,
		CASE
			WHEN coalesce(a.distribution_provider_interes_payment, 0) = 0
			AND coalesce(
				a.distribution_provider_amount_capital_payment,
				0
			) = 0
			AND coalesce(guarantee_paid, 0) = 0 THEN a.distribution_provider_capital_with_interes - a.distribution_provider_interes ELSE a.distribution_provider_amount_capital_payment
		END AS capital_paid,
		CASE
			WHEN guarantee_exchange_flag = 'usd2pen' THEN coalesce(
				round(
					a.guarantee_paid * a.distribution_provider_rate,
					2
				),
				0
			)
			WHEN guarantee_exchange_flag = 'pen2usd' THEN coalesce(
				round(
					a.guarantee_paid / a.distribution_provider_rate,
					2
				),
				0
			) ELSE coalesce(guarantee_paid, 0)
		END AS guarantee_paid,
		CAST(
			from_iso8601_timestamp(
				REPLACE(
					SUBSTRING(CAST(a.date AS varchar), 1, 19),
					' ',
					'T'
				)
			) AS DATE
		) as payment_date,
		max(
			CAST(
				from_iso8601_timestamp(
					REPLACE(
						SUBSTRING(CAST(a.date AS varchar), 1, 19),
						' ',
						'T'
					)
				) AS DATE
			)
		) OVER (PARTITION BY b.auction_code) AS last_paid_date,
		CAST(
			date_format(
				date_parse(
					replace(CAST(a.date AS varchar), '.000', ''),
					'%Y-%m-%d %H:%i:%s'
				),
				'%Y%m'
			) AS int
		) AS codmes,
		distribution_provider_amount_bussinesman,
		pay_order_businessman_amount,
		distribution_provider_igv
	FROM client_payments_1 a
		LEFT JOIN prod_datalake_analytics.fac_client_payments b on a.client_payment_id = b._id
)
SELECT auction_code as loan_id,
	payment_id,
	payment_date as "date",
	CASE
		WHEN auction_currency = 'USD' THEN (interest_paid + capital_paid + guarantee_paid) * b.exchange_rate ELSE (interest_paid + capital_paid + guarantee_paid)
	END AS amount,
	CASE
		WHEN auction_currency = 'USD' THEN capital_paid * b.exchange_rate ELSE capital_paid
	END AS principal_amount,
	CASE
		WHEN auction_currency = 'USD' THEN interest_paid * b.exchange_rate ELSE interest_paid 
	END AS interest_amount,
	CASE
		WHEN auction_currency = 'USD' THEN guarantee_paid * b.exchange_rate ELSE guarantee_paid
	END AS fee_amount,
	'' AS penalty_amount,
	'' AS payment_mode,
	'' AS payment_source,
	'' AS payment_source_payment_id
FROM client_payments_2 a --17,528
	left join prod_datalake_analytics.tipo_cambio_jmontoya b on cast(b.tc_codmes as int) = a.codmes
where a.codmes <= {cierre}
ORDER BY payment_date  


'''

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
df_pagos = pd.DataFrame(resultados, columns = column_names)

#%% creación de hoja Individual Loan Checks
temp_group = df_pagos.groupby('loan_id').agg({'amount':'sum', 'payment_id':'count'}).reset_index()
temp_group.columns = ['loan_id','amount','n_payments']

temp_indiv = df[['loan_id','begin_date','original_maturity_date','principal_amount',
                 'asset_product','interest_rate','status','principal_outstanding','days_past_due']]

individual = pd.merge(temp_indiv, temp_group, 
                      on  = 'loan_id', 
                      how = 'left')

individual = individual[['loan_id','begin_date','original_maturity_date',
                         'principal_amount','asset_product','interest_rate',
                         'status','amount','principal_outstanding','n_payments',
                         'days_past_due']]

# renombre de las columnas
individual.columns = [ 'loan_id',
                       'begin_date',
                       'original_maturity_date',
                       'principal_amount',
                       'asset_product',
                       'interest_rate',
                       'status',
                       'Total amount paid to date',
                       'Principal remaining',
                       'Number of payments made',
                       'DPD' ]

#%% CÁLCULO DE Aggregate Checks
count_of_loans           = df.shape[0] # conteo de operaciones
count_unique_clients     = df['customer_id'].unique().shape[0] # conteo de distintos clientes
fully_paid_loans         = df[ df['status'] == 'CLOSED'].shape[0] # conteo de ops 

fecha = pd.to_datetime(str(cierre), format="%Y%m") + pd.offsets.MonthEnd(0)
defaulted_loans_late90   = df[ (df['status'] == 'CURRENT') & 
                            ((fecha - pd.to_datetime(df['original_maturity_date'])).dt.days > 90) ].shape[0]

total_repayment          = individual['Total amount paid to date'].fillna(0).sum()
total_value_loan         = individual['principal_amount'].fillna(0).sum()
total_remaining_loan     = individual['Principal remaining'].fillna(0).sum()
# total_remaining_interest = repayments[pd.isna(repayments['paid_date'])]['interest_amount'].fillna(0).sum()
total_remaining_interest = df[ df['status'] == 'CURRENT']['interest_amount'].fillna(0).sum()

columnas = ['Count of all loans issued between yyyy-m', 'Count of all unique clients(Users) assoc',
            'Count of loans fully paid for loans issu', 'Count of defaulted loans (late by more t',
            'Total value of repayments collected for ', 'Total value of loan principal issued bet',
            'Total Remaining loan principal on loans ', 'Total Remaining loan interest on loans i',
            'Total Value of Discounts for loans issue', 'Total Value of Fees charged for loans is']
valores = [count_of_loans, count_unique_clients, fully_paid_loans, defaulted_loans_late90, 
           total_repayment, total_value_loan, total_remaining_loan, total_remaining_interest, 0, 0]

#%% ecuaciones loan tape
e1 = """.=+CONTARA('Individual Loan Checks'!A:A)-1"""
e2 = """.=+CONTARA(UNICOS('Loans File'!B:B))-1"""
e3 = """.=+CONTAR.SI.CONJUNTO('Loans File'!$H$2:$H$28080;"CLOSED")"""
e4 = """.=CONTAR.SI.CONJUNTO('Loans File'!H:H;"CURRENT";'Loans File'!AQ:AQ;">"&90)"""
e5 = """.=+SUMA('Payments File'!D:D)"""
e6 = """.=+SUMA('Individual Loan Checks'!D:D)"""
e7 = """.=+SUMA('Individual Loan Checks'!I:I)"""
e8 = """.=+SUMAR.SI.CONJUNTO('Loans File'!Y:Y;'Loans File'!H:H;"CURRENT")"""
e9 = ''
e10= ''

ecuas = [e1,e2,e3,e4,e5,e6,e7,e8,e9,e10]
# DATAFRAME DE AGREGATE CHECKS
aggregate_checks = pd.DataFrame({
        "Test Metric" : columnas,
        "Value as per <Alt Lender>": valores,
        "Ecu" : ecuas
})

#%% columna Withholding Amount (Detracción) del Loan File
df['Withholding Amount (Detracción)'] = df['collateral_value'].fillna(0) - df['total_loan_amount'].fillna(0)


#%% columna Financing Percentage
df['Financing Percentage'] = df['principal_amount'] / df['total_loan_amount']

#%% porcentaje de Avance
df['Percentage of advance'] = df['Monto Adelantado'] / df['principal_amount']

#%% columnas convertidas al final:
    
# columna total_loan_amount (collateral_value - Withholding Amount (Detracción))
df['total_loan_amount'] = '=+S2-T2'

df['principal_amount']  = '=+V2*U2'

df['Financing Cost']    = '=W2*((1+X2)^(P2/30)-1)'

df['Adavance Amount']   = '=+Z2*W2'

df['Structuring Fee']   = '=W2-AA2'

df['Warranty']          = '.=SI(J2="confirming";0;U2-AA2-AB2-Y2)'

df['Repayment Amount']  = '.=SI(J2="confirming";0;+AC2-Y2)'

df['Success Fee'] = df['FEE Comi Éxito']

df['Withholding Amount (IR)'] = df['Success Fee'].fillna(0)/2

#%% ordenamiento de columnas
columnas = ['loan_id','customer_id','provider_id','customer_birth_year','customer_gender',
'customer_sector','branch','status','product','asset_product','loan_purpose',
'begin_date','maturity_date','original_maturity_date','closure_date','Fecha Pago - Fecha Desembolso',
'Fecha vencimiento proveedor - Fecha Desembolso','currency','collateral_value','Withholding Amount (Detracción)',
'total_loan_amount','Financing Percentage','principal_amount','interest_rate','Financing Cost',
'Percentage of advance','Adavance Amount','Structuring Fee','Warranty','Repayment Amount',
'Success Fee','Withholding Amount (IR)',' ','interest_period','downpayment','fees',
'warranty','principal_remaining','principal_outstanding','interest_outstanding',
'fee_outstanding','penalty_outstanding','days_past_due','collateral_description']

df = df[columnas]
#%% ESCRIBIR UN SOLO EXCEL
'''
with pd.ExcelWriter(f"{cierre}_Loan Tape Document For Alt Lenders Factoring Mb.xlsx", engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name ="Loans File", index=False)
    df_pagos.to_excel(writer, sheet_name ="Payments File", index=False)
'''

#%%% copiar excel de ejemplo
ejemplo_original = r'C:/Users/Joseph Montoya/Desktop/loans tape/ejemplo/ejemplo.xlsx'
destino = f"{cierre}_Loan Tape Document For Alt Lenders Factoring Mb {hoy_formateado}.xlsx"

if crear_excel == True:
    # Copiar y renombrar al mismo tiempo
    shutil.copy(ejemplo_original, destino)
    print(f"✅ Archivo copiado y renombrado como '{destino}'")

#%% Escribir los dataframes en el excel ya existente
if crear_excel == True:
    with pd.ExcelWriter(
        destino,
        engine="openpyxl",
        mode="a",                       # append en vez de sobrescribir
        if_sheet_exists="replace"       # o "new" para crear nueva hoja aunque el nombre coincida
    ) as writer:
        df.to_excel(writer,         sheet_name="Loans File",               index =False)
        individual.to_excel(writer, sheet_name="Individual Loan Checks",   index =False)
        df_pagos.to_excel(writer,   sheet_name="Payments File",            index =False)
        repayments.to_excel(writer, sheet_name="Repayment Schedules File", index =False)
        aggregate_checks.to_excel(writer,sheet_name="agg checks",          index =False)

#%% Carga al lake
if upload_s3 == True:
    ######## hoja loans ###########################################################
    df['restructured_id'] = ''
    df['renewed_id'] = ''
    df['is_pledged_to_lendable'] = ''
    
    df['total_loan_amount'] = round(df['collateral_value'] - df['Withholding Amount (Detracción)'],2)
    df['principal_amount'] = round(df['total_loan_amount'] * df['Financing Percentage'] ,2)
    
    loans_s3 = df[[ 'loan_id','customer_id','customer_birth_year','customer_gender','customer_sector',
                    'branch','status','product','currency','asset_product','loan_purpose',
                    'begin_date','maturity_date','original_maturity_date','closure_date',
                    'principal_amount','total_loan_amount','interest_rate','interest_period',
                    'downpayment','fees','principal_remaining','principal_outstanding',
                    'interest_outstanding','fee_outstanding','penalty_outstanding',
                    'days_past_due','collateral_description','collateral_value',
                    'restructured_id','renewed_id','is_pledged_to_lendable',]]
    
    ######## hoja factoring? ######################################################
    df['interest_amount'] = ''
    factoring_s3 = df[['loan_id','customer_id','provider_id','customer_birth_year',
                        'customer_gender','customer_sector','branch','status','product','asset_product',
                        'loan_purpose','begin_date','maturity_date','original_maturity_date',
                        'closure_date','currency','principal_amount','total_loan_amount','interest_rate',
                        'interest_period','downpayment','fees','warranty','interest_amount','principal_remaining',
                        'principal_outstanding','interest_outstanding','fee_outstanding','penalty_outstanding',
                        'days_past_due','collateral_description','collateral_value','is_pledged_to_lendable',]]
    
    ######## hoja schedules #######################################################
    repayments['fee_amount'] = ''
    schedules_s3 = repayments[['loan_id','due_date','amount','principal_amount',
                                'interest_amount','fee_amount','paid_date']]
    
    ######## hoja transactions ####################################################
    df_pagos['Monto renovado']  = ''
    df_pagos['payment ranking'] = ''
    df_pagos['last payment']    = ''
    transactions_s3 = df_pagos[['loan_id','payment_id','date','amount','principal_amount',
                                'interest_amount','fee_amount','penalty_amount','payment_mode','payment_source',
                                'payment_source_payment_id','Monto renovado','payment ranking','last payment',]]

#%%
# loans_s3.to_csv('loantape_loans_factoring.csv',
#                 index = False,
#                 sep = ',',
#                 encoding = 'utf-8-sig')

# factoring_s3.to_csv('loantape_loans_v2_factoring.csv',
#                 index = False,
#                 sep = ',',
#                 encoding = 'utf-8-sig')

# schedules_s3.to_csv('loantape_schedules_factoring.csv',
#                 index = False,
#                 sep = ',',
#                 encoding = 'utf-8-sig')

# transactions_s3.to_csv('loantape_transactions_factoring.csv',
#                 index = False,
#                 sep = ',',
#                 encoding = 'utf-8-sig')

#%% cargar a Amazon athena
# Cliente de S3
s3 = boto3.client(
    "s3",
    aws_access_key_id        = creds["AccessKeyId"],
    aws_secret_access_key    = creds["SecretAccessKey"],
    aws_session_token        = creds["SessionToken"],
    region_name              = creds["region_name"]
)

###############################################################################
dataframes = [
    ("loantape_loans_factoring",        loans_s3),
    ("loantape_loans_v2_factoring",     factoring_s3),
    ("loantape_schedules_factoring",    schedules_s3),
    ("loantape_transactions_factoring", transactions_s3)
]

###############################################################################
bucket_name = "prod-datalake-raw-730335218320" 

for nombre_tabla, df in dataframes:
    # ==== CONFIGURACIÓN ==== 
    s3_prefix = f"manual/ba/{nombre_tabla}/" # carpeta lógica en el bucket 
    
    # ==== EXPORTAR A PARQUET EN MEMORIA ====
    csv_buffer = io.StringIO() 
    df.to_csv(csv_buffer, index=False, encoding="utf-8-sig") 
    
    # Nombre de archivo con timestamp (opcional, para histórico) 
    s3_key = f"{s3_prefix}{nombre_tabla}.csv" 
    
    # Subir directamente desde el buffer 
    s3.put_object(Bucket  = bucket_name, 
                  Key     = s3_key, 
                  Body    = csv_buffer.getvalue() 
                  )
    
    print(f"✅ Archivo subido a s3://{bucket_name}/{s3_key}")
    



#%%
print('fin')



