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

import warnings
warnings.filterwarnings("ignore")

from datetime import datetime
hoy_formateado = datetime.today().strftime('%Y-%m-%d')

#%%
ubi = r'C:\Users\Joseph Montoya\Desktop\loans tape\2025 08'

cierre = 202508

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

query = ''' 
---   factoring_tape_jalfaro

WITH ranked_rows AS (
	SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY code
			ORDER BY codmes DESC
		) AS rn
	FROM prod_datalake_analytics.fac_outst_unidos_f_desembolso_jmontoya
-- 	where cast(codmes as int) <= 202412
	where cast(codmes as int) <= 202507
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
	date_format(a.transfer_date, '%Y-%m-%d') as begin_date,
	'' as maturity_date,
	date_format(a.e_payment_date, '%Y-%m-%d') as original_maturity_date,
	a.payment_date as closure_date,
	date_diff('day', a.transfer_date, a.payment_date ) as "Fecha Pago - Fecha Desembolso",
	date_diff('day', a.transfer_date, a.e_payment_date ) as "Fecha vencimiento proveedor - Fecha Desembolso",
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
	currency_auctions as currency,
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
		FROM prod_datalake_analytics.fac_outst_unidos_f_desembolso_jmontoya
-- 		WHERE codmes = '202412'
		WHERE codmes = '202507'
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
    dias = (30 * math.log((row['interest_amount'] / row['principal_amount']) + 1)) / math.log(1 + row['interest_rate'])
    return dias

df['dias calc'] = df.apply(calcular_dias, axis=1)
df['dias calc r0'] = df['dias calc'].round(0)

raro = df[   df['dias calc r0'] < 0]

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
df['original_maturity_date'] = df.apply(ajuste_closure_date, axis = 1)

###############################################################################

df["Fecha Pago - Fecha Desembolso"] = (df["closure_date"] - df["begin_date"]).dt.days

df["Fecha vencimiento proveedor - Fecha Desembolso"] = (df["original_maturity_date"] - df["begin_date"]).dt.days

del df['dias calc']
del df['dias calc r0']
#%% parcheo puntual
df['GARANTIA'] = """'=SI(J2="confirming";0;T2-Z2-AA2-Y2)"""
df['Importe a Devo Pro'] = """'=SI(J2="confirming";0;+AB2-AC2)"""

#%% porcentaje de adelanto
df['Monto Adelantado'] = df['Monto Adelantado'].astype(float).fillna(0)
df['principal_amount'] = df['principal_amount'].astype(float).fillna(0)
df['percentage of advance'] = df['Monto Adelantado'] / df['principal_amount']

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

#%% CÁLCULO DE Aggregate Checks
count_of_loans         = df.shape[0]
count_unique_clients   = df['customer_id'].unique().shape[0]
fully_paid_loans       = df[ df['Status'] == 'CLOSED'].shape[0]
defaulted_loans_late90 = df[ df['Status'] == 'CLOSED'].shape[0]

#%%
# df_pagos.to_excel(ubi + fr'\Payments File {hoy_formateado}.xlsx',
#                   index = False,
#                   sheet_name = 'Payments File')

#%% ESCRIBIR UN SOLO EXCEL
'''
with pd.ExcelWriter(f"{cierre}_Loan Tape Document For Alt Lenders Factoring Mb.xlsx", engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name ="Loans File", index=False)
    df_pagos.to_excel(writer, sheet_name ="Payments File", index=False)
'''

#%%% copiar columnas de excel ejemplo
ejemplo_original = r'C:/Users/Joseph Montoya/Desktop/loans tape/ejemplo/ejemplo.xlsx'
destino = f"{cierre}_Loan Tape Document For Alt Lenders Factoring Mb.xlsx"

# Copiar y renombrar al mismo tiempo
shutil.copy(ejemplo_original, destino)
print("✅ Archivo copiado y renombrado como 'destino'")

#%% Escribir los dataframes en el excel ya existente
with pd.ExcelWriter(
    destino,
    engine="openpyxl",
    mode="a",                       # append en vez de sobrescribir
    if_sheet_exists="replace"       # o "new" para crear nueva hoja aunque el nombre coincida
) as writer:
    df.to_excel(writer, sheet_name="Loans File", index=False)
    df_pagos.to_excel(writer, sheet_name="Payments File", index=False)

#%%
print('fin')



