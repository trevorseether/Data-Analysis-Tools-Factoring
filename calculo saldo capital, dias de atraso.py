# -*- coding: utf-8 -*-
"""
Created on Wed May 21 16:55:50 2025

@author: Joseph Montoya
"""

import pandas as pd
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

#%%
query = ''' 
WITH pagado as (
                select 
                    c.code                                 AS Subasta, 
                    round(sum(a.amount_capital_payment),2) AS capital_pagado 
               
                from prod_datalake_analytics.fac_client_payment_investors a
                left join prod_datalake_analytics.fac_client_payments b
                    on a.client_payment_id=b._id
                left join prod_datalake_analytics.fac_requests c
                    on b.request_id=c._id               
                group by c.code),
capital as (select
                fr.code,
                sum(fb.amount) as amount
            from prod_datalake_analytics.fac_bids as fb
            LEFT JOIN prod_datalake_analytics.fac_requests as fr
            on fb.request_id = fr._id
            where fb.status = 'ganado'
                group by fr.code )


SELECT
    FR.code        AS CodSubasta,
    FCP.STATUS,
    FR.product,
    
    cap.amount,
    P.capital_pagado,
    round(cap.amount - coalesce(P.capital_pagado,0)  ,2)                AS saldo_capital,
    
    FR.proforma_client_payment_date_expected       AS Fecha_de_Vencimiento_proveedor,
    HT.fecha_de_pago___confirmado_por_correo       AS Fecha_Vencimiento_confirmada_deudor,
    greatest(date_diff('day', FR.proforma_client_payment_date_expected, current_date),0)  AS atraso_segun_proveedor,
    greatest(date_diff('day', HT.fecha_de_pago___confirmado_por_correo, current_date),0)  AS atraso_segun_confirmacion_deudor,
    
    FR.Company_name                                AS proveedor,
    FR.company_ruc                                 AS RUC_proveedor,
    FR.user_third_party_name                       AS DEUDOR,
    FR.user_third_party_ruc                        AS RUC_deudor,
    (CASE WHEN (FR.product = 'factoring') THEN FR.proforma_simulation_financing_total ELSE FR.proforma_simulation_financing END) AS Monto_Financiado,
    FR.proforma_simulation_currency moneda_monto_financiado
    
FROM prod_datalake_analytics.fac_requests AS FR
LEFT JOIN prod_datalake_analytics.fac_client_payments AS FCP ON (FR._id = FCP.request_id)

LEFT JOIN pagado AS p 
    ON P.Subasta = FR.CODE
LEFT JOIN capital AS cap
    ON cap.code = fr.code
LEFT JOIN (     select * 
                from prod_datalake_analytics.hubspot_tickets 
                where subject is not null
                and hs_pipeline = 26417284) AS HT
    ON HT.SUBJECT = FR.CODE
where FR.user_third_party_ruc = '20370146994'
and FR.status = 'closed'
AND FCP.STATUS = 'por pagar'



'''

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
df = pd.DataFrame(resultados, columns = column_names)

#%%
df.to_excel(r'C:\Users\Joseph Montoya\Desktop\pruebas\deuda CORPORACION ACEROS AREQUIPA S.A. para compra de deuda(2).xlsx',
            index = False)


