# -*- coding: utf-8 -*-
"""
Created on Fri May 16 18:48:02 2025

@author: Joseph Montoya
"""


import pandas as pd
from pyathena import connect

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

with Inversiones as (select a.code as subasta,
    a.proforma_simulation_currency as moneda,
    DATE(a.proforma_client_payment_date_expected) as fecha_vencimiento,
    MAX((CASE WHEN (a.product = 'factoring') THEN a.proforma_simulation_financing_total ELSE a.proforma_simulation_financing END)) Monto_Financiado,
    SUM(case when customer_email='gestora@prestamype.com' then amount else 0 end) monto_gestora,
    SUM(case when customer_email='factoringfinanzas@prestamype.com' then amount else 0 end) monto_fp,
    SUM(case when customer_email not in ('gestora@prestamype.com','factoringfinanzas@prestamype.com') then amount else 0 end) monto_crowd,
    c.status
    from prod_datalake_analytics.fac_requests         AS A
left join prod_datalake_analytics.fac_bids            AS B
    on a._id=b.request_id and b.status='ganado'
left join prod_datalake_analytics.fac_client_payments AS C
    on a._id = c.request_id
where a.status in ('closed')
    group by a.code, a.proforma_simulation_currency, DATE(a.proforma_client_payment_date_expected), c.status  )

Select
    subasta,
    fecha_vencimiento,
    case when monto_gestora >0 then 1 else 0 end as gestora,
    case when monto_fp >0 then 1 else 0 end as fondos_propios,
    case when monto_crowd >0 then 1 else 0 end as crowd,
    status
from Inversiones
where case when monto_gestora >0 then 1 else 0 end = 1
order by fecha_vencimiento desc

'''

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
pagos_gestora = pd.DataFrame(resultados, columns = column_names)

#%%
pagos_gestora.drop_duplicates(subset='subasta', inplace = True)

pagos_gestora = pagos_gestora[  pagos_gestora['status'] ==  'por pagar'  ]

#%%
import os
os.chdir(r'C:\Users\Joseph Montoya\Desktop\pruebas')

pagos_gestora.to_excel('garantías gestora.xlsx', index = False)





