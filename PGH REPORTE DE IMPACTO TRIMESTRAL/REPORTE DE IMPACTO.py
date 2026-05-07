# -*- coding: utf-8 -*-
"""
Created on Wed May  6 10:32:44 2026

@author: Joseph Montoya
"""

# =============================================================================
# REPORTE DE IMPACTO
# =============================================================================

import pandas as pd
from pyathena import connect
import os
import numpy as np

import shutil
from datetime import datetime

import warnings
warnings.filterwarnings("ignore")

#%%
cierre_ = '2026-03-31'

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


query = ''' select * from prod_datalake_master.ba__data_portafolio_pgh 
limit 1000

'''

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
df_portafolio = pd.DataFrame(resultados, columns = column_names)
print('portafolio creado')

#%%

def restar_meses_eom(fecha_str, meses):
    """
    fecha_str: str en formato 'dd-mm-yyyy' (siempre fin de mes)
    meses: int (cantidad de meses a restar)

    return: str en formato 'dd-mm-yyyy'
    """
    fecha = pd.to_datetime(fecha_str, format="%Y-%m-%d")
    # Restar meses
    nueva_fecha = fecha - pd.DateOffset(months=meses)
    # Forzar fin de mes (por seguridad)
    nueva_fecha = nueva_fecha.replace(day=1)
    return nueva_fecha.strftime("%Y-%m-%d")
cierre_inicio = restar_meses_eom (cierre_, 2)


query = f''' 

select

    hs_object_id,
    dealname,
    dealstage,
    codigo_de_contrato,
    closedate,
    tienen_un_negocio_o_trabajo_independiente___no_considerar_alquileres_,
    sector_del_negocio_o_trabajo_independiente,
    ventas_promedio_mensual_indicada_por_el_cliente,
    ventas_promedio_mensuales_declaradas,
    ventas_promedios_mensuales,
    sector_del_negocio,
    detalle_del_sector_del_negocio,
    numero_de_empleos,
    motivo_del_prestamo,
    motivo_principal_del_prestamo,
    reciben_ingresos_por_alquileres_,
    fecha_de_nacimiento_del_solicitante_principal__pgh_

from prod_datalake_master.hubspot__deal
where pipeline in  ('6613542', '766601363')
and dealstage in ('19616271')
and cast(closedate as date) between date '{cierre_inicio}' and date '{cierre_}'

limit 1000

 '''

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
df_hubspot_deal = pd.DataFrame(resultados, columns = column_names)
print('hubspot extraído')

#%% se pone True en tiene_un_negocio_o_trabajo_independiente
df_hubspot_deal['tienen_un_negocio_o_trabajo_independiente___no_considerar_alquileres_'] = np.where(df_hubspot_deal['sector_del_negocio_o_trabajo_independiente'].notnull(),
                                                                                                    True,
                                                                                                    df_hubspot_deal['tienen_un_negocio_o_trabajo_independiente___no_considerar_alquileres_'])                         



