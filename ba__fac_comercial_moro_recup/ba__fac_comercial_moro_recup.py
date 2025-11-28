# -*- coding: utf-8 -*-
"""
Created on Fri Nov 28 10:14:41 2025

@author: Joseph Montoya
"""

# =============================================================================
#    Actualizar ejecutivos factoring
# =============================================================================

import pandas as pd
import boto3
import json
import io
# import os
# from datetime import datetime

from pyathena import connect

#%% mes para insertar
codmes = '2025-11-30'

#%% Credenciales de AmazonAthena
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


with calculo_provision as (
    select 
      a.cierre_fecha Cierre_fecha,
      a.codmes Periodo_Cierre, 
      trim(a.Codigo_subasta) Codigo_subasta,
      trim(case
        when strpos(Codigo_subasta, '-') > 0
            then substr(Codigo_subasta, 1, strpos(Codigo_subasta, '-') - 1)
        else Codigo_subasta
        end) AS codigo_extraido,
      case  when Ejecutivo like '%369395334%' then 'ALBERT'
            when Ejecutivo like 'Albert Chocce' then 'ALBERT'
            when Ejecutivo like '503367918' then 'ALEJANDRA'
            when Ejecutivo like 'Alejandra Tupia' then 'ALEJANDRA'
            when Ejecutivo like '%1157450502%' then 'ALESSANDRO'
            when Ejecutivo like 'Alessandro Pachas Pacheco' then 'ALESSANDRO'
            when Ejecutivo like '%2060651040%' then 'CARLOS'
            when Ejecutivo like 'Carlos Paliza' then 'CARLOS'
            when Ejecutivo like '%291288188%' then 'CARMEN'
            when Ejecutivo like 'Nicol Alvarez' then 'CARMEN'
            when Ejecutivo like '%260893535%' then 'CRISTHIAN R.'
            when Ejecutivo like 'Cristhian Rodriguez' then 'CRISTHIAN R.'
            when Ejecutivo like '%260893538%' then 'DANIELA L.'
            when Ejecutivo like 'Daniela López' then 'DANIELA L.'
            when Ejecutivo like '%1689911023%' then 'DANNY'
            when Ejecutivo like 'Danny Adanaque' then 'DANNY'
            when Ejecutivo like 'Dasy Gutierrez' then 'DASY'
            when Ejecutivo like '%490537743%' then 'DASY'
            when Ejecutivo like 'Canal Digital' then 'PLATAFORMA'
            when Ejecutivo like '291494314' then 'PLATAFORMA'
            when Ejecutivo like '%427699034%' then 'FERNANDO'
            when Ejecutivo like 'Fernando Cano' then 'FERNANDO'
            when Ejecutivo like 'Giancarlo Loli' then 'GIANCARLO'
            when Ejecutivo like '%1342035417%' then 'GIANCARLO'
            when Ejecutivo like '%79302640%' then 'ISMAEL S.'
            when Ejecutivo like 'Ismael Sanchez' then 'ISMAEL S.'
            when Ejecutivo like 'Jackelin Dámazo' then 'JACKELIN'
            when Ejecutivo like '%462539703%' then 'JACKELIN'
            when Ejecutivo like '%488872060%' then 'JACKELINE'
            when Ejecutivo like 'Jackeline Abanto' then 'JACKELINE'
            when Ejecutivo like '%488872060%' then 'JENNER'
            when Ejecutivo like 'Jenner Albert López Puente' then 'JENNER'
            when Ejecutivo like '%80565119%' then 'JOAQUIN'
            when Ejecutivo like 'Joaquin Ponce Catalino' then 'JOAQUIN'
            when Ejecutivo like '%79064918%' then 'KARINA L.'
            when Ejecutivo like 'Karina Larrea' then 'KARINA L.'
            when Ejecutivo like '%858879470%' then 'LAURA'
            when Ejecutivo like 'Laura Donayre Francia' then 'LAURA'
            when Ejecutivo like '%1150715494%' then 'LUIS' 
            when Ejecutivo like 'Luis Cunya' then 'LUIS'
            when Ejecutivo like '%80751428%' then 'LUIS Z.' 
            when Ejecutivo like 'Luis de Zela' then 'LUIS Z.'
            when Ejecutivo like '%80751526%' then 'LUIS M.'
            when Ejecutivo like 'Luis Marquez' then 'LUIS M.'
            when Ejecutivo like '%80951500%' then 'MAURICIO ORTIZ'
            when Ejecutivo like 'Mauricio Ortiz' then 'MAURICIO ORTIZ'
            when Ejecutivo like '%600871348%' then 'MIGUEL' 
            when Ejecutivo like 'Miguel Ruiz' then 'MIGUEL'
            when Ejecutivo like '78043121' then 'MIGUEL C.'
            when Ejecutivo like 'Miguel Castro' then 'MIGUEL C.'
            when Ejecutivo like '%1269113980%' then 'PATRICIA F.'
            when Ejecutivo like 'Patricia Franco' then 'PATRICIA F.'
            when Ejecutivo like '%601852763%' then 'ROBERTO'
            when Ejecutivo like 'Roberto Mueras' then 'ROBERTO'
            when Ejecutivo like '%228514987%' then 'ROSARIO'
            when Ejecutivo like 'Rosario Pérez Palma' then 'ROSARIO'
            when Ejecutivo like '%729225537%' then 'SANDRA'
            when Ejecutivo like 'Sandra Horna' then 'SANDRA'
            when Ejecutivo like '%349371305%' then 'SANDRA E.'
            when Ejecutivo like 'Sandra Elias' then 'SANDRA E.'
            when Ejecutivo like '%79064939%' then 'SHEYLA E.'
            when Ejecutivo like 'Sheyla Escobedo' then 'SHEYLA E.'
            when Ejecutivo like '%1243783144%' then 'SOL'
            when Ejecutivo like 'Sol Garcia' then 'SOL'
            when Ejecutivo like '%1697831531%' then 'VIVIAN'
            when Ejecutivo like 'Vivian Pizarro' then 'VIVIAN'
            when Ejecutivo like '%1365116736%' then 'WALTER'
            when Ejecutivo like 'Walter Cabana' then 'WALTER'
            when Ejecutivo like '%80565125%' then 'WILFREDO'
            when Ejecutivo like 'Wilfredo Quispe Fabian' then 'WILFREDO'
            when Ejecutivo like '%1130372007%' then 'MARTIN'
            when Ejecutivo like '%471944227%' then 'FRANK J.'
            when Ejecutivo like '%373867171%' then 'KATHERIN'
            when Ejecutivo like '%75886970%' then 'RONALD'
            when Ejecutivo like 'Ronald Ubillus' then 'RONALD'
            when Ejecutivo like '%77303689%' then 'JUAN F.'
            when Ejecutivo like '%78296964%' then 'Jessica C.'
            when Ejecutivo like '%81469730%' then 'VIVIANA O.' 
            when Ejecutivo like '%80236326%' then 'JEAN PIERRE'
            else trim(Ejecutivo)
            end as Ejecutivo, 
        a.Saldo_capital_soles,
        a.Dias_atraso,
        a.status,
        a.Fecha_vencimiento,
        case when q_desembolso = 1 then a.utilidad_soles else 0 end Utilidad_soles,
        case when (Dias_atraso>60 and Dias_atraso<=210) then 0.2 else 0 end Penalidad,
        case when (Dias_atraso>60 and Dias_atraso<=210) then 0.2*Saldo_capital_soles else 0 end Provision
    
    from prod_datalake_analytics."view_fac_maestro_portafolio_factoring" a

    where 
        -- year(Fecha_desembolso) >= 2025
        Fecha_desembolso >= date '2025-01-01'
        
),

provision_ejecutivo as(
select Periodo_Cierre, Ejecutivo, sum(Utilidad_soles) as Utilidad,sum(Provision) AS Provision
    from calculo_provision
    group by Periodo_Cierre,Ejecutivo
    
),

metas as (
select 
        date_format(cierre, '%Y%m') Periodo_Cierre
        ,ejecutivo Ejecutivo
        ,meta Meta
from prod_datalake_master."ba__metas_factoring"
)
    
, calculo_morosidad as(
select 
        
        u.Periodo_Cierre
        ,u.Ejecutivo
        ,u.Utilidad / m.Meta Avance_bruto
        ,u.Utilidad Utilidad
        ,case 
            when (u.Utilidad / m.Meta) >= 0.8
                then least(coalesce(u.Provision, 0), u.Utilidad)
            else 0 
        end
        as Morosidad
    from provision_ejecutivo u
    left join metas m on (u.Ejecutivo = m.Ejecutivo and u.Periodo_Cierre = m.Periodo_Cierre)
),

morosidad_total_historico as(
select 
        cp.Ejecutivo Ejecutivo
        ,cp.codigo_extraido Codigo_subasta
        ,cp.Fecha_vencimiento Fecha_Vencimiento
        ,sum(
        round(
        case 
            when cp.Periodo_Cierre <='202509' then coalesce(bm.Morosidad,0)
            when cp.Periodo_Cierre >='202510' then 
                case 
                    when pt.Provision > 0 then 
                    cm.Morosidad * (cp.Provision / pt.Provision)
                    else 0 
                end
            else 0
        end
        ,2)
        ) as Morosidad_acumulada
    from calculo_provision cp
    
    left join provision_ejecutivo pt on (cp.Ejecutivo = pt.Ejecutivo and cp.Periodo_Cierre = pt.Periodo_Cierre)
    left join calculo_morosidad cm on (cp.Ejecutivo = cm.Ejecutivo and cp.Periodo_Cierre = cm.Periodo_Cierre)
    left join prod_datalake_master.ba_fac_morosidad_historica_comercial bm on (cp.Periodo_Cierre = bm.Periodo_Cierre and cp.codigo_extraido = bm.Codigo_subasta)
    group by cp.Ejecutivo,cp.codigo_extraido,cp.Fecha_vencimiento
)

, base_previa as (select 
    cp.Periodo_Cierre Periodo_Cierre
    ,cp.codigo_extraido Codigo_subasta
    ,coalesce(bm.Ejecutivo,cp.Ejecutivo) Ejecutivo
    ,max(round(coalesce(bm.Saldo_capital,cp.Saldo_capital_soles),2)) Saldo_capital_soles
    ,max(cp.Dias_atraso) Dias_atraso
    ,max(round(cp.Utilidad_soles,2)) Utilidad
    ,sum(round(cp.Provision,2)) Provision
    ,max(round(coalesce(cm.Avance_bruto,0),2)) Avance_bruto
    ,sum(round(coalesce(bm.Morosidad,coalesce(case 
        when pt.Provision > 0 then 
            cm.Morosidad * (cp.Provision / pt.Provision)
        else 0 
    end,0)),2)) as Morosidad
from calculo_provision cp
left join provision_ejecutivo pt on (cp.Ejecutivo = pt.Ejecutivo and cp.Periodo_Cierre = pt.Periodo_Cierre)
left join calculo_morosidad cm on (cp.Ejecutivo = cm.Ejecutivo and cp.Periodo_Cierre = cm.Periodo_Cierre)
left join prod_datalake_master.ba_fac_morosidad_historica_comercial bm on (cp.Periodo_Cierre = bm.Periodo_Cierre and cp.codigo_extraido = bm.Codigo_subasta)

group by cp.Periodo_Cierre
    ,cp.codigo_extraido 
    ,coalesce(bm.Ejecutivo,cp.Ejecutivo) 
)

select 
    bp.*, 
    case 
        when ((Saldo_capital_soles = 0 or Dias_atraso=0) and  bp.Periodo_Cierre>=DATE_FORMAT(mth.Fecha_vencimiento, '%Y%m'))  then mth.Morosidad_acumulada    
        else 0 
        end Recuperacion from base_previa bp
left join morosidad_total_historico mth on (bp.Ejecutivo = mth.Ejecutivo and bp.Codigo_subasta = mth.Codigo_subasta)



'''

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
df = pd.DataFrame(resultados, columns = column_names)
df['Periodo_Cierre'] = df['Periodo_Cierre'].astype(int)

# datos que se van a insertar
codmes_yyyymm = pd.to_datetime(codmes).strftime('%Y%m')
df.columns = df.columns.str.lower()

df = df [df['periodo_cierre'] == int(codmes_yyyymm) ]

#%% columna _timestamp
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
################################################################################
# Hora actual en Perú (UTC-5)
now = datetime.now(ZoneInfo("America/Lima"))

# Guardar directamente el objeto datetime
df["_timestamp"] = now - timedelta(hours=5)

#%% leer datos estaticos de meses anteriores
query = ''' select * from prod_datalake_sandbox.ba__fac_comercial_moro_recup '''

cursor = conn.cursor()
cursor.execute(query)

# Obtener los resultados
resultados = cursor.fetchall()

# Obtener los nombres de las columnas
column_names = [desc[0] for desc in cursor.description]

# Convertir los resultados a un DataFrame de pandas
df_estatico         = pd.DataFrame(resultados, columns = column_names)
df_estatico.columns = df_estatico.columns.str.lower()

df_estatico['periodo_cierre'] = df_estatico['periodo_cierre'].astype(int)

df_estatico = df_estatico[df_estatico['periodo_cierre'] != int(codmes_yyyymm)]

#%% concatenación para incluir el mes actual
df_final = pd.concat([df_estatico, df], ignore_index = True)

#%% CARGA AL LAKE
nombre_tabla = 'fac_comercial_moro_recup'

# Cliente de S3
s3 = boto3.client(
    "s3",
    aws_access_key_id     = creds["AccessKeyId"],
    aws_secret_access_key = creds["SecretAccessKey"],
    aws_session_token     = creds["SessionToken"],
    region_name           = creds["region_name"]
)

# ==== CONFIGURACIÓN ====
bucket_name = "prod-datalake-sandbox-730335218320"
s3_prefix   = f"{nombre_tabla}/"  # carpeta lógica en el bucket

# ==== EXPORTAR A PARQUET EN MEMORIA ====
parquet_buffer = io.BytesIO()
df_final.to_parquet(parquet_buffer, index=False, engine="pyarrow")
# también puedes usar engine="fastparquet" si lo prefieres

# Nombre de archivo con timestamp (opcional)
s3_key = f"{s3_prefix}{nombre_tabla}.parquet"

# Subir directamente desde el buffer
s3.put_object(
    Bucket = bucket_name,
    Key    = s3_key,
    Body   = parquet_buffer.getvalue()
)

print(f"✅ Archivo subido a s3://{bucket_name}{s3_key}")

#%%
print('fin')

