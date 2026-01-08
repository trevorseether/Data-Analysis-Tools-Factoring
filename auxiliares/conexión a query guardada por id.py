# -*- coding: utf-8 -*-
"""
Created on Thu Jan  8 11:38:57 2026

@author: Joseph Montoya
"""

import boto3
from pyathena import connect
import pandas as pd
import json

# Credenciales
with open(r"C:/Users/Joseph Montoya/Desktop/credenciales actualizado.txt") as f:
    creds = json.load(f)

# Cliente Athena
athena = boto3.client(
    'athena',
    aws_access_key_id=creds["AccessKeyId"],
    aws_secret_access_key=creds["SecretAccessKey"],
    aws_session_token=creds["SessionToken"],
    region_name=creds["region_name"]
)

# ID de la query guardada (lo ves en la URL de Athena)
named_query_id = "51eb3903-f7b6-4b2c-a527-c35765e74134"

# Obtener SQL
response = athena.get_named_query(NamedQueryId=named_query_id)
query_sql = response["NamedQuery"]["QueryString"]

# Conexión PyAthena
conn = connect(
    aws_access_key_id=creds["AccessKeyId"],
    aws_secret_access_key=creds["SecretAccessKey"],
    aws_session_token=creds["SessionToken"],
    s3_staging_dir=creds["s3_staging_dir"],
    region_name=creds["region_name"]
)

cursor = conn.cursor()
cursor.execute(query_sql)

df = pd.DataFrame(cursor.fetchall(), columns=[c[0] for c in cursor.description])