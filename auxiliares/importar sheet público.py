# -*- coding: utf-8 -*-
"""
Created on Fri Apr 25 11:57:31 2025

@author: Joseph Montoya
"""

import pandas as pd

# Reemplaza con el ID de tu Google Sheet
sheet_id = "1-rjxRNSqi5gkn6GNBz5wjyQEc4MnvQF2PBQmbC97aWc"
sheet_name = "Hoja1"  # El nombre exacto de la pestaña
# URL para exportar como CSV
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
# Leer directamente en un DataFrame
df = pd.read_csv(url, sep=',')

print(df.head())

df.head

