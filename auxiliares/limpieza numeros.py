# -*- coding: utf-8 -*-
"""
Created on Fri Aug 22 16:24:10 2025

@author: Joseph Montoya
"""

# =============================================================================
# limpieza de datos numérico
# =============================================================================

import re
import pandas as pd

df = pd.read_excel(r'G:/Mi unidad/Pagados 122024 en adelante.xlsx',
                   sheet_name = 'Online')

#%%

def convertir_a_float(valor):
    if pd.isna(valor):
        return None

    # Asegurar string
    texto = str(valor)

    # Extraer la primera parte numérica (ej: "1.499,34 x" -> "1.499,34")
    match = re.search(r"[\d]+(?:[.,]\d+)*", texto)
    if not match:
        return None

    numero = match.group(0)

    # Normalizar: todas las comas a puntos
    numero = numero.replace(",", ".")

    # Manejo de múltiples puntos (separador de miles)
    if numero.count('.') > 1:
        partes = numero.split('.')
        # concatenar todas menos la última como enteros (miles)
        numero = ''.join(partes[:-1]) + '.' + partes[-1]

    try:
        return float(numero)
    except ValueError:
        return None




def limpiar_valor_numerico(valor):
    """
    Limpia un valor que representa un número posiblemente mal formateado.
    Soporta separadores decimales ',' y '.', texto adicional y errores.

    Parámetros:
    - valor: string o cualquier tipo (cada celda de la columna)

    Retorna:
    - float o NaN si no se puede convertir
    """
    import re
    import numpy as np

    if pd.isna(valor):
        return np.nan

    # Convertir a string y reemplazar , por .
    valor_str = str(valor).replace(",", ".")

    # Buscar un patrón de número válido (ej: -123.45)
    match = re.search(r"[+-]?\d*\.?\d+", valor_str)
    if match:
        try:
            return float(match.group())
        except ValueError:
            return np.nan
    else:
        return np.nan

#%%
df['Interés Moratorio\n15 / 03 en adelante (comentarios)'] = df['Interés Moratorio\n15 / 03 en adelante (comentarios)'].apply( convertir_a_float)

df.to_excel(r'C:\Users\Joseph Montoya\Desktop\pruebas\col limp.xlsx')
