# -*- coding: utf-8 -*-
"""
Created on Tue Jul 22 12:24:01 2025

@author: Joseph Montoya
"""

# =============================================================================
# procesamiento de
# =============================================================================
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

hoy_formateado = datetime.today().strftime('%d-%m-%Y')  # o '%Y-%m-%d', etc.

#%%
archivo = 'G:/Mi unidad/Pagados 122024 en adelante.xlsx'

df_online = pd.read_excel( archivo,
                           sheet_name = 'Online',
                           dtype = {'Interés Bruto pagado a Crowd (Victor E)' : str,
                                    'Monto_Financiado' : str})
df_emitidos = pd.read_excel( archivo,
                           sheet_name = 'Masivos Emitidos',
                           )
# eliminación de filas vacías
col_factura_relacionada = 'COM. VINCULADO'   #'Factura Relacionada'
col_comprobante_emitido = 'COMPROBANTE EMITIDO'  #'Comprobante Emitido'
df_emitidos = df_emitidos[~(df_emitidos[col_factura_relacionada].isna() & df_emitidos[col_comprobante_emitido].isna() & df_emitidos['RUC'].isna())]

df_emitidos = df_emitidos[df_emitidos['TIPO DE COMPROBANTE'].str.contains('NOTA DE', na=False)]

#%%
df_online = df_online[['Subasta',
                       'Comprobante_costo_financiamiento',
                       'Fecha_Pago_real',
                       'Interés Bruto pagado a Crowd (Victor E)',
                       'Costo de Financiamiento Liquidado emp(numérico)',
                       'Interés Moratorio\n15 / 03 en adelante (numérico)']]



#%% limpieza numérica
# def convertir_a_float(valor):
#     if pd.isna(valor):
#         return None
#     valor = str(valor).replace(',', '.').replace('..', '.')
#     if valor.count('.') > 1:
#         # Si hay más de un punto, el primero es separador de miles → eliminar todos menos el último
#         partes = valor.split('.')
#         valor = ''.join(partes[:-1]) + '.' + partes[-1]
#     return float(valor)
def convertir_a_float(valor):
    import re
    if pd.isna(valor):
        return None

    # Convertir a string y normalizar
    valor = str(valor).replace(',', '.').replace('..', '.')

    # Buscar el primer número decimal en la cadena usando regex
    match = re.search(r'\d+(?:\.\d+)?', valor)
    if match:
        return float(match.group())
    return None  # Si no hay número válido, devuelve None
df_online['Interés Bruto pagado a Crowd (Victor E)(2)'] = df_online['Interés Bruto pagado a Crowd (Victor E)'].apply(convertir_a_float)
df_online['Costo de Financiamiento Liquidado emp(2)'] = df_online['Costo de Financiamiento Liquidado emp(numérico)'].apply(convertir_a_float)
df_online['Interés Moratorio\n15 / 03 en adelante(2)'] = df_online['Interés Moratorio\n15 / 03 en adelante (numérico)'].apply(convertir_a_float)

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

df_online['Interés Bruto pagado a Crowd (Victor E)(3)'] = df_online['Interés Bruto pagado a Crowd (Victor E)(2)'].apply(limpiar_valor_numerico)
df_online['Costo de Financiamiento Liquidado emp(3)'] = df_online['Costo de Financiamiento Liquidado emp(2)'].apply(limpiar_valor_numerico)
df_online['Interés Moratorio\n15 / 03 en adelante(3)'] = df_online['Interés Moratorio\n15 / 03 en adelante(2)'].apply(limpiar_valor_numerico)

#%%
df_online = df_online[['Subasta',
                       'Comprobante_costo_financiamiento',
                       'Fecha_Pago_real',
                       'Interés Bruto pagado a Crowd (Victor E)(3)',
                       'Costo de Financiamiento Liquidado emp(3)',
                       'Interés Moratorio\n15 / 03 en adelante(3)']]

df_online = df_online.merge(df_emitidos[['COM. VINCULADO','COMPROBANTE EMITIDO', 'FECHA DE EMISIÓN']],
                            left_on = 'Comprobante_costo_financiamiento',
                            right_on = 'COM. VINCULADO',
                            how = 'left')

del df_online['COM. VINCULADO']

df_online = df_online[    ~pd.isna(df_online['Interés Bruto pagado a Crowd (Victor E)(3)'])  | ~pd.isna(df_online['Costo de Financiamiento Liquidado emp(3)'])  |  ~pd.isna(df_online['Interés Moratorio\n15 / 03 en adelante(3)']) ]

df_online.to_excel(rf'C:\Users\Joseph Montoya\Desktop\pruebas\ingresos reales {hoy_formateado}.xlsx', index = False)




'loan_id',
'credito',
'situacion_credito',
'q',
'loan_type',
'tipo_cuota',
'contract_id',
'customer_id',
'investor_id',
'begin_date_year',
'begin_date_codmes',
'begin_date',
'currency_type',
'cuotas',
'rango_cuotas_ant',
'rango_cuotas',
'loan_amount',
'loan_amount_soles',
'Rango_MS',
'loan_amount_recieved',
'loan_amount_recieved_soles',
'Castigos',
'Mes_Castigo',
'capital_actual',
'capital_actual_atrasado',
'capital_actual_30d',
'capital_actual_60d',
'capital_actual_90d',
'estado_actual',
'last_paid',
'q_par60',
'diasatraso_actual',
'cierre_actual',
'fund',
'financiamiento',
'Distrito de Garantia',
'analista',
'zona_distrito',
'zona_distrito2',
'Categoria_pagador',
'max_atraso',
'meses_maduracion',
'Comision Prestamype',
'tasa_mensual_interes',
'rango_tasa_interes',
'Tipo Retención Operativa',
'canal',
'detalle_de_excepcion_',
'Nivel_Riesgo_Ant',
'Nivel_Riesgo',
'sector',
'sector_detalle',
'area_terreno_de_la_propiedad',
'rango_tamano_prop',
'tipo_de_inmueble_agrupado',
'tipo_de_inmueble_principal',
'sub_tipo_de_propiedad_principal',
'la_propiedad_es_precaria_',
'tasacion_aprobada_dolares',
'Precio$_m2',
'ltv',
'rango_ltv_ant',
'rango_ltv',
'estado_de_conservacion_de_la_propiedad',
'motivo_principal_del_prestamo',
'motivo_secundario_del_prestamo',
'edad',
'rango_edad',
'responsable_analista_de_riesgo',
'al_menos_uno_de_los_solicitantes_vive_en_la_garantia_',
'se_incluye_en_la_base_de_seguimiento_',
'seguimiento',
'tiene_un_trabajo_dependiente__o_jubilacion__o_pension_',
'tienen_un_negocio_o_trabajo_independiente___no_considerar_alquileres_',
'tipo_compra_de_deuda',
'tipo_de_regimen_tributario',
'tipo_de_ruc',
'dueno_de_garantia',
'fuente_principal_de_ingresos',
'numero_de_propiedades_puestas_en_garantia',
'Tipo de persona',
'Posicion contractual',
'Sexo',
'flag_ejecutado',
'fecha_ejecucion',
'Flag_VentaC',
'Fecha_VentaC',
'estado_ejecucion',
'motivo_cierre_ejecucion',
'nro_documento',
'periodo_rcc',
'periodo_rcc_2',
'nro_entidades',
'deuda_total_sbs_microf',
'calificacion_sbs_microf',
'nor',
'cpp',
'def',
'dud',
'per',
'linea_otorgada',
'linea_utilizada',
'linea_disponible',
'maximo_dias_atraso_ddirectasbs',
'rango_deuda_sf',
'deuda_directa',
'deuda_castigada',
'dia_atraso_9m',
'malos60_9m_oct24',
'plazo_pond',
'Tasa Pond',
'target_60_abr25',
'nuevo_rating_abr25',
'status_00',
'diasatraso_00',
'capital_00',
'capital_00_30d',
'capital_00_60d',
'capital_00_90d',
'status_01',
'diasatraso_01',
'capital_01',
'capital_01_30d',
'capital_01_60d',
'capital_01_90d',
'status_02',
'diasatraso_02',
'capital_02',
'capital_02_30d',
'capital_02_60d',
'capital_02_90d',
'status_03',
'diasatraso_03',
'capital_03',
'capital_03_30d',
'capital_03_60d',
'capital_03_90d',
'status_04',
'diasatraso_04',
'capital_04',
'capital_04_30d',
'capital_04_60d',
'capital_04_90d',
'status_05',
'diasatraso_05',
'capital_05',
'capital_05_30d',
'capital_05_60d',
'capital_05_90d',
'status_06',
'diasatraso_06',
'capital_06',
'capital_06_30d',
'capital_06_60d',
'capital_06_90d',
'status_07',
'diasatraso_07',
'capital_07',
'capital_07_30d',
'capital_07_60d',
'capital_07_90d',
'status_08',
'diasatraso_08',
'capital_08',
'capital_08_30d',
'capital_08_60d',
'capital_08_90d',
'status_09',
'diasatraso_09',
'capital_09',
'capital_09_30d',
'capital_09_60d',
'capital_09_90d',
'status_10',
'diasatraso_10',
'capital_10',
'capital_10_30d',
'capital_10_60d',
'capital_10_90d',
'status_11',
'diasatraso_11',
'capital_11',
'capital_11_30d',
'capital_11_60d',
'capital_11_90d',
'status_12',
'diasatraso_12',
'capital_12',
'capital_12_30d',
'capital_12_60d',
'capital_12_90d',
'status_13',
'diasatraso_13',
'capital_13',
'capital_13_30d',
'capital_13_60d',
'capital_13_90d',
'status_14',
'diasatraso_14',
'capital_14',
'capital_14_30d',
'capital_14_60d',
'capital_14_90d',
'status_15',
'diasatraso_15',
'capital_15',
'capital_15_30d',
'capital_15_60d',
'capital_15_90d',
'status_16',
'diasatraso_16',
'capital_16',
'capital_16_30d',
'capital_16_60d',
'capital_16_90d',
'status_17',
'diasatraso_17',
'capital_17',
'capital_17_30d',
'capital_17_60d',
'capital_17_90d',
'status_18',
'diasatraso_18',
'capital_18',
'capital_18_30d',
'capital_18_60d',
'capital_18_90d',
'status_19',
'diasatraso_19',
'capital_19',
'capital_19_30d',
'capital_19_60d',
'capital_19_90d',
'status_20',
'diasatraso_20',
'capital_20',
'capital_20_30d',
'capital_20_60d',
'capital_20_90d',
'status_21',
'diasatraso_21',
'capital_21',
'capital_21_30d',
'capital_21_60d',
'capital_21_90d',
'status_22',
'diasatraso_22',
'capital_22',
'capital_22_30d',
'capital_22_60d',
'capital_22_90d',
'status_23',
'diasatraso_23',
'capital_23',
'capital_23_30d',
'capital_23_60d',
'capital_23_90d',
'status_24',
'diasatraso_24',
'capital_24',
'capital_24_30d',
'capital_24_60d',
'capital_24_90d',
'status_27',
'diasatraso_27',
'capital_27',
'capital_27_30d',
'capital_27_60d',
'capital_27_90d',
'status_30',
'diasatraso_30',
'capital_30',
'capital_30_30d',
'capital_30_60d',
'capital_30_90d',
'status_33',
'diasatraso_33',
'capital_33',
'capital_33_30d',
'capital_33_60d',
'capital_33_90d',
'status_36',
'diasatraso_36',
'capital_36',
'capital_36_30d',
'capital_36_60d',
'capital_36_90d',
'status_39',
'diasatraso_39',
'capital_39',
'status_42',
'diasatraso_42',
'capital_42',
'status_45',
'diasatraso_45',
'capital_45',
'status_48',
'diasatraso_48',
'capital_48',



