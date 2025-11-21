# -*- coding: utf-8 -*-
"""
Created on Mon Sep  1 17:01:13 2025

@author: Joseph Montoya
"""

# =============================================================================
# LOAN TAPE -  SCRIPT 1
# =============================================================================
import pandas as pd
from datetime import datetime
# import numpy as np
# import datetime
# from tqdm import tqdm
from dateutil.relativedelta import relativedelta
# pip install unidecode
# from unidecode import unidecode

def sum_date(codmes,months):
    temp = datetime.strptime(codmes, '%Y%m') + relativedelta(months=months)
    return datetime.strftime(temp,'%Y%m')

cierre = '202510'
fecha_cierre = pd.to_datetime(cierre, format='%Y%m') + pd.offsets.MonthEnd(0)

# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_colwidth', None)

#%%
# BD PAGOS
#bd_pagos = pd.read_excel('G:/Mi unidad/Cierre PGH/archivos/cierre_fuentes/'+cierre+'/BD_Pagos.xlsm',sheet_name='BD PAGOS')
bd_pagos = pd.read_excel(f'G:/.shortcut-targets-by-id/103C1ITMg88pYuTOUdrxjoOtU5u15eVkj/Cierre PGH/archivos/cierre_fuentes/{cierre}/BD_Pagos.xlsm',
                         sheet_name='BD PAGOS')

bd_pagos['Codigo Operación'] = bd_pagos['Codigo Operación'].str.upper()
 
# datos del cierre anterior
bd_pagos_last_cierre = pd.read_excel(f'G:/.shortcut-targets-by-id/103C1ITMg88pYuTOUdrxjoOtU5u15eVkj/Cierre PGH/archivos/cierre_fuentes/{cierre}/BD_Pagos.xlsm',
                                     sheet_name='BD PAGOS')
bd_pagos_last_cierre['Codigo Operación'] = bd_pagos_last_cierre['Codigo Operación'].str.upper()

#%% esto se mantiene sin camios
loanfile_202412 = pd.read_excel('C:/Users/Joseph Montoya/Desktop/LoanTape_PGH/202412_Loan Tape Document For Alt Lenders V3.xlsx', 
                                sheet_name='Loans File')
# loanfile_202412.head(2)

#%%
#BD PAGOS CONTRATOS FINALIZADOS
bd_pagos_finalizados = pd.read_excel('C:/Users/Joseph Montoya/Desktop/LoanTape_PGH/BD_PAGOS contratos finalizados - P01004.xlsx',
                                     sheet_name='Hoja2')
bd_pagos_finalizados['Codigo Operación'] = bd_pagos_finalizados['Codigo Operación'].str.upper()

bd_pagos_finalizados_last_cierre = pd.read_excel('G:/.shortcut-targets-by-id/103C1ITMg88pYuTOUdrxjoOtU5u15eVkj/Cierre PGH/archivos/BD_PAGOS contratos finalizados - P01004.xlsx',
                                                 sheet_name='Hoja2')
bd_pagos_finalizados_last_cierre['Codigo Operación'] = bd_pagos_finalizados_last_cierre['Codigo Operación'].str.upper()

bd_pagos_finalizados_last_cierre = bd_pagos_finalizados_last_cierre[ ~bd_pagos_finalizados_last_cierre['Codigo Operación'].isin(list(bd_pagos_last_cierre['Codigo Operación']))]

#%%
bd_pagos_finalizados= bd_pagos_finalizados[~(bd_pagos_finalizados['Codigo Operación'].isna())]
bd_pagos_finalizados_last_cierre = bd_pagos_finalizados_last_cierre[~(bd_pagos_finalizados_last_cierre['Codigo Operación'].isna())]

#%%
bd_pagos_concat_last_cierre = pd.concat([bd_pagos_last_cierre,bd_pagos_finalizados_last_cierre],axis=0)
bd_pagos_concat_before = pd.concat([bd_pagos,bd_pagos_finalizados],axis=0)

#%%#%%
temp_new_loans = bd_pagos_concat_last_cierre[~(bd_pagos_concat_last_cierre['Codigo Operación'].isin(loanfile_202412['loan_id']))]
print(len(temp_new_loans['Codigo Operación'].unique()))

temp_new_loans.to_excel(fr'C:\Users\Joseph Montoya\Desktop\LoanTape_PGH\temp\temp_new_loans_{cierre}.xlsx', index=False)

aver = temp_new_loans[temp_new_loans['Codigo Contrato'] == 'P03593' ]
#%%#%%
def check_if_finished(my_column):
    # print(type(my_column))
    if 'FINALIZADO' in my_column.to_list():
        return 'FINALIZADO'
    else:
        return 'VIGENTE'
bd_pagos_last_cierre_status = bd_pagos_concat_last_cierre.groupby(['Codigo Operación'])['Condición actual del crédito'].agg([check_if_finished]).reset_index()
bd_pagos_before_status = bd_pagos_concat_before.groupby(['Codigo Operación'])['Condición actual del crédito'].agg([check_if_finished]).reset_index()

print(bd_pagos_last_cierre_status.shape)
print(bd_pagos_before_status.shape)

#%%#%%
temp_current_loan_ids = loanfile_202412[loanfile_202412['status'] == 'CURRENT']['loan_id'].unique()

temp_current_loans = bd_pagos_concat_last_cierre[(bd_pagos_concat_last_cierre['Codigo Operación'].isin(temp_current_loan_ids))]
print(len(temp_current_loans['Codigo Operación'].unique()))

temp_current_loans.to_excel(fr'C:\Users\Joseph Montoya\Desktop\LoanTape_PGH\temp\temp_current_loans_{cierre}.xlsx', index=False)

#%%#%%


#%%






