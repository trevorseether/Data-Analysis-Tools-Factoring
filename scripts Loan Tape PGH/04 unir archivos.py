# -*- coding: utf-8 -*-
"""
Created on Tue Sep  2 19:01:28 2025

@author: Joseph Montoya
"""
# =============================================================================
# concatenador de dataframes
# =============================================================================
import pandas as pd
import os 
import shutil # para copiar archivos en windows
import numpy as np

from datetime import datetime, timezone, timedelta
peru_tz = timezone(timedelta(hours=-5))
today_date = datetime.now(peru_tz).strftime('%Y%m%d')

today_date = '20251002'
#%%
cierre = '202508'
os.chdir(rf'C:\Users\Joseph Montoya\Desktop\LoanTape_PGH\temp\{cierre} existing')

path_new      = rf'C:\Users\Joseph Montoya\Desktop\LoanTape_PGH\temp\{cierre} news'
path_existing = rf'C:\Users\Joseph Montoya\Desktop\LoanTape_PGH\temp\{cierre} existing'

#%% lectura de los existings
lt_ex  = pd.read_excel(path_existing + r'\\' + fr'loans_file_{today_date}_updated.xlsx')
ind_ex = pd.read_excel(path_existing + r'\\' + fr'individual_{today_date}_updated.xlsx')
rep_ex = pd.read_excel(path_existing + r'\\' + fr'repayments_{today_date}_updated.xlsx')
pay_ex = pd.read_excel(path_existing + r'\\' + fr'payments_{today_date}_updated.xlsx')

#%% lectura de news
lt_new  = pd.read_excel(path_new + r'\\' + fr'loans_file_{today_date}_new_loans.xlsx')
ind_new = pd.read_excel(path_new + r'\\' + fr'individual_{today_date}_new_loans.xlsx')
rep_new = pd.read_excel(path_new + r'\\' + fr'repayments_{today_date}_new_loans.xlsx')
pay_new = pd.read_excel(path_new + r'\\' + fr'payments_{today_date}_new_loans.xlsx')

#%% concatenación
loans       = pd.concat([lt_ex,  lt_new], ignore_index=True)
individuals = pd.concat([ind_ex, ind_new], ignore_index=True)
repayments  = pd.concat([rep_ex, rep_new], ignore_index=True)
payments    = pd.concat([pay_ex, pay_new], ignore_index=True)

print('archivos concatenados')
#%% calculando la FEE con un porcentaje
loans['FEE'] = loans['fees']

loans['principal_amount'] = loans['principal_amount'].round(2)

loans['percent_Fee'] = loans['FEE'].fillna(0) / loans['principal_amount']

loans['FEE'] = """.=REDONDEAR(V2*Q2;2)"""

#%% añadiendo columna en Loans
# loans['Importe Solicitado'] = '=+Q2'
loans['Importe Principal']  = '=+Q2-U2'
loans['Interes']            = """.=SUMAR.SI.CONJUNTO('Repayment Schedules File'!E:E;'Repayment Schedules File'!A:A;'Loans File'!A2)"""
loans['Ganancia Total']     = '=+Q2+X2'
loans['interest_outstanding'] = """.=SI(G2="CLOSED";0;MAX(REDONDEAR(X2-SUMAR.SI.CONJUNTO('Payments File'!F:F;'Payments File'!A:A;'Loans File'!A2);2);0))"""

columnas_loans = [  'loan_id',
                    'customer_id',
                    'customer_birth_year',
                    'customer_gender',
                    'customer_sector',
                    'branch',
                    'status',
                    'credit_situation',
                    'product',
                    'asset_product',
                    'currency',
                    'loan_purpose',
                    'begin_date',
                    'maturity_date',
                    'original_maturity_date',
                    'closure_date',
                    'principal_amount',
                    'total_loan_amount',
                    'interest_rate',
                    'interest_period',
                    #'Importe Solicitado',
                    'FEE',
                    'percent_Fee',
                    'Importe Principal',
                    'Interes',
                    'Ganancia Total',
                    
                    #'downpayment',
                    #'fees',
                    #'principal_remaining',
                    'principal_outstanding',
                    'interest_outstanding',
                    #'fee_outstanding',
                    #'penalty_outstanding',
                    'days_past_due',
                    'collateral_description',
                    'collateral_value',
                    'restructured_id',
                    'renewed_id'
                        ]
loans = loans[columnas_loans]

#%%
# Calculando collateral_value vacíos
collateral_value_rate = 0.2
tc = 3.535
loans['collateral_value'] = loans['collateral_value'].fillna( (loans['principal_amount'] * (1/collateral_value_rate))/tc )
loans['collateral_value'] = loans['collateral_value'].round(2)

#%% CALCULO DE DAYS_PAST_DUE
fecha_u = pd.to_datetime(str(cierre), format="%Y%m") + pd.offsets.MonthEnd(0)

loans['days_past_due'] = np.where(loans['status'] == 'CLOSED',
                                  0,
                                  ((fecha_u - loans['original_maturity_date']).dt.days).apply(lambda x: x if x >= 0 else 0).astype(int))

#%% CÁLCULO DE Aggregate Checks
count_of_loans           = loans.shape[0] # conteo de operaciones
count_unique_clients     = loans['customer_id'].unique().shape[0] # conteo de distintos clientes
fully_paid_loans         = loans[ loans['status'] == 'CLOSED'].shape[0] # conteo de ops 

fecha = pd.to_datetime(str(cierre), format="%Y%m") + pd.offsets.MonthEnd(0)
defaulted_loans_late90   = loans[ (loans['status'] != 'CLOSED') & 
                            ((fecha - pd.to_datetime(loans['original_maturity_date'])).dt.days > 90) ].shape[0]

total_repayment          = individuals['Total amount paid to date'].fillna(0).sum()
total_value_loan         = individuals['principal_amount'].fillna(0).sum()
total_remaining_loan     = individuals['Principal remaining'].fillna(0).sum()
# total_remaining_interest = repayments[pd.isna(repayments['paid_date'])]['interest_amount'].fillna(0).sum()
rep_aux = repayments.merge(loans[['loan_id', 'status']],
                           on = 'loan_id',
                           how = 'left')
payments_aux = payments.merge(loans[['loan_id', 'status']],
                              on = 'loan_id',
                              how = 'left')

aux1 = rep_aux[ rep_aux['status'] == 'CURRENT']['interest_amount'].fillna(0).sum()
aux2 = payments_aux[ payments_aux['status'] == 'CURRENT']['interest_amount'].fillna(0).sum()
total_remaining_interest = aux1 - aux2

columnas = ['Count of all loans issued between yyyy-m','Count of all unique clients(Users) assoc',
            'Count of loans fully paid for loans issu','Count of defaulted loans (late by more t',
            'Total value of repayments collected for ','Total value of loan principal issued bet',
            'Total Remaining loan principal on loans ','Total Remaining loan interest on loans i',
            'Total Value of Discounts for loans issue', 'Total Value of Fees charged for loans is']
valores = [count_of_loans, count_unique_clients, fully_paid_loans, defaulted_loans_late90, 
           total_repayment, total_value_loan, total_remaining_loan, total_remaining_interest, 0, 0]

#%% ecuaciones loan tape
e1 = """.=CONTARA('Loans File'!A:A)-1"""
e2 = """.=CONTARA(UNICOS('Loans File'!B:B))-1"""
e3 = """.=+CONTAR.SI.CONJUNTO('Loans File'!G:G;"CLOSED")"""
e4 = """.=CONTAR.SI.CONJUNTO('Loans File'!G:G;"<>CLOSED";'Loans File'!AB:AB;">"&90)"""
e5 = """.=SUMA('Individual Loan Checks'!H:H)"""
e6 = """.=SUMA('Individual Loan Checks'!D:D)"""
e7 = """.=SUMA('Individual Loan Checks'!I:I)"""
e8 = """.=SUMAR.SI.CONJUNTO('Loans File'!AA:AA;'Loans File'!G:G;"CURRENT";'Loans File'!AA:AA;">0")"""
e9 = ''
e10= ''

ecuas = [e1,e2,e3,e4,e5,e6,e7,e8,e9,e10]
# DATAFRAME DE AGREGATE CHECKS
aggregate_checks = pd.DataFrame({
        "Test Metric" : columnas,
        "Value as per <Alt Lender>": valores,
        "Ecu" : ecuas
})
#%% AJUSTE CREDIT SITUATION y CURRENCY
bd_ops = pd.read_excel(r"G:/.shortcut-targets-by-id/103C1ITMg88pYuTOUdrxjoOtU5u15eVkj/Cierre PGH/archivos/BD_Operaciones.xlsx")

bd_ops = bd_ops[['Codigo Prestamo', 'Situación del credito', 'Moneda']]
bd_ops['Moneda'] = bd_ops['Moneda'].replace({'SOLES': 'PEN', 'DOLARES': 'USD'})
bd_ops.columns = ['cod_aux', 'situacion_aux', 'moneda_aux']

loans = loans.merge(bd_ops,
                    left_on = 'loan_id',
                    right_on = 'cod_aux',
                    how = 'left')

loans['credit_situation'] = np.where(loans['credit_situation'].isnull(),
                                     loans['situacion_aux'],
                                     loans['credit_situation'])

loans['credit_situation'] = np.where((loans['credit_situation'].isnull())  &  (loans['loan_id'].str.contains('NORP2P')),
                                     'NOR',
                                     loans['credit_situation'])

loans['credit_situation'] = np.where((loans['credit_situation'].isnull())  &  (loans['loan_id'].str.contains('RENP2P')),
                                     'REN',
                                     loans['credit_situation'])

del loans['situacion_aux']

loans['currency'] = np.where(loans['currency'].isnull(),
                             loans['moneda_aux'],
                             loans['currency'])

del loans['moneda_aux']
del loans['cod_aux']
# Parchamiento puntual
loans_pen = [    'P02082E01679Y00567NORP2P',
                 'P03088E01156Y00663NORP2P',
                 'P02445E01962Y00663NORP2P']
loans_usd = [    'P03063E01697Y00573RENP2P',
                 'P02085E01697Y00573NORP2P']

loans.loc[loans['loan_id'].isin(loans_pen), 'currency'] = 'PEN'
loans.loc[loans['loan_id'].isin(loans_usd), 'currency'] = 'USD'

#%% Recálculo de total_loan_amount de la tabla Loans File
int_agrupados = repayments.pivot_table(index   = 'loan_id',
                                       values  = 'interest_amount',
                                       aggfunc = 'sum').reset_index()
int_agrupados.columns = ['loan_id', 'int agrupado aux']
loans = loans.merge(int_agrupados,
                    on = 'loan_id',
                    how = 'left')
loans['int agrupado aux'] = loans['int agrupado aux'].fillna(0)

loans['total_loan_amount'] = loans['principal_amount'].fillna(0) + loans['int agrupado aux']
del loans['int agrupado aux']

#%%
'''
# Guardar en un mismo Excel con varias hojas
with pd.ExcelWriter(f"{cierre}_Loan Tape Document For Alt Lenders_moises_barrueta.xlsx", 
                    engine="xlsxwriter") as writer:
    loans.to_excel(writer, sheet_name="Loans File", index=False)
    #individuals.to_excel(writer, sheet_name="Hoja2", index=False)
    repayments.to_excel(writer, sheet_name="Repayment Schedules File", index=False)
    payments.to_excel(writer, sheet_name="Payments File", index=False)
'''
#%%% copiar excel de ejemplo
ejemplo_original = r'C:/Users/Joseph Montoya/Desktop/LoanTape_PGH/ejemplo.xlsx'
destino = f"{cierre}_Loan Tape Document For Alt Lenders_moises_barrueta V2.xlsx"

# Copiar y renombrar al mismo tiempo
shutil.copy(ejemplo_original, destino)
print(f"✅ Archivo copiado y renombrado como '{destino}'")

#%% Escribir los dataframes en el excel ya existente
with pd.ExcelWriter(
    destino,
    engine="openpyxl",
    mode="a",                       # append en vez de sobrescribir
    if_sheet_exists="replace"       # o "new" para crear nueva hoja aunque el nombre coincida
) as writer:
    loans.to_excel(writer,       sheet_name="Loans File",               index = False)
    individuals.to_excel(writer, sheet_name="Individual Loan Checks",   index = False)
    repayments.to_excel(writer,  sheet_name="Repayment Schedules File", index = False)
    payments.to_excel(writer,    sheet_name="Payments File",            index = False)
    aggregate_checks.to_excel(writer, sheet_name="agg checks",          index = False)

#%%
print('fin')











