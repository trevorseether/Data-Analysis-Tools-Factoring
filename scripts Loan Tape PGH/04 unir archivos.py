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
from datetime import datetime, timezone, timedelta
peru_tz = timezone(timedelta(hours=-5))
today_date = datetime.now(peru_tz).strftime('%Y%m%d')

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

#%% añadiendo columna en Loans
loans['Importe Solicitado'] = '=+Q2'
loans['FEE']                = '=+AA2'
loans['Importe Principal']  = '=+U2-V2'
loans['Interes']            = '=+AD2'
loans['Ganancia Total']     = '=+U2+X2'

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
                    'Importe Solicitado',
                    'FEE',
                    'Importe Principal',
                    'Interes',
                    'Ganancia Total',
                    
                    'downpayment',
                    'fees',
                    'principal_remaining',
                    'principal_outstanding',
                    'interest_outstanding',
                    'fee_outstanding',
                    'penalty_outstanding',
                    'days_past_due',
                    'collateral_description',
                    'collateral_value',
                    'restructured_id',
                    'renewed_id'
                        ]
loans = loans[columnas_loans]

#%%
# Guardar en un mismo Excel con varias hojas
with pd.ExcelWriter(f"{cierre}_Loan Tape Document For Alt Lenders_moises_barrueta.xlsx", 
                    engine="xlsxwriter") as writer:
    loans.to_excel(writer, sheet_name="Loans File", index=False)
    #individuals.to_excel(writer, sheet_name="Hoja2", index=False)
    repayments.to_excel(writer, sheet_name="Repayment Schedules File", index=False)
    payments.to_excel(writer, sheet_name="Payments File", index=False)


