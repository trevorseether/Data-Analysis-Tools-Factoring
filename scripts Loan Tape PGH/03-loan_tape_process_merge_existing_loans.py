# -*- coding: utf-8 -*-
"""
Created on Tue Sep  2 15:53:16 2025

@author: Joseph Montoya
"""

# =============================================================================
# LOAN TAPE PGH - PASO FINAL
# =============================================================================
import pandas as pd
from datetime import datetime, timezone

from datetime import datetime, timezone, timedelta
peru_tz = timezone(timedelta(hours=-5))
today_date = datetime.now(peru_tz).strftime('%Y%m%d')

import os
os.chdir(r'C:\Users\Joseph Montoya\Desktop\LoanTape_PGH\temp\202512 existing')

#%%
payments_file_last_cierre = pd.read_excel(f'./payments_{today_date}_current_loans.xlsx')
print(payments_file_last_cierre.shape)
payments_file_last_cierre.head(2)
#%%
toupdate_payments_file = pd.read_excel(r'C:/Users/Joseph Montoya/Desktop/LoanTape_PGH/202412_Loan Tape Document For Alt Lenders V3.xlsx', 
                                       sheet_name='Payments File')
print(toupdate_payments_file.shape)
toupdate_payments_file.head(2)
#%%
toupdate_payments_file = toupdate_payments_file[~(toupdate_payments_file['loan_id'].isna())]
# toupdate_payments_file = toupdate_payments_file.iloc[:41415,:]
#%%
toupdate_payments_file_temp = toupdate_payments_file[~(toupdate_payments_file['loan_id'].isin(payments_file_last_cierre['loan_id'].unique()))]
toupdate_payments_file_to_last_cierre = pd.concat([toupdate_payments_file_temp, payments_file_last_cierre], axis=0)
print(toupdate_payments_file_to_last_cierre.shape)
#%%
toupdate_payments_file_to_last_cierre.to_excel(f'./payments_{today_date}_updated.xlsx', index=False)

#%%
# =============================================================================
# REPAYMENT SCHEDULE
# =============================================================================
#%%
repayments_file_last_cierre = pd.read_excel(f'./repayments_{today_date}_current_loans.xlsx')
print(repayments_file_last_cierre.shape)
repayments_file_last_cierre.head(2)
#%%
toupdate_repayments_file = pd.read_excel(r'C:/Users/Joseph Montoya/Desktop/LoanTape_PGH/202412_Loan Tape Document For Alt Lenders V3.xlsx', 
                                         sheet_name='Repayment Schedules File')
print(toupdate_repayments_file.shape)
toupdate_repayments_file.head(2)

#%%
toupdate_repayments_file = toupdate_repayments_file[~(toupdate_repayments_file['loan_id'].isna())]
print(toupdate_repayments_file.shape)
#%%
## Compare loan_ids schedule

temp_repayment_1 = repayments_file_last_cierre['loan_id'].value_counts().reset_index()
temp_repayment_2 = toupdate_repayments_file['loan_id'].value_counts().reset_index() 

check_repayments = temp_repayment_1.merge(temp_repayment_2, on='loan_id', how='inner', suffixes=('', '_2'))
# check_repayments.head(5)
check_repayments[check_repayments['count'] != check_repayments['count_2']]


#%%
toupdate_repayments_file_temp = toupdate_repayments_file[~(toupdate_repayments_file['loan_id'].isin(repayments_file_last_cierre['loan_id'].unique()))]
toupdate_repayments_file_to_last_cierre = pd.concat([toupdate_repayments_file_temp, repayments_file_last_cierre], axis=0)
print(toupdate_repayments_file_to_last_cierre.shape)
#%%
toupdate_repayments_file_to_last_cierre.to_excel(f'./repayments_{today_date}_updated.xlsx', index=False)
#%%
# =============================================================================
# INDIVIDUALS
# =============================================================================
#%%
individual_file_last_cierre = pd.read_excel(f'./individual_{today_date}_current_loans.xlsx')
print(individual_file_last_cierre.shape)
individual_file_last_cierre.head(2)
#%%
toupdate_individual_file = pd.read_excel(r'C:/Users/Joseph Montoya/Desktop/LoanTape_PGH/202412_Loan Tape Document For Alt Lenders V3.xlsx', 
                                         sheet_name='Individual Loan Checks')
print(toupdate_individual_file.shape)
toupdate_individual_file.head(2)
#%%
toupdate_individual_file_temp = toupdate_individual_file[~(toupdate_individual_file['loan_id'].isin(individual_file_last_cierre['loan_id'].unique()))]
toupdate_individual_file_to_last_cierre = pd.concat([toupdate_individual_file_temp, individual_file_last_cierre], axis=0)
print(toupdate_individual_file_to_last_cierre.shape)
#%%
toupdate_individual_file_to_last_cierre[toupdate_individual_file_to_last_cierre['loan_id'].isna()]
#%%
toupdate_individual_file_to_last_cierre.to_excel(f'./individual_{today_date}_updated.xlsx', index=False)
#%%
# =============================================================================
# LOAN FILE
# =============================================================================
#%%
loanfile_file_last_cierre = pd.read_excel(f'./loans_file_{today_date}_current_loans.xlsx')
print(loanfile_file_last_cierre.shape)
loanfile_file_last_cierre.head(2)
#%%
toupdate_loanfile_file = pd.read_excel(r'C:/Users/Joseph Montoya/Desktop/LoanTape_PGH/202412_Loan Tape Document For Alt Lenders V3.xlsx', sheet_name='Loans File')
del toupdate_loanfile_file['DE REPAYMENT']
del toupdate_loanfile_file['total_loan_amount.1']
del toupdate_loanfile_file['check']
print(toupdate_loanfile_file.shape)
toupdate_loanfile_file.head(2)
#%%
toupdate_loanfile_file_temp = toupdate_loanfile_file[~(toupdate_loanfile_file['loan_id'].isin(loanfile_file_last_cierre['loan_id'].unique()))]
toupdate_loanfile_file_to_last_cierre = pd.concat([toupdate_loanfile_file_temp, loanfile_file_last_cierre], axis=0)
print(toupdate_loanfile_file_to_last_cierre.shape)
#%%
toupdate_loanfile_file_to_last_cierre.to_excel(f'./loans_file_{today_date}_updated.xlsx', index=False)
#%%

#%%

#%%

#%%

#%%


