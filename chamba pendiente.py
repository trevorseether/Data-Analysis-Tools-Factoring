# -*- coding: utf-8 -*-
"""
Created on Fri Sep 19 18:01:39 2025

@author: Joseph Montoya
"""

chamba pendiente, detectar operaciones hijas cuya factura debe ser anulada



select 
    fr.proforma_simulation_warranty_zero_status, 
    fr.code,
    cpe.code,
    fr.interest_proforma_disbursement_date,
    fr.interest_proforma_simulation_financing_cost_value,
    *

from prod_datalake_analytics.fac_requests as fr
left join (select * from prod_datalake_analytics."view_prestamype_fac_cpe" where concept = 'interest-factoring') as cpe
on fr._id = cpe.request_id

where proforma_simulation_warranty_zero_status = True
and cpe.code is not null




select * 
from prod_datalake_analytics."view_prestamype_fac_cpe"

where concept = 'interest-factoring'
limit 50
 
-------- descartar operaciones normales (no madres ni hijas)
-------- añadir factura, y descargar operaciones hijas, porque son 
--- preguntar a Migceli : df_items['Descripción del item'] = 'Ajuste al descuento por operación de Factoring en referencia el Contrato Empresario.'