select 
    a.code,
    b.code,
    b.created_at

from
prod_datalake_analytics.fac_requests a
left join (select * from prod_datalake_analytics.view_prestamype_fac_cpe where concept='commission-factoring') b
on a._id=b.request_id

where a.status not in  ('rejected', 'canceled')
and b.code is not null
