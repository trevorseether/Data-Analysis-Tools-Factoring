SELECT 
    fr.code,
    array_join(array_agg(ii.code), ', ') AS facturas,
    'online' as flag    --max(fr.product) as prod
FROM prod_datalake_analytics.im_invoices AS ii
LEFT JOIN prod_datalake_analytics.fac_requests AS fr
    ON fr._id = ii.request_id
WHERE ii.request_id IS NOT NULL
and fr.code not in ('rejected', 'canceled')

  AND fr.code = 'EY7GLR0t'
GROUP BY fr.code