SELECT
    SUM(valor_total_item) AS receita_total
FROM refined.vw_fato_vendas_enriquecida
WHERE status_pagamento = 'confirmado';