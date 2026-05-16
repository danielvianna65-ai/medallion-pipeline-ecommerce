SELECT
    ano_mes,
    SUM(valor_total_item) AS receita_mensal
FROM refined.vw_fato_vendas_enriquecida
WHERE status_pagamento = 'confirmado'
GROUP BY ano_mes
ORDER BY ano_mes;