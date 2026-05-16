SELECT
    nome_cliente,
    SUM(valor_total_item) AS receita_cliente
FROM refined.vw_fato_vendas_enriquecida
WHERE status_pagamento = 'confirmado'
GROUP BY nome_cliente
ORDER BY receita_cliente DESC
LIMIT 10;