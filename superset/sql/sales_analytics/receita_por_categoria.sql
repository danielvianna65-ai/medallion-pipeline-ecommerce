SELECT
    nome_categoria,
    SUM(valor_total_item) AS receita_categoria
FROM refined.vw_fato_vendas_enriquecida
WHERE status_pagamento = 'confirmado'
GROUP BY nome_categoria
ORDER BY receita_categoria DESC
LIMIT 10;