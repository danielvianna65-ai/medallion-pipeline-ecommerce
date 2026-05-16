SELECT
    nome_produto,
    SUM(valor_total_item) AS receita_produto
FROM refined.vw_fato_vendas_enriquecida
WHERE status_pagamento = 'confirmado'
GROUP BY nome_produto
ORDER BY receita_produto DESC
LIMIT 10;