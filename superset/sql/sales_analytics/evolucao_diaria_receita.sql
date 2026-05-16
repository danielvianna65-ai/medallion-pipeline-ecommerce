SELECT
    dt_pedido,
    SUM(valor_total_item) AS receita_diaria
FROM refined.vw_fato_vendas_enriquecida
WHERE status_pagamento = 'confirmado'
GROUP BY dt_pedido
ORDER BY dt_pedido;