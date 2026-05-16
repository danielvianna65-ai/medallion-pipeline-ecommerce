SELECT
    AVG(total_pedido) AS ticket_medio
FROM (
    SELECT
        id_pedido,
        SUM(valor_total_item) AS total_pedido
    FROM refined.vw_fato_vendas_enriquecida
    WHERE status_pagamento = 'confirmado'
    GROUP BY id_pedido
) pedidos;