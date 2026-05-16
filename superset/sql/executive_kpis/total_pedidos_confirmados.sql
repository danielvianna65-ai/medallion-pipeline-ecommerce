SELECT
    COUNT(DISTINCT id_pedido) AS total_pedidos
FROM refined.vw_fato_vendas_enriquecida
WHERE status_pagamento = 'confirmado';