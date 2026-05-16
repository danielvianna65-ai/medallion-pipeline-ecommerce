SELECT
    status_pagamento,
    COUNT(DISTINCT id_pedido) AS total_pedidos
FROM refined.vw_fato_vendas_enriquecida
GROUP BY status_pagamento
ORDER BY total_pedidos DESC;