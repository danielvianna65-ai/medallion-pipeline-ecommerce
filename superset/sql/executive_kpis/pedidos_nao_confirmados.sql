SELECT
    COUNT(DISTINCT id_pedido) AS pedidos_nao_confirmados
FROM refined.vw_fato_vendas_enriquecida
WHERE status_pagamento <> 'confirmado';