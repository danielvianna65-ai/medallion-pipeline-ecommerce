SELECT
    dt_pedido,
    forma_pagamento,
    status_pagamento,
    SUM(valor_total_item) AS receita_pagamentos
FROM refined.vw_fato_vendas_enriquecida
GROUP BY
    dt_pedido,
    forma_pagamento,
    status_pagamento
ORDER BY dt_pedido;