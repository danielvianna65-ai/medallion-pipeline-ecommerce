SELECT
    nome_dia,
    SUM(valor_total_item) AS receita_dia_semana
FROM refined.vw_fato_vendas_enriquecida
WHERE status_pagamento = 'confirmado'
GROUP BY nome_dia
ORDER BY receita_dia_semana DESC;