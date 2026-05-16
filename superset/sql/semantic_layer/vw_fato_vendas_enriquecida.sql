CREATE OR REPLACE VIEW refined.vw_fato_vendas_enriquecida AS

SELECT
    -- pedido
    f.id_pedido,
    f.id_item_pedido,
    f.dt_pedido,

    -- cliente
    c.id_cliente,
    c.nome AS nome_cliente,

    -- produto
    p.id_produto,
    p.nome_produto,
    p.nome_categoria,

    -- métricas
    f.quantidade,
    f.preco_unitario,
    f.valor_total_item,

    -- tempo
    d.data,
    d.ano,
    d.mes,
    d.dia,
    d.ano_mes,
    d.dia_semana,
    d.trimestre,
    d.semana_ano,
    d.fim_de_semana,
    d.nome_dia,
    d.dia_util,

    -- pagamento
    pg.forma_pagamento,
    pg.status_pagamento,
    pg.valor_pago,
    pg.data_transacao AS data_pagamento

FROM refined.fato_vendas f

LEFT JOIN refined.dim_cliente c
    ON f.sk_cliente = c.sk_cliente

LEFT JOIN refined.dim_produto p
    ON f.sk_produto = p.sk_produto

LEFT JOIN refined.dim_data d
    ON f.sk_data_pedido = d.sk_data

LEFT JOIN refined.dim_pagamento pg
    ON f.sk_pagamento = pg.sk_pagamento;