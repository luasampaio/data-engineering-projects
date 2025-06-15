-- Criação das tabelas necessárias para o pipeline

CREATE TABLE IF NOT EXISTS orders_rt (
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    user_id      VARCHAR(32),
    gmv          DOUBLE PRECISION,
    order_cnt    BIGINT,
    PRIMARY KEY (window_start, user_id)
);

CREATE TABLE IF NOT EXISTS orders_daily (
    day        DATE,
    gmv        DOUBLE PRECISION,
    orders     BIGINT
);
