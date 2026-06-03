USE CATALOG fintech_finpay;
USE SCHEMA gold;

CREATE OR REPLACE MATERIALIZED VIEW fintech_finpay_prod.gold.dim_merchant
COMMENT 'Dimension de comercios afiliados FinPay'
AS
SELECT
    merchant_id,
    merchant_name,
    category,
    country,
    affiliation_date,
    status AS merchant_status,
    risk_level,
    current_timestamp() AS mv_created_at
FROM fintech_finpay_prod.silver.merchants
WHERE merchant_id IS NOT NULL;


CREATE OR REPLACE MATERIALIZED VIEW fintech_finpay_prod.gold.dim_user
COMMENT 'Dimension de usuarios FinPay con campos PII'
AS
SELECT
    u.user_id,
    u.full_name,
    u.document_id,
    u.email,
    u.phone,
    u.country,
    u.segment,
    u.registration_date,
    COALESCE(p.preferred_channel, 'sin_canal') AS preferred_channel,
    current_timestamp() AS mv_created_at
FROM fintech_finpay_prod.silver.users u
LEFT JOIN (
    SELECT
        user_id,
        channel AS preferred_channel
    FROM (
        SELECT
            user_id,
            channel,
            COUNT(*) AS total_transactions,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY COUNT(*) DESC
            ) AS rn
        FROM fintech_finpay_prod.silver.transactions
        WHERE user_id IS NOT NULL
          AND channel IS NOT NULL
        GROUP BY user_id, channel
    )
    WHERE rn = 1
) p
ON u.user_id = p.user_id
WHERE u.user_id IS NOT NULL;


CREATE OR REPLACE MATERIALIZED VIEW fintech_finpay_prod.gold.dim_channel
COMMENT 'Dimension de canales transaccionales'
AS
SELECT
    channel,
    CASE
        WHEN channel = 'web' THEN 'Canal Web'
        WHEN channel = 'app' THEN 'Aplicacion movil'
        WHEN channel = 'pos' THEN 'POS fisico'
        ELSE 'Canal desconocido'
    END AS channel_description,
    current_timestamp() AS mv_created_at
FROM (
    SELECT DISTINCT channel
    FROM fintech_finpay_prod.silver.transactions
    WHERE channel IS NOT NULL
);


CREATE OR REPLACE MATERIALIZED VIEW fintech_finpay_prod.gold.dim_date
COMMENT 'Dimension calendario derivada de fechas transaccionales'
AS
SELECT
    transaction_date AS date_id,
    YEAR(transaction_date) AS year,
    QUARTER(transaction_date) AS quarter,
    MONTH(transaction_date) AS month,
    DATE_FORMAT(transaction_date, 'MMMM') AS month_name,
    WEEKOFYEAR(transaction_date) AS week_of_year,
    DAY(transaction_date) AS day,
    DAYOFWEEK(transaction_date) AS day_of_week,
    DATE_FORMAT(transaction_date, 'EEEE') AS day_name,
    current_timestamp() AS mv_created_at
FROM (
    SELECT DISTINCT transaction_date
    FROM fintech_finpay_prod.silver.transactions
    WHERE transaction_date IS NOT NULL
);


CREATE OR REPLACE MATERIALIZED VIEW fintech_finpay_prod.gold.fact_transactions
COMMENT 'Tabla de hechos transaccional con metricas de riesgo'
AS
SELECT
    t.transaction_date AS date_id,
    t.merchant_id,
    t.user_id,
    t.channel,
    t.currency,

    COUNT(*) AS transaction_count,

    SUM(CASE WHEN t.transaction_type = 'pago' THEN 1 ELSE 0 END) AS payment_count,
    SUM(CASE WHEN t.transaction_type = 'reversa' THEN 1 ELSE 0 END) AS reversal_count,
    SUM(CASE WHEN t.transaction_type = 'retiro' THEN 1 ELSE 0 END) AS withdrawal_count,

    SUM(CASE WHEN t.status = 'aprobado' THEN 1 ELSE 0 END) AS approved_count,
    SUM(CASE WHEN t.status = 'rechazado' THEN 1 ELSE 0 END) AS rejected_count,
    SUM(CASE WHEN t.status = 'pendiente' THEN 1 ELSE 0 END) AS pending_count,

    ROUND(SUM(t.amount), 2) AS total_amount,
    ROUND(AVG(t.amount), 2) AS avg_amount,

    ROUND(
        SUM(CASE WHEN t.transaction_type = 'reversa' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        4
    ) AS reversal_rate,

    ROUND(
        SUM(CASE WHEN t.status = 'rechazado' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        4
    ) AS rejected_rate,

    ROUND(
        (
            0.50 * (
                SUM(CASE WHEN t.transaction_type = 'reversa' THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0)
            )
            +
            0.30 * (
                SUM(CASE WHEN t.status = 'rechazado' THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0)
            )
            +
            0.20 * CASE
                WHEN AVG(t.amount) >= 1000 THEN 1
                WHEN AVG(t.amount) >= 500 THEN 0.5
                ELSE 0
            END
        ),
        4
    ) AS risk_score,

    current_timestamp() AS mv_created_at

FROM fintech_finpay_prod.silver.transactions t
WHERE t.transaction_id IS NOT NULL
GROUP BY
    t.transaction_date,
    t.merchant_id,
    t.user_id,
    t.channel,
    t.currency;


CREATE OR REPLACE MATERIALIZED VIEW fintech_finpay_prod.gold.mv_risk_by_merchant_channel
COMMENT 'Agregado analitico de riesgo por comercio y canal'
AS
SELECT
    f.merchant_id,
    m.merchant_name,
    m.category,
    m.country,
    f.channel,

    SUM(f.transaction_count) AS transaction_count,
    SUM(f.payment_count) AS payment_count,
    SUM(f.reversal_count) AS reversal_count,
    SUM(f.withdrawal_count) AS withdrawal_count,
    SUM(f.approved_count) AS approved_count,
    SUM(f.rejected_count) AS rejected_count,

    ROUND(SUM(f.total_amount), 2) AS total_amount,
    ROUND(AVG(f.avg_amount), 2) AS avg_amount,

    ROUND(
        SUM(f.reversal_count) / NULLIF(SUM(f.transaction_count), 0),
        4
    ) AS reversal_rate,

    ROUND(
        SUM(f.rejected_count) / NULLIF(SUM(f.transaction_count), 0),
        4
    ) AS rejected_rate,

    ROUND(AVG(f.risk_score), 4) AS avg_risk_score,

    CASE
        WHEN AVG(f.risk_score) >= 0.70 THEN 'alto'
        WHEN AVG(f.risk_score) >= 0.40 THEN 'medio'
        ELSE 'bajo'
    END AS calculated_risk_level,

    current_timestamp() AS mv_created_at

FROM fintech_finpay_prod.gold.fact_transactions f
LEFT JOIN fintech_finpay_prod.gold.dim_merchant m
    ON f.merchant_id = m.merchant_id
GROUP BY
    f.merchant_id,
    m.merchant_name,
    m.category,
    m.country,
    f.channel;


CREATE OR REPLACE MATERIALIZED VIEW fintech_finpay_prod.gold.mv_daily_reversal_rate
COMMENT 'Tasa diaria de reversas por pais, canal y moneda'
AS
SELECT
    f.date_id,
    m.country,
    f.channel,
    f.currency,

    SUM(f.transaction_count) AS transaction_count,
    SUM(f.reversal_count) AS reversal_count,
    ROUND(SUM(f.total_amount), 2) AS total_amount,

    ROUND(
        SUM(f.reversal_count) / NULLIF(SUM(f.transaction_count), 0),
        4
    ) AS reversal_rate,

    current_timestamp() AS mv_created_at

FROM fintech_finpay_prod.gold.fact_transactions f
LEFT JOIN fintech_finpay_prod.gold.dim_merchant m
    ON f.merchant_id = m.merchant_id
GROUP BY
    f.date_id,
    m.country,
    f.channel,
    f.currency;