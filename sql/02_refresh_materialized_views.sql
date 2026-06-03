USE CATALOG fintech_finpay;
USE SCHEMA gold;

REFRESH MATERIALIZED VIEW fintech_finpay_prod.gold.dim_merchant;

REFRESH MATERIALIZED VIEW fintech_finpay_prod.gold.dim_user;

REFRESH MATERIALIZED VIEW fintech_finpay_prod.gold.dim_channel;

REFRESH MATERIALIZED VIEW fintech_finpay_prod.gold.dim_date;

REFRESH MATERIALIZED VIEW fintech_finpay_prod.gold.fact_transactions;

REFRESH MATERIALIZED VIEW fintech_finpay_prod.gold.mv_risk_by_merchant_channel;

REFRESH MATERIALIZED VIEW fintech_finpay_prod.gold.mv_daily_reversal_rate;


SELECT
    current_timestamp() AS refresh_timestamp,
    'dim_merchant' AS object_name,
    COUNT(*) AS row_count
FROM fintech_finpay_prod.gold.dim_merchant

UNION ALL

SELECT
    current_timestamp() AS refresh_timestamp,
    'dim_user' AS object_name,
    COUNT(*) AS row_count
FROM fintech_finpay_prod.gold.dim_user

UNION ALL

SELECT
    current_timestamp() AS refresh_timestamp,
    'dim_channel' AS object_name,
    COUNT(*) AS row_count
FROM fintech_finpay_prod.gold.dim_channel

UNION ALL

SELECT
    current_timestamp() AS refresh_timestamp,
    'dim_date' AS object_name,
    COUNT(*) AS row_count
FROM fintech_finpay_prod.gold.dim_date

UNION ALL

SELECT
    current_timestamp() AS refresh_timestamp,
    'fact_transactions' AS object_name,
    COUNT(*) AS row_count
FROM fintech_finpay_prod.gold.fact_transactions

UNION ALL

SELECT
    current_timestamp() AS refresh_timestamp,
    'mv_risk_by_merchant_channel' AS object_name,
    COUNT(*) AS row_count
FROM fintech_finpay_prod.gold.mv_risk_by_merchant_channel

UNION ALL

SELECT
    current_timestamp() AS refresh_timestamp,
    'mv_daily_reversal_rate' AS object_name,
    COUNT(*) AS row_count
FROM fintech_finpay_prod.gold.mv_daily_reversal_rate;