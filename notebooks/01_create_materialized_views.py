{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "69939da4",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "USE CATALOG fintech_finpay;\n",
    "USE SCHEMA gold;"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "47910177",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "CREATE OR REPLACE MATERIALIZED VIEW fintech_finpay.gold.dim_merchant\n",
    "COMMENT 'Dimension de comercios afiliados FinPay'\n",
    "AS\n",
    "SELECT\n",
    "    merchant_id,\n",
    "    merchant_name,\n",
    "    category,\n",
    "    country,\n",
    "    affiliation_date,\n",
    "    status AS merchant_status,\n",
    "    risk_level,\n",
    "    current_timestamp() AS mv_created_at\n",
    "FROM fintech_finpay.silver.silver_merchants\n",
    "WHERE merchant_id IS NOT NULL;"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "6da0d220",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "CREATE OR REPLACE MATERIALIZED VIEW fintech_finpay.gold.dim_user\n",
    "COMMENT 'Dimension de usuarios FinPay con campos PII protegidos por masking en silver'\n",
    "AS\n",
    "SELECT\n",
    "    u.user_id,\n",
    "    u.full_name,\n",
    "    u.document_id,\n",
    "    u.email,\n",
    "    u.phone,\n",
    "    u.country,\n",
    "    u.segment,\n",
    "    u.registration_date,\n",
    "    COALESCE(p.preferred_channel, 'sin_canal') AS preferred_channel,\n",
    "    current_timestamp() AS mv_created_at\n",
    "FROM fintech_finpay.silver.silver_users u\n",
    "LEFT JOIN (\n",
    "    SELECT\n",
    "        user_id,\n",
    "        channel AS preferred_channel\n",
    "    FROM (\n",
    "        SELECT\n",
    "            user_id,\n",
    "            channel,\n",
    "            COUNT(*) AS total_transactions,\n",
    "            ROW_NUMBER() OVER (\n",
    "                PARTITION BY user_id\n",
    "                ORDER BY COUNT(*) DESC\n",
    "            ) AS rn\n",
    "        FROM fintech_finpay.silver.silver_transactions\n",
    "        WHERE user_id IS NOT NULL\n",
    "          AND channel IS NOT NULL\n",
    "        GROUP BY user_id, channel\n",
    "    )\n",
    "    WHERE rn = 1\n",
    ") p\n",
    "ON u.user_id = p.user_id\n",
    "WHERE u.user_id IS NOT NULL;"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "a9290ed7",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "CREATE OR REPLACE MATERIALIZED VIEW fintech_finpay.gold.dim_channel\n",
    "COMMENT 'Dimension de canales transaccionales'\n",
    "AS\n",
    "SELECT\n",
    "    channel,\n",
    "    CASE\n",
    "        WHEN channel = 'web' THEN 'Canal Web'\n",
    "        WHEN channel = 'app' THEN 'Aplicacion movil'\n",
    "        WHEN channel = 'pos' THEN 'POS fisico'\n",
    "        ELSE 'Canal desconocido'\n",
    "    END AS channel_description,\n",
    "    current_timestamp() AS mv_created_at\n",
    "FROM (\n",
    "    SELECT DISTINCT channel\n",
    "    FROM fintech_finpay.silver.silver_transactions\n",
    "    WHERE channel IS NOT NULL\n",
    ");"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "fcc699e7",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "CREATE OR REPLACE MATERIALIZED VIEW fintech_finpay.gold.dim_channel\n",
    "COMMENT 'Dimension de canales transaccionales'\n",
    "AS\n",
    "SELECT\n",
    "    channel,\n",
    "    CASE\n",
    "        WHEN channel = 'web' THEN 'Canal Web'\n",
    "        WHEN channel = 'app' THEN 'Aplicacion movil'\n",
    "        WHEN channel = 'pos' THEN 'POS fisico'\n",
    "        ELSE 'Canal desconocido'\n",
    "    END AS channel_description,\n",
    "    current_timestamp() AS mv_created_at\n",
    "FROM (\n",
    "    SELECT DISTINCT channel\n",
    "    FROM fintech_finpay.silver.silver_transactions\n",
    "    WHERE channel IS NOT NULL\n",
    ");"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "03df2328",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "CREATE OR REPLACE MATERIALIZED VIEW fintech_finpay.gold.dim_date\n",
    "COMMENT 'Dimension calendario derivada de las fechas transaccionales'\n",
    "AS\n",
    "SELECT\n",
    "    transaction_date AS date_id,\n",
    "    YEAR(transaction_date) AS year,\n",
    "    QUARTER(transaction_date) AS quarter,\n",
    "    MONTH(transaction_date) AS month,\n",
    "    DATE_FORMAT(transaction_date, 'MMMM') AS month_name,\n",
    "    WEEKOFYEAR(transaction_date) AS week_of_year,\n",
    "    DAY(transaction_date) AS day,\n",
    "    DAYOFWEEK(transaction_date) AS day_of_week,\n",
    "    DATE_FORMAT(transaction_date, 'EEEE') AS day_name,\n",
    "    current_timestamp() AS mv_created_at\n",
    "FROM (\n",
    "    SELECT DISTINCT transaction_date\n",
    "    FROM fintech_finpay.silver.silver_transactions\n",
    "    WHERE transaction_date IS NOT NULL\n",
    ");"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "ed1ffc11",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "CREATE OR REPLACE MATERIALIZED VIEW fintech_finpay.gold.fact_transactions\n",
    "COMMENT 'Tabla de hechos transaccional con metricas de riesgo'\n",
    "AS\n",
    "SELECT\n",
    "    t.transaction_date AS date_id,\n",
    "    t.merchant_id,\n",
    "    t.user_id,\n",
    "    t.channel,\n",
    "    t.currency,\n",
    "\n",
    "    COUNT(*) AS transaction_count,\n",
    "\n",
    "    SUM(CASE WHEN t.transaction_type = 'pago' THEN 1 ELSE 0 END) AS payment_count,\n",
    "    SUM(CASE WHEN t.transaction_type = 'reversa' THEN 1 ELSE 0 END) AS reversal_count,\n",
    "    SUM(CASE WHEN t.transaction_type = 'retiro' THEN 1 ELSE 0 END) AS withdrawal_count,\n",
    "\n",
    "    SUM(CASE WHEN t.status = 'aprobado' THEN 1 ELSE 0 END) AS approved_count,\n",
    "    SUM(CASE WHEN t.status = 'rechazado' THEN 1 ELSE 0 END) AS rejected_count,\n",
    "    SUM(CASE WHEN t.status = 'pendiente' THEN 1 ELSE 0 END) AS pending_count,\n",
    "\n",
    "    ROUND(SUM(t.amount_value), 2) AS total_amount,\n",
    "    ROUND(AVG(t.amount_value), 2) AS avg_amount,\n",
    "\n",
    "    ROUND(\n",
    "        SUM(CASE WHEN t.transaction_type = 'reversa' THEN 1 ELSE 0 END)\n",
    "        / NULLIF(COUNT(*), 0),\n",
    "        4\n",
    "    ) AS reversal_rate,\n",
    "\n",
    "    ROUND(\n",
    "        SUM(CASE WHEN t.status = 'rechazado' THEN 1 ELSE 0 END)\n",
    "        / NULLIF(COUNT(*), 0),\n",
    "        4\n",
    "    ) AS rejected_rate,\n",
    "\n",
    "    ROUND(\n",
    "        (\n",
    "            0.50 * (\n",
    "                SUM(CASE WHEN t.transaction_type = 'reversa' THEN 1 ELSE 0 END)\n",
    "                / NULLIF(COUNT(*), 0)\n",
    "            )\n",
    "            +\n",
    "            0.30 * (\n",
    "                SUM(CASE WHEN t.status = 'rechazado' THEN 1 ELSE 0 END)\n",
    "                / NULLIF(COUNT(*), 0)\n",
    "            )\n",
    "            +\n",
    "            0.20 * CASE\n",
    "                WHEN AVG(t.amount_value) >= 1000 THEN 1\n",
    "                WHEN AVG(t.amount_value) >= 500 THEN 0.5\n",
    "                ELSE 0\n",
    "            END\n",
    "        ),\n",
    "        4\n",
    "    ) AS risk_score,\n",
    "\n",
    "    current_timestamp() AS mv_created_at\n",
    "\n",
    "FROM fintech_finpay.silver.silver_transactions t\n",
    "WHERE t.transaction_id IS NOT NULL\n",
    "GROUP BY\n",
    "    t.transaction_date,\n",
    "    t.merchant_id,\n",
    "    t.user_id,\n",
    "    t.channel,\n",
    "    t.currency;"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "42720c62",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "\n",
    "CREATE OR REPLACE MATERIALIZED VIEW fintech_finpay.gold.mv_risk_by_merchant_channel\n",
    "COMMENT 'Agregado analitico de riesgo por comercio y canal'\n",
    "AS\n",
    "SELECT\n",
    "    f.merchant_id,\n",
    "    m.merchant_name,\n",
    "    m.category,\n",
    "    m.country,\n",
    "    f.channel,\n",
    "\n",
    "    SUM(f.transaction_count) AS transaction_count,\n",
    "    SUM(f.payment_count) AS payment_count,\n",
    "    SUM(f.reversal_count) AS reversal_count,\n",
    "    SUM(f.withdrawal_count) AS withdrawal_count,\n",
    "    SUM(f.approved_count) AS approved_count,\n",
    "    SUM(f.rejected_count) AS rejected_count,\n",
    "\n",
    "    ROUND(SUM(f.total_amount), 2) AS total_amount,\n",
    "    ROUND(AVG(f.avg_amount), 2) AS avg_amount,\n",
    "\n",
    "    ROUND(\n",
    "        SUM(f.reversal_count) / NULLIF(SUM(f.transaction_count), 0),\n",
    "        4\n",
    "    ) AS reversal_rate,\n",
    "\n",
    "    ROUND(\n",
    "        SUM(f.rejected_count) / NULLIF(SUM(f.transaction_count), 0),\n",
    "        4\n",
    "    ) AS rejected_rate,\n",
    "\n",
    "    ROUND(AVG(f.risk_score), 4) AS avg_risk_score,\n",
    "\n",
    "    CASE\n",
    "        WHEN AVG(f.risk_score) >= 0.70 THEN 'alto'\n",
    "        WHEN AVG(f.risk_score) >= 0.40 THEN 'medio'\n",
    "        ELSE 'bajo'\n",
    "    END AS calculated_risk_level,\n",
    "\n",
    "    current_timestamp() AS mv_created_at\n",
    "\n",
    "FROM fintech_finpay.gold.fact_transactions f\n",
    "LEFT JOIN fintech_finpay.gold.dim_merchant m\n",
    "    ON f.merchant_id = m.merchant_id\n",
    "GROUP BY\n",
    "    f.merchant_id,\n",
    "    m.merchant_name,\n",
    "    m.category,\n",
    "    m.country,\n",
    "    f.channel;"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "4e1f5d4d",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "CREATE OR REPLACE MATERIALIZED VIEW fintech_finpay.gold.mv_daily_reversal_rate\n",
    "COMMENT 'Tasa diaria de reversas por pais, canal y moneda'\n",
    "AS\n",
    "SELECT\n",
    "    f.date_id,\n",
    "    m.country,\n",
    "    f.channel,\n",
    "    f.currency,\n",
    "\n",
    "    SUM(f.transaction_count) AS transaction_count,\n",
    "    SUM(f.reversal_count) AS reversal_count,\n",
    "    ROUND(SUM(f.total_amount), 2) AS total_amount,\n",
    "\n",
    "    ROUND(\n",
    "        SUM(f.reversal_count) / NULLIF(SUM(f.transaction_count), 0),\n",
    "        4\n",
    "    ) AS reversal_rate,\n",
    "\n",
    "    current_timestamp() AS mv_created_at\n",
    "\n",
    "FROM fintech_finpay.gold.fact_transactions f\n",
    "LEFT JOIN fintech_finpay.gold.dim_merchant m\n",
    "    ON f.merchant_id = m.merchant_id\n",
    "GROUP BY\n",
    "    f.date_id,\n",
    "    m.country,\n",
    "    f.channel,\n",
    "    f.currency;"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "061124ac",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "\n",
    "SELECT\n",
    "    'dim_merchant' AS object_name,\n",
    "    COUNT(*) AS row_count\n",
    "FROM fintech_finpay.gold.dim_merchant\n",
    "\n",
    "UNION ALL\n",
    "\n",
    "SELECT\n",
    "    'dim_user' AS object_name,\n",
    "    COUNT(*) AS row_count\n",
    "FROM fintech_finpay.gold.dim_user\n",
    "\n",
    "UNION ALL\n",
    "\n",
    "SELECT\n",
    "    'dim_channel' AS object_name,\n",
    "    COUNT(*) AS row_count\n",
    "FROM fintech_finpay.gold.dim_channel\n",
    "\n",
    "UNION ALL\n",
    "\n",
    "SELECT\n",
    "    'dim_date' AS object_name,\n",
    "    COUNT(*) AS row_count\n",
    "FROM fintech_finpay.gold.dim_date\n",
    "\n",
    "UNION ALL\n",
    "\n",
    "SELECT\n",
    "    'fact_transactions' AS object_name,\n",
    "    COUNT(*) AS row_count\n",
    "FROM fintech_finpay.gold.fact_transactions\n",
    "\n",
    "UNION ALL\n",
    "\n",
    "SELECT\n",
    "    'mv_risk_by_merchant_channel' AS object_name,\n",
    "    COUNT(*) AS row_count\n",
    "FROM fintech_finpay.gold.mv_risk_by_merchant_channel\n",
    "\n",
    "UNION ALL\n",
    "\n",
    "SELECT\n",
    "    'mv_daily_reversal_rate' AS object_name,\n",
    "    COUNT(*) AS row_count\n",
    "FROM fintech_finpay.gold.mv_daily_reversal_rate;"
   ]
  }
 ],
 "metadata": {
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
