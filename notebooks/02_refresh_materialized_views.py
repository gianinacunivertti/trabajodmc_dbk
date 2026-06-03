{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "e2a60ca2",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "USE CATALOG fintech_finpay;\n",
    "USE SCHEMA gold;\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "86413cb8",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "REFRESH MATERIALIZED VIEW fintech_finpay.gold.dim_merchant;\n",
    "REFRESH MATERIALIZED VIEW fintech_finpay.gold.dim_user;\n",
    "REFRESH MATERIALIZED VIEW fintech_finpay.gold.dim_channel;\n",
    "REFRESH MATERIALIZED VIEW fintech_finpay.gold.dim_date;"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "b4863b63",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "REFRESH MATERIALIZED VIEW fintech_finpay.gold.fact_transactions;"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "d66df47f",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "SELECT 'dim_merchant' AS object_name, COUNT(*) AS records FROM fintech_finpay.gold.dim_merchant\n",
    "UNION ALL\n",
    "SELECT 'dim_user' AS object_name, COUNT(*) AS records FROM fintech_finpay.gold.dim_user\n",
    "UNION ALL\n",
    "SELECT 'dim_channel' AS object_name, COUNT(*) AS records FROM fintech_finpay.gold.dim_channel\n",
    "UNION ALL\n",
    "SELECT 'dim_date' AS object_name, COUNT(*) AS records FROM fintech_finpay.gold.dim_date\n",
    "UNION ALL\n",
    "SELECT 'fact_transactions' AS object_name, COUNT(*) AS records FROM fintech_finpay.gold.fact_transactions;"
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
