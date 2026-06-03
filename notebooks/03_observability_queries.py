{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "2b4fa3f7",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "97b2a428",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "CREATE SCHEMA IF NOT EXISTS fintech_finpay.observability;"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "f2a497ba",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "%sql\n",
    "\n",
    "-- Esta celda se activa despues de desplegar y ejecutar el Lakeflow Pipeline.\n",
    "-- Reemplazar PIPELINE_ID_REAL por el ID real del pipeline.\n",
    "\n",
    "-- CREATE OR REPLACE TABLE fintech_finpay.observability.pipeline_event_log AS\n",
    "-- SELECT *\n",
    "-- FROM event_log('PIPELINE_ID_REAL');"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "d90574a0",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "CREATE OR REPLACE VIEW fintech_finpay.observability.vw_records_by_layer AS\n",
    "SELECT\n",
    "    CASE\n",
    "        WHEN origin.flow_name LIKE '%bronze.transactions'\n",
    "          OR origin.flow_name LIKE '%bronze.merchants'\n",
    "          OR origin.flow_name LIKE '%bronze.users'\n",
    "          OR origin.flow_name IN ('transactions', 'merchants', 'users')\n",
    "        THEN 'Bronze'\n",
    "\n",
    "        WHEN origin.flow_name LIKE '%silver_transactions'\n",
    "          OR origin.flow_name LIKE '%silver_merchants'\n",
    "          OR origin.flow_name LIKE '%silver_users'\n",
    "          OR origin.flow_name LIKE '%quarantine%'\n",
    "          OR origin.flow_name IN ('silver_transactions', 'silver_merchants', 'silver_users', 'quarantine')\n",
    "        THEN 'Silver'\n",
    "\n",
    "        WHEN origin.flow_name LIKE '%gold_%'\n",
    "        THEN 'Gold'\n",
    "\n",
    "        ELSE 'Other'\n",
    "    END AS layer,\n",
    "\n",
    "    origin.flow_name AS dataset_name,\n",
    "    DATE(timestamp) AS event_date,\n",
    "    SUM(CAST(details:flow_progress:metrics:num_output_rows AS BIGINT)) AS processed_records\n",
    "\n",
    "FROM fintech_finpay.observability.pipeline_event_log\n",
    "WHERE event_type = 'flow_progress'\n",
    "GROUP BY\n",
    "    CASE\n",
    "        WHEN origin.flow_name LIKE '%bronze.transactions'\n",
    "          OR origin.flow_name LIKE '%bronze.merchants'\n",
    "          OR origin.flow_name LIKE '%bronze.users'\n",
    "          OR origin.flow_name IN ('transactions', 'merchants', 'users')\n",
    "        THEN 'Bronze'\n",
    "\n",
    "        WHEN origin.flow_name LIKE '%silver_transactions'\n",
    "          OR origin.flow_name LIKE '%silver_merchants'\n",
    "          OR origin.flow_name LIKE '%silver_users'\n",
    "          OR origin.flow_name LIKE '%quarantine%'\n",
    "          OR origin.flow_name IN ('silver_transactions', 'silver_merchants', 'silver_users', 'quarantine')\n",
    "        THEN 'Silver'\n",
    "\n",
    "        WHEN origin.flow_name LIKE '%gold_%'\n",
    "        THEN 'Gold'\n",
    "\n",
    "        ELSE 'Other'\n",
    "    END,\n",
    "    origin.flow_name,\n",
    "    DATE(timestamp);"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c7f7fedf",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "CREATE OR REPLACE VIEW fintech_finpay.observability.vw_quarantine_rejections AS\n",
    "SELECT\n",
    "    source_name,\n",
    "    rejection_reason,\n",
    "    DATE(processing_timestamp) AS rejection_date,\n",
    "    COUNT(*) AS rejected_records\n",
    "FROM fintech_finpay.silver.quarantine\n",
    "GROUP BY\n",
    "    source_name,\n",
    "    rejection_reason,\n",
    "    DATE(processing_timestamp);"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "26aa77e1",
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "CREATE OR REPLACE VIEW fintech_finpay.observability.vw_quality_expectations AS\n",
    "SELECT\n",
    "    timestamp,\n",
    "    origin.flow_name AS dataset_name,\n",
    "    details:flow_progress:data_quality:expectations AS expectations\n",
    "FROM fintech_finpay.observability.pipeline_event_log\n",
    "WHERE event_type = 'flow_progress'\n",
    "  AND details:flow_progress:data_quality:expectations IS NOT NULL;"
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
