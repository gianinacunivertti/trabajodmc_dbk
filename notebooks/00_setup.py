# Databricks notebook source
# ============================================================
# 00_setup.py
# Proyecto FinPay - Lakehouse Fraud Detection
# Setup inicial de catalogos, schemas, volume, carpetas y metadata
# Compatible con dev/prod mediante parametro catalog
# ============================================================

# COMMAND ----------

# ------------------------------------------------------------
# Parametros
# ------------------------------------------------------------
# El job del bundle debe enviar:
# base_parameters:
#   catalog: ${var.catalog}
#
# Dev  -> fintech_finpay
# Prod -> fintech_finpay_prod
# ------------------------------------------------------------

try:
    dbutils.widgets.text("catalog", "fintech_finpay")
    CATALOG = dbutils.widgets.get("catalog")
except Exception:
    CATALOG = "fintech_finpay"

SCHEMA_DEFAULT = "default"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"
SCHEMA_OBSERVABILITY = "observability"

VOLUME_NAME = "vol_landing"
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA_DEFAULT}/{VOLUME_NAME}"

print("Catalog:", CATALOG)
print("Landing path:", LANDING_PATH)

# COMMAND ----------

# ------------------------------------------------------------
# Crear catalogo, schemas y volume
# ------------------------------------------------------------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_DEFAULT}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_SILVER}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_OBSERVABILITY}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_DEFAULT}.{VOLUME_NAME}")

print("Catalog, schemas y volume creados o ya existentes.")

# COMMAND ----------

# ------------------------------------------------------------
# Crear estructura de carpetas en landing zone
# ------------------------------------------------------------

folders = [
    "transactions",
    "merchants",
    "users",
    "metadata",
    "schemas",
    "schemas/transactions",
    "schemas/merchants",
    "schemas/users",
    "checkpoints",
    "checkpoints/transactions",
    "checkpoints/merchants",
    "checkpoints/users",
    "bad_records",
    "bad_records/transactions",
    "bad_records/merchants",
    "bad_records/users"
]

for folder in folders:
    dbutils.fs.mkdirs(f"{LANDING_PATH}/{folder}")

display(dbutils.fs.ls(LANDING_PATH))

# COMMAND ----------

# ------------------------------------------------------------
# Crear archivo metadata-driven ingestion_archetypes.json
# ------------------------------------------------------------

import json

archetypes = [
    {
        "source_name": "transactions",
        "source_path": f"{LANDING_PATH}/transactions",
        "file_format": "csv",
        "delimiter": ",",
        "header": True,
        "target_table": "transactions_raw",
        "schema_location": f"{LANDING_PATH}/schemas/transactions",
        "checkpoint_path": f"{LANDING_PATH}/checkpoints/transactions",
        "bad_records_path": f"{LANDING_PATH}/bad_records/transactions",
        "active": True
    },
    {
        "source_name": "merchants",
        "source_path": f"{LANDING_PATH}/merchants",
        "file_format": "json",
        "delimiter": None,
        "header": False,
        "target_table": "merchants_raw",
        "schema_location": f"{LANDING_PATH}/schemas/merchants",
        "checkpoint_path": f"{LANDING_PATH}/checkpoints/merchants",
        "bad_records_path": f"{LANDING_PATH}/bad_records/merchants",
        "active": True
    },
    {
        "source_name": "users",
        "source_path": f"{LANDING_PATH}/users",
        "file_format": "txt",
        "delimiter": "|",
        "header": True,
        "target_table": "users_raw",
        "schema_location": f"{LANDING_PATH}/schemas/users",
        "checkpoint_path": f"{LANDING_PATH}/checkpoints/users",
        "bad_records_path": f"{LANDING_PATH}/bad_records/users",
        "active": True
    }
]

metadata_path = f"{LANDING_PATH}/metadata/ingestion_archetypes.json"

dbutils.fs.put(
    metadata_path,
    json.dumps(archetypes, indent=2),
    overwrite=True
)

print(f"Archivo metadata creado: {metadata_path}")

# COMMAND ----------

# ------------------------------------------------------------
# Validar archivo metadata
# ------------------------------------------------------------

metadata_content = dbutils.fs.head(metadata_path)
metadata_json = json.loads(metadata_content)

print("Tipo:", type(metadata_json))
print("Fuentes activas:", len([x for x in metadata_json if x.get("active") is True]))

display(spark.createDataFrame(metadata_json))

# COMMAND ----------

# ------------------------------------------------------------
# Crear datos de ejemplo si no existen archivos en landing
# Esto permite que el pipeline tenga datos para correr en dev/prod.
# ------------------------------------------------------------

transactions_path = f"{LANDING_PATH}/transactions/transactions_sample.csv"
merchants_path = f"{LANDING_PATH}/merchants/merchants.json"
users_path = f"{LANDING_PATH}/users/users_sample.txt"

# COMMAND ----------

# Transactions CSV

transactions_csv = """transaction_id,user_id,merchant_id,channel,transaction_type,amount,currency,transaction_date,status,reference_id
TXN-20260530-00001,USR-000001,MCH-00001,app,pago,120.50,PEN,2026-05-30,aprobado,
TXN-20260530-00002,USR-000002,MCH-00002,web,pago,850.00,PEN,2026-05-30,aprobado,
TXN-20260530-00003,USR-000001,MCH-00001,app,reversa,120.50,PEN,2026-05-31,aprobado,TXN-20260530-00001
TXN-20260530-00004,USR-000003,MCH-00003,pos,retiro,300.00,USD,2026-05-31,rechazado,
TXN-20260530-00005,USR-000004,MCH-00002,web,pago,1500.00,PEN,2026-06-01,pendiente,
"""

dbutils.fs.put(transactions_path, transactions_csv, overwrite=True)
print(f"Transactions sample creado: {transactions_path}")

# COMMAND ----------

# Merchants JSON
# Se escribe como JSON lines para facilitar lectura con Auto Loader JSON.

merchants_jsonl = """{"merchant_id":"MCH-00001","merchant_name":"Comercio Lima Centro","category":"retail","country":"PE","affiliation_date":"2025-01-15","status":"activo","risk_level":"bajo"}
{"merchant_id":"MCH-00002","merchant_name":"Fintech Market","category":"tecnologia","country":"PE","affiliation_date":"2025-03-10","status":"activo","risk_level":"medio"}
{"merchant_id":"MCH-00003","merchant_name":"Restaurante Norte","category":"restaurante","country":"PE","affiliation_date":"2025-05-20","status":"activo","risk_level":"alto"}
"""

dbutils.fs.put(merchants_path, merchants_jsonl, overwrite=True)
print(f"Merchants sample creado: {merchants_path}")

# COMMAND ----------

# Users TXT delimitado por |

users_txt = """user_id|full_name|document_id|email|phone|country|segment|registration_date
USR-000001|Ana Perez|DNI12345678|ana.perez@example.com|+51987654321|PE|premium|2025-01-01
USR-000002|Luis Gomez|DNI87654321|luis.gomez@example.com|+51912345678|PE|estandar|2025-02-15
USR-000003|Maria Torres|DNI56781234|maria.torres@example.com|+51955555555|PE|nuevo|2025-04-10
USR-000004|Carlos Ruiz|DNI11112222|carlos.ruiz@example.com|+51999999999|PE|premium|2025-06-01
"""

dbutils.fs.put(users_path, users_txt, overwrite=True)
print(f"Users sample creado: {users_path}")

# COMMAND ----------

# ------------------------------------------------------------
# Validar archivos creados
# ------------------------------------------------------------

print("Landing transactions:")
display(dbutils.fs.ls(f"{LANDING_PATH}/transactions"))

print("Landing merchants:")
display(dbutils.fs.ls(f"{LANDING_PATH}/merchants"))

print("Landing users:")
display(dbutils.fs.ls(f"{LANDING_PATH}/users"))

print("Landing metadata:")
display(dbutils.fs.ls(f"{LANDING_PATH}/metadata"))

# COMMAND ----------

# ------------------------------------------------------------
# Permisos opcionales
# Si los grupos no existen, se omiten sin detener el setup.
# ------------------------------------------------------------

groups = ["ingenieria", "riesgo", "auditoria"]

for group_name in groups:
    try:
        spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{group_name}`")
        print(f"Permiso USE CATALOG asignado a {group_name}")
    except Exception as e:
        print(f"No se pudo asignar permiso a grupo {group_name}: {str(e)}")

# COMMAND ----------

# ------------------------------------------------------------
# Funciones de gobernanza opcionales
# Masking function y row filter function
# ------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA_DEFAULT}.mask_pii(value STRING)
RETURN
  CASE
    WHEN is_account_group_member('ingenieria') THEN value
    ELSE '***MASKED***'
  END
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA_DEFAULT}.users_row_filter(country STRING)
RETURN
  CASE
    WHEN is_account_group_member('ingenieria') THEN TRUE
    WHEN is_account_group_member('riesgo') THEN TRUE
    WHEN is_account_group_member('auditoria') THEN TRUE
    ELSE FALSE
  END
""")

print("Funciones de gobernanza creadas.")

# COMMAND ----------

# ------------------------------------------------------------
# Resumen final
# ------------------------------------------------------------

summary = [
    ("catalog", CATALOG),
    ("default_schema", f"{CATALOG}.{SCHEMA_DEFAULT}"),
    ("bronze_schema", f"{CATALOG}.{SCHEMA_BRONZE}"),
    ("silver_schema", f"{CATALOG}.{SCHEMA_SILVER}"),
    ("gold_schema", f"{CATALOG}.{SCHEMA_GOLD}"),
    ("observability_schema", f"{CATALOG}.{SCHEMA_OBSERVABILITY}"),
    ("landing_path", LANDING_PATH),
    ("metadata_path", metadata_path),
]

display(
    spark.createDataFrame(summary, ["item", "value"])
)

print("Setup finalizado correctamente.")