# Databricks notebook source
import builtins
import random
from datetime import datetime, timedelta

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DateType, DoubleType, TimestampType
)
from pyspark.sql.functions import year, month, col

# =========== CONFIGURAÇÃO (ajusta aqui) ===========
catalog = "databrickstheloop"    # substitui pelo teu catálogo (ex.: databrickstheloop)
schema = "databricksfundamentals"               # substitui pelo teu schema (ex.: default)
table_dim_date = "dim_date"
table_dim_product = "dim_product"
table_dim_customer = "dim_customer"
table_dim_store = "dim_store"
table_fact_sales = "fact_sales"

# volume de dados para o workshop (ajusta p/ performance)
num_customers = 1000
num_products = 200
num_stores = 10
num_days = 365
num_facts = 20000   # linha de fact; reduz se o cluster for pequeno

random.seed(42)  # reproducibilidade para demonstrações
# ====================================================

# Helper: nomes completos
full_dim_date = f"{catalog}.{schema}.{table_dim_date}"
full_dim_product = f"{catalog}.{schema}.{table_dim_product}"
full_dim_customer = f"{catalog}.{schema}.{table_dim_customer}"
full_dim_store = f"{catalog}.{schema}.{table_dim_store}"
full_fact_sales = f"{catalog}.{schema}.{table_fact_sales}"

# ------------------------
# 1) Date dimension
# ------------------------
start_date = datetime(2025, 1, 1)
dates = []
for i in range(num_days):
    d = start_date + timedelta(days=i)
    dates.append((d.date(), d.year, d.month, d.day, d.isoweekday()))

date_schema = StructType([
    StructField("date", DateType(), False),
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("day", IntegerType(), False),
    StructField("weekday", IntegerType(), False)
])
date_df = spark.createDataFrame(dates, schema=date_schema)

# ------------------------
# 2) Product dimension
# ------------------------
categories = ["Electronics", "Home", "Clothing", "Toys", "Food"]
products = []
for pid in range(1, num_products + 1):
    price = builtins.round(random.uniform(5, 500), 2)   # usa builtins.round para evitar conflito
    products.append((pid, f"prod_{pid:04d}", random.choice(categories), price))

prod_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("price", DoubleType(), False)
])
product_df = spark.createDataFrame(products, schema=prod_schema)

# ------------------------
# 3) Customer dimension
# ------------------------
countries = ["PT", "ES", "FR", "GB", "DE"]
customers = []
for cid in range(1, num_customers + 1):
    customers.append((cid, f"cust_{cid:05d}", f"user{cid}@example.com", random.choice(countries)))

cust_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("country", StringType(), False)
])
customer_df = spark.createDataFrame(customers, schema=cust_schema)

# ------------------------
# 4) Store dimension
# ------------------------
stores = []
for sid in range(1, num_stores + 1):
    stores.append((sid, f"store_{sid:02d}", f"city_{sid}"))

store_schema = StructType([
    StructField("store_id", IntegerType(), False),
    StructField("store_name", StringType(), False),
    StructField("city", StringType(), False)
])
store_df = spark.createDataFrame(stores, schema=store_schema)

# ------------------------
# 5) Sales fact — gerar de forma eficiente
# ------------------------
# Reutilizar listas (evita collect dentro do loop)
date_list = [r[0] for r in dates]  # lista de date Python (não Spark)
product_ids = [p[0] for p in products]
customer_ids = [c[0] for c in customers]
store_ids = [s[0] for s in stores]
# map product price para lookup rápido
prod_price_map = {p[0]: p[3] for p in products}

facts = []
for i in range(1, num_facts + 1):
    d = random.choice(date_list)
    pid = random.choice(product_ids)
    cid = random.choice(customer_ids)
    sid = random.choice(store_ids)
    qty = random.randint(1, 5)
    price = float(prod_price_map[pid])
    total = builtins.round(qty * price, 2)
    facts.append((i, d, pid, cid, sid, qty, price, total, datetime.combine(d, datetime.min.time())))

fact_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("date", DateType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("store_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("event_ts", TimestampType(), False)
])
fact_df = spark.createDataFrame(facts, schema=fact_schema)

# ------------------------
# 6) Enriquecer fact para particionamento
# ------------------------
fact_df = fact_df.withColumn("year", year(col("date"))).withColumn("month", month(col("date")))

# ------------------------
# 7) Criar as MANAGED TABLES (Delta) — saveAsTable
# ------------------------
# Opcional: ajustar spark.sql.shuffle.partitions para o tamanho do cluster (evita many small tasks)
try:
    spark.conf.set("spark.sql.shuffle.partitions", "200")
except Exception:
    # ignora se não der
    pass

# Gravar dims
print("Criando tabelas geridas (overwrite)...")
date_df.write.format("delta").mode("overwrite").saveAsTable(full_dim_date)
product_df.write.format("delta").mode("overwrite").saveAsTable(full_dim_product)
customer_df.write.format("delta").mode("overwrite").saveAsTable(full_dim_customer)
store_df.write.format("delta").mode("overwrite").saveAsTable(full_dim_store)

# Gravar fact particionada
fact_df.write.format("delta").mode("overwrite").partitionBy("year", "month").saveAsTable(full_fact_sales)

print("Tabelas criadas:")
print(" -", full_dim_date)
print(" -", full_dim_product)
print(" -", full_dim_customer)
print(" -", full_dim_store)
print(" -", full_fact_sales)

# ------------------------
# 8) Verificações e amostras
# ------------------------
print("\nContagens (verificando):")
for t in [full_dim_date, full_dim_product, full_dim_customer, full_dim_store, full_fact_sales]:
    try:
        cnt = spark.table(t).count()
    except Exception as e:
        cnt = f"ERROR: {e}"
    print(f" - {t}: {cnt}")

print("\nExemplo join (amostra):")
spark.sql(f"""
SELECT s.sale_id, s.date, s.quantity, s.total_amount, p.product_name, c.customer_name, st.store_name
FROM {full_fact_sales} s
JOIN {full_dim_product} p ON s.product_id = p.product_id
JOIN {full_dim_customer} c ON s.customer_id = c.customer_id
JOIN {full_dim_store} st ON s.store_id = st.store_id
LIMIT 20
""").show(truncate=False)

# Fim
print("\nScript concluído. Se houver erros de permissão: peça ao admin para conceder 'CREATE' no schema.")
