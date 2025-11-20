# Databricks notebook source
# MAGIC %md
# MAGIC # Seção 1 —  Configurar acesso ao Data Lake, criar Managed Table e referência de comandos
# MAGIC
# MAGIC Objetivo desta secção
# MAGIC - Descrever de forma concisa o que faz esta secção.
# MAGIC - Mostrar como configurar acesso ao ADLS Gen2 (exemplos com account key ou usando secret scope).
# MAGIC - Ler dados do storage, criar uma tabela gerida (managed table) no catálogo e mostrar comandos úteis de referência (listar, criar views, investigar Delta).
# MAGIC
# MAGIC Descrição
# MAGIC - Esta secção explica como dar acesso ao Data Lake (ADLS Gen2), como ler ficheiros (parquet) e transformar esses dados numa tabela gerida no catálogo (Unity Catalog ou metastore legacy).
# MAGIC - Inclui exemplos em Python (notebook) e SQL, e a lista de comandos de referência para operações comuns.
# MAGIC
# MAGIC Pré-requisitos
# MAGIC - Acesso ao workspace Databricks e permissões de criação de tabelas no schema alvo (`CREATE` no schema).
# MAGIC - Credenciais para aceder ao storage (account key, SAS, Service Principal ou Managed Identity).
# MAGIC - Se estiveres a usar Unity Catalog e paths abfss, o metastore pode exigir External Location / Storage Credential (administrador do metastore).
# MAGIC
# MAGIC
# MAGIC - Criar view permanente (SQL):
# MAGIC ```sql
# MAGIC CREATE OR REPLACE VIEW my_catalog.my_schema.sales_view AS
# MAGIC SELECT customer_id, SUM(total) AS revenue
# MAGIC FROM my_catalog.my_schema.workshop_sales
# MAGIC GROUP BY customer_id;
# MAGIC ```
# MAGIC
# MAGIC - Criar temp view (Python / sessão atual):
# MAGIC ```python
# MAGIC df.createOrReplaceTempView("tmp_sales")
# MAGIC # depois podes usar spark.sql("SELECT * FROM tmp_sales WHERE ...")
# MAGIC ```
# MAGIC
# MAGIC 5) Comandos de referência úteis (lista que podes colar)
# MAGIC - Listar tabelas/views:
# MAGIC ```sql
# MAGIC SHOW TABLES IN my_catalog.my_schema;
# MAGIC SHOW VIEWS IN my_catalog.my_schema;
# MAGIC ```
# MAGIC
# MAGIC - Criar tabela gerida:
# MAGIC ```python
# MAGIC df.write.format("delta").saveAsTable("my_catalog.my_schema.table_name")
# MAGIC ```
# MAGIC
# MAGIC - Criar tabela externa:
# MAGIC ```sql
# MAGIC CREATE TABLE my_catalog.my_schema.table
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://...'
# MAGIC ```
# MAGIC
# MAGIC - Criar view:
# MAGIC ```sql
# MAGIC CREATE OR REPLACE VIEW my_catalog.my_schema.v AS SELECT ...
# MAGIC ```
# MAGIC
# MAGIC - Criar temp view:
# MAGIC ```python
# MAGIC df.createOrReplaceTempView("tmp_name")
# MAGIC ```
# MAGIC
# MAGIC - Investigar Delta:
# MAGIC ```sql
# MAGIC DESCRIBE DETAIL my_catalog.my_schema.table;
# MAGIC DESCRIBE HISTORY my_catalog.my_schema.table;
# MAGIC ```

# COMMAND ----------

# Import all dependeces from the utilities module
from utilities import *

# COMMAND ----------

# Set the storage account and container details
storage_account = ""
container = ""
account_key = ""  # Ensure that BlobStorageEvents and SoftDelete are disabled in the Azure portal.

# Configure the account key for the DFS endpoint
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    account_key
)

# Define the abfss path
abfss_path = (
    f"abfss://{container}@{storage_account}.dfs.core.windows.net/converted-file.parquet"
)

# Read the parquet file
df = spark.read.parquet(abfss_path)
display(df.limit(100))

#Use function from utilities
read_parquet_with_metadata_filtered(abfss_path,"2025-10-21","2025-10-21")

# COMMAND ----------

# 1) criar/inscrever como tabela MANAGED no catálogo
catalog_table = "databrickstheloop.default.workshop_sales"   # ajusta catalog.schema.table conforme queres

# O método saveAsTable cria uma tabela gerida (managed) no metastore/catalog
df.write.format("delta").mode("overwrite").saveAsTable(catalog_table)

# 2) verificar
print("Tabela escrita:", catalog_table)
display(spark.sql(f"SELECT COUNT(*) AS total_rows FROM {catalog_table}"))
display(spark.sql(f"SELECT * FROM {catalog_table} LIMIT 10"))

# delta_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/delta/sales_delta"



# COMMAND ----------

# MAGIC %md
# MAGIC # Seção 2 — Clusters e Compute: tipos, policies, Photon, Spot instances e boas práticas
# MAGIC
# MAGIC Objetivo
# MAGIC - Explicar, de forma prática, os conceitos principais de compute no Databricks: tipos de cluster, políticas (cluster policies), Photon, uso de instâncias Spot/preemptible, pools e autoscaling.
# MAGIC - Fornecer orientações de configuração, trade‑offs de custo vs performance e pequenos exemplos/trechos prontos para usar como referência no workshop.
# MAGIC
# MAGIC Sumário rápido
# MAGIC - Tipos de cluster: All‑purpose (interactive), Job (automated), SQL Endpoints / Serverless SQL e Single node.
# MAGIC - Policies: regras aplicadas para controlar configurações de cluster (tamanhos, versões, tipos de instância, permissões).
# MAGIC - Photon: motor nativo da Databricks para acelerar workloads SQL/Analíticos (requer runtime compatível).
# MAGIC - Spot / Preemptible: instâncias de baixo custo, mas que podem ser reclamadas; exige configuração de fallback.
# MAGIC - Pools / Instance Pools: reduzem startup time e custos, mantêm instâncias quentes.
# MAGIC - Autoscaling: ajustar min/max workers para balancear custo vs tempo de execução.
# MAGIC
# MAGIC 1) Tipos de cluster (quando usar cada um)
# MAGIC - All‑purpose (Interactive)
# MAGIC   - Uso: notebooks exploratórios, desenvolvimento, debugging por utilizadores.
# MAGIC   - Características: cluster interativo, costuma ter libraries instaladas dinamicamente, vida útil ligada à sessão.
# MAGIC   - Boas práticas: auto‑terminate curto (ex.: 10–30 min) para controlar custos; políticas que restringem tamanho.
# MAGIC
# MAGIC - Job (Job clusters / Automated)
# MAGIC   - Uso: pipelines de produção orquestrados via Jobs; criados dinamicamente para executar uma tarefa e destruídos.
# MAGIC   - Características: tipicamente instanciados por Jobs API; evita “ruído” do ambiente de desenvolvimento.
# MAGIC   - Boas práticas: usar images/initialization controlados, definir min/max workers apropriados e usar pools para reduzir startup.
# MAGIC
# MAGIC - SQL Endpoints / Serverless SQL
# MAGIC   - Uso: queries BI interativas, dashboards, BI tools via JDBC/ODBC.
# MAGIC   - Características: otimizado para queries, muitas vezes usa Photon para acelerar SQL; pode ter caching e escalonamento diferenciado.
# MAGIC
# MAGIC - Single node (Single VM)
# MAGIC   - Uso: cargas pequenas, desenvolvimento ou transformações que não precisam de distribuição.
# MAGIC   - Observação: bom para debug local; não recomendado para produção massiva.
# MAGIC
# MAGIC 2) Cluster Policies (o quê e porquê)
# MAGIC - O que são: políticas que forçam regras sobre configurações de cluster (quem pode criar, quais tipos de instância, limits, tags, se pode usar public IP, etc.).
# MAGIC - Porquê: impor governança, controlar custos, evitar nodes muito grandes ou imagens inseguras.
# MAGIC - Exemplos de regras típicas:
# MAGIC   - Limitar node types a uma whitelist (p.ex. famílias aprovadas).
# MAGIC   - Forçar autoscaling com min/max definidos.
# MAGIC   - Proibir clusters com public IP.
# MAGIC   - Definir timeout de auto_terminate mínimo.
# MAGIC   - Bloquear instalação de init scripts não aprovados.
# MAGIC - Como aplicar:
# MAGIC   - Criar Cluster Policy no Admin Console → Cluster Policies → new policy, aplicar a usuários/grupos.
# MAGIC   - As políticas podem ser exigentes (não permitem override) ou por default com valores sugeridos.
# MAGIC
# MAGIC Exemplo (pseudo‑JSON) de constraints numa policy
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "Shared Compute",
# MAGIC   "rules": {
# MAGIC     "instanceType": { "allowedValues": ["Standard_D4s_v3","Standard_E8s_v3"] },
# MAGIC     "minWorkers": { "minValue": 0, "maxValue": 2 },
# MAGIC     "maxWorkers": { "minValue": 2, "maxValue": 20 },
# MAGIC     "autoTerminateMins": { "minValue": 5, "defaultValue": 20 },
# MAGIC     "allowPublicIp": { "allowedValues": [false] }
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC (Implementar esta policy via UI; sintaxe exacta varia conforme UI/versão.)
# MAGIC
# MAGIC 3) Photon — quando e como usar
# MAGIC - O que é: motor nativo Databricks (Photon) optimizado em código nativo para acelerar queries SQL, operações de leitura/parsing e agregações sobre Delta/Parquet.
# MAGIC - Benefícios:
# MAGIC   - Ganhos de latência e throughput em workloads SQL/analytics (útil para dashboards e BI).
# MAGIC   - Menor custo por query para workloads compatíveis.
# MAGIC - Requisitos:
# MAGIC   - Usar uma Databricks Runtime versão que suporte Photon (ver notas de release).
# MAGIC   - Nem todas as workloads ou bibliotecas são aceleradas — excelente para cargas analíticas com operações colunares e agregações.
# MAGIC - Como ativar:
# MAGIC   - Criar cluster/endpoint escolhendo um runtime com Photon habilitado (no UI, escolher “Photon” ou um runtime que indique suporte).
# MAGIC - Observações:
# MAGIC   - Testar com um benchmark antes de migrar produção.
# MAGIC   - Photon é especialmente eficaz em SQL endpoints (Databricks SQL) e workloads Spark SQL intensivas.
# MAGIC
# MAGIC 4) Spot / Preemptible instances (economizar com risco)
# MAGIC - O que são: instâncias oferecidas a preço reduzido pela cloud mas que podem ser reclamadas a qualquer momento (Azure Spot VMs / AWS Spot).
# MAGIC - Pros: custo significativamente menor.
# MAGIC - Cons: interrupções; necessidade de tolerância a falhas e re‑execução.
# MAGIC - Estratégias de uso:
# MAGIC   - Usar Spot para worker nodes e reservar driver em on‑demand (ou permitir fallback automático).
# MAGIC   - Configurar fallback para on‑demand se quota de spot não disponível.
# MAGIC   - Projetar jobs para serem resilientes (checkpointing, idempotência).
# MAGIC   - Evitar usar Spot para workloads sensíveis a latência ou para o driver (onde perda interrompe a sessão interativa).
# MAGIC - Configuração prática no Databricks:
# MAGIC   - No UI do cluster escolher opção Spot/preemptible para workers; definir política de fallback se oferecida.
# MAGIC   - Em Azure, é comum escolher Spot VMs para workers e definir “Allow Spot” + eviction policy.
# MAGIC - Recomendações:
# MAGIC   - Para ETL batch não crítico: usar Spot.
# MAGIC   - Para production interactive jobs: preferir on‑demand ou pools com mistura spot/on‑demand.
# MAGIC
# MAGIC 5) Pools / Instance Pools / Shared compute
# MAGIC - O que são: um conjunto gerido de instâncias que ficam quentes para reduzir tempo de startup (fast allocation).
# MAGIC - Benefícios:
# MAGIC   - Startup mais rápido de clusters, menor latência para notebooks/jobs.
# MAGIC   - Pode reduzir custos em workloads com muitos clusters curtos.
# MAGIC - Práticas:
# MAGIC   - Criar pools com tipos aprovados pelas policies.
# MAGIC   - Usar pools para jobs críticos que precisam de start rápido.
# MAGIC   - Monitorizar pool utilization para ajustar tamanho.
# MAGIC
# MAGIC 6) Autoscaling e sizing
# MAGIC - Autoscaling (Databricks dynamic autoscaling)
# MAGIC   - Ajusta número de workers entre minWorkers e maxWorkers com base em carga.
# MAGIC   - Evita sobredimensionamento (minimiza custos) e permite picos (maxWorkers).
# MAGIC - Escolher min/max:
# MAGIC   - Min = 0 ou 1 para baixo custo; para estabilidade, usar 1 como mínimo.
# MAGIC   - Max = dimensionar para o pior caso esperado (não exagerar).
# MAGIC - Driver vs Workers:
# MAGIC   - Driver deve ter recursos suficientes para o trabalho de coordenação (memória para plan).
# MAGIC   - Workers devem corresponder ao tipo de carga (CPU bound vs I/O bound vs GPU).
# MAGIC - Dicas de sizing:
# MAGIC   - Preferir mais nós menores ao invés de poucos nós gigantes para paralelismo (mas testar).
# MAGIC   - Considerar shuffle e memória, usar executor memory tuning se necessário.
# MAGIC   - Testar com amostras representativas.
# MAGIC
# MAGIC 7) Segurança, logging e governance
# MAGIC - Regras úteis em policies:
# MAGIC   - Forçar tags para billing e owner.
# MAGIC   - Proibir clusters com public IP e habilitar Private Link/Private IP.
# MAGIC   - Bloquear init scripts não aprovados.
# MAGIC - Logging:
# MAGIC   - Habilitar diagnostic logs de clusters e jobs (export para Log Analytics / storage).
# MAGIC   - Coletar metrics via Ganglia/Cloud monitoring.
# MAGIC - IAM / Instance Profiles:
# MAGIC   - Usar instance profiles / managed identity para dar acesso ao storage, evitando keys em texto.
# MAGIC   - Associar roles mínimos necessários (principle of least privilege).
# MAGIC
# MAGIC 8) Boas práticas resumidas
# MAGIC - Use cluster policies para governança e controlo de custos.
# MAGIC - Prefira pools para reduzir startup latency e custos com clusters curtos.
# MAGIC - Use Spot para workers quando a aplicação for tolerante à interrupção; mantenha driver em on‑demand.
# MAGIC - Teste Photon com cargas representativas antes de adoção plena.
# MAGIC - Auto‑terminate curto em clusters interativos; para jobs, preferir job clusters com lifecycle curto.
# MAGIC - Evite comitar segredos em código; use secret scopes / Key Vault / managed identities.
# MAGIC - Monitore e ajuste: use métricas de CPU, I/O, shuffle e GC para otimizar tamanho e configuração.
# MAGIC
# MAGIC 9) Checklist rápido ao criar um cluster
# MAGIC - [ ] Selecionar tipo: All‑purpose / Job / SQL Endpoint.
# MAGIC - [ ] Selecionar runtime compatível (Photon se aplicável).
# MAGIC - [ ] Definir node type (família aprovada) e min/max workers.
# MAGIC - [ ] Decidir uso de Spot para workers e fallback para on‑demand.
# MAGIC - [ ] Associar pool se houver necessidade de startup rápido.
# MAGIC - [ ] Configurar auto_terminate (10–30m para dev).
# MAGIC - [ ] Aplicar cluster policy apropriada.
# MAGIC - [ ] Associar instance profile / managed identity para acesso a storage.
# MAGIC - [ ] Habilitar logs/diagnostics.
# MAGIC
# MAGIC 10) Exemplos rápidos (UI / Notebook)
# MAGIC - Criar cluster interativo (UI): Compute → Create cluster → escolher policy → escolher runtime (Photon habilitado se aplicável) → definir instance types → min/max workers → pools/spot → Create.
# MAGIC - Criar job cluster (Jobs): Jobs → Create Job → Task → new cluster (especificar same settings) ou reuse an existing job cluster.
# MAGIC - Habilitar Photon: selecionar Databricks Runtime versão com “Photon” no nome (UI indica compatibilidade).

# COMMAND ----------

# MAGIC %md
# MAGIC #  Seção 3 — Criar Secret Scope (Key Vault) + Ler CSV + Criar tabela simples no catálogo
# MAGIC ###  Objetivo: adicionar ao notebook uma secção passo-a-passo para:
# MAGIC   1) criar um secret scope (Key Vault-backed ou Databricks-managed)
# MAGIC   2) colocar um segredo no scope
# MAGIC   3) aceder ao segredo via dbutils.secrets.get
# MAGIC   4) ler um CSV e escrever como tabela (catalog/workspace) simples
# MAGIC
# MAGIC ###  Nota importante:
# MAGIC  - A criação de secret scopes normalmente exige privilégios de admin no workspace ou acesso a Account Admin (dependendo do tipo de backend).
# MAGIC  - Exemplos abaixo mostram 2 caminhos: (A) via Databricks CLI (recomendado fora do notebook) e (B) via REST API (pode ser executado dentro do notebook usando um PAT).
# MAGIC  - Substitui todos os placeholders (<...>) pelos valores do teu ambiente.
# MAGIC

# COMMAND ----------

# Recomendo criar scope e secret via UI e depois fazer:
host = ""   # poucas razões para esconder o host
token = "" #dbutils.secrets.get(scope="workshop-scope", key="databricks_token")

import os
os.environ["DATABRICKS_HOST"] = host
os.environ["DATABRICKS_TOKEN"] = token


# COMMAND ----------

import os
import json
import requests

def create_scope_via_rest(scope_name, backend_type="DATABRICKS", azure_resource_id=None, azure_dns=None):
    """
    Tenta criar um secret scope usando a API do workspace.
    backend_type: "DATABRICKS" ou "AZURE_KEYVAULT"
    Se AZURE_KEYVAULT, fornece azure_resource_id e azure_dns.
    """
    # Detect workspace host and token
    try:
        host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
    except Exception:
        host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        raise RuntimeError("É necessário definir DATABRICKS_HOST e DATABRICKS_TOKEN")
    url = host.rstrip("/") + "/api/2.0/secrets/scopes/create"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"scope": scope_name}
    if backend_type.upper() == "AZURE_KEYVAULT":
        if not azure_resource_id or not azure_dns:
            raise ValueError("Para AZURE_KEYVAULT precisa de azure_resource_id e azure_dns.")
        payload["scope_backend_type"] = "AZURE_KEYVAULT"
        payload["backend_azure_keyvault"] = {"resource_id": azure_resource_id, "dns_name": azure_dns}
    else:
        payload["scope_backend_type"] = "DATABRICKS"
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code == 200:
        print("Scope criado com sucesso:", scope_name)
    else:
        print("Resposta API:", resp.status_code, resp.text)
        resp.raise_for_status()

# 2) Preenche os valores do teu Key Vault:
scope_name = "workshop-kv-test"   # nome que queres dar ao scope
azure_resource_id = ""

# 3) Chama a função (verifica primeiro que DATABRICKS_HOST e DATABRICKS_TOKEN estão definidos)
create_scope_via_rest(scope_name, azure_resource_id)

# COMMAND ----------

print("\nScopes visíveis via dbutils:")
try:
    for s in dbutils.secrets.listScopes():
        print(" -", s.name)
except Exception as e:
    print("Erro a listar scopes via dbutils.secrets.listScopes():", e)
    
try:
    val = dbutils.secrets.get(scope=scope_name, key="storage_key")
    print("dbutils.secrets.get() ok — segredos acessíveis (não imprimas em produção).")
except Exception as e:
    print("dbutils.secrets.get() falhou — verifica permissões no Key Vault / nome do segredo. Erro:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC #  Seção 4 — Script PySpark (Databricks) — gerar dims + fact com dados aleatórios e criar MANAGED TABLES (Delta)
# MAGIC  Ajusta os parâmetros abaixo (catalog, schema, volumes) antes de executar.

# COMMAND ----------

# MAGIC %sql
# MAGIC --Criar um schema/default schema dentro do catalog
# MAGIC CREATE SCHEMA IF NOT EXISTS databrickstheloop.databricksfundamentals
# MAGIC COMMENT 'Schema default para exercícios e tabelas geridas';

# COMMAND ----------

import builtins
import random
from datetime import datetime, timedelta

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DateType, DoubleType, TimestampType
)
from pyspark.sql.functions import year, month, col

# =========== CONFIGURAÇÃO (ajusta aqui) ===========
catalog = ""    # substitui pelo teu catálogo (ex.: databrickstheloop)
schema = ""               # substitui pelo teu schema (ex.: default)
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

# COMMAND ----------

# MAGIC %md
# MAGIC # Seção 5 — Delta Tables e gerar Parquet para escrever no teu Blob (ADLS Gen2 / Azure Blob)
# MAGIC
# MAGIC Objetivo
# MAGIC - Explicar o que são Delta Tables e porquê usar Delta em vez de Parquet puro.  
# MAGIC - Mostrar como gerar ficheiros Parquet localmente (PySpark), gravá‑los no teu Data Lake (ADLS Gen2 / Blob) de forma segura e, em seguida, criar tabelas (managed ou external) usando Delta.  
# MAGIC - Incluir exemplos práticos prontos a colar no Databricks; todos os placeholders ficam visíveis para substituíres (não partilhes segredos).
# MAGIC
# MAGIC Sumário rápido
# MAGIC - Delta = formato baseado em Parquet + log transaccional (ACID), time travel, compaction/OPTIMIZE, concorrência boa para ETL/BI.
# MAGIC - Workflow típico:
# MAGIC   1. Gerar/ingestar ficheiros Parquet no Blob (abfss://...).
# MAGIC   2. Ler esses Parquet e convertê‑los para Delta (ou escrever diretamente em Delta).
# MAGIC   3. Registar tabela no catálogo: Managed table (saveAsTable) ou External table (CREATE TABLE ... LOCATION 'abfss://...').
# MAGIC   4. Otimizar/particionar/usar Time Travel e VACUUM conforme necessário.
# MAGIC
# MAGIC Porquê usar Delta em vez de Parquet puro?
# MAGIC - ACID transactions: evita dados parcialmente escritos e problemas de concorrência.  
# MAGIC - Time Travel: consultar versões históricas com `VERSION AS OF` ou `TIMESTAMP AS OF`.  
# MAGIC - Schema evolution / enforcement: alterações de schema controladas.  
# MAGIC - Performance: OPTIMIZE + ZORDER para melhorar queries OLAP.  
# MAGIC - Operações MERGE (MERGE INTO) para ETL incremental (SCD, upserts).
# MAGIC
# MAGIC Boas práticas antes de gravar
# MAGIC - Não comitar secrets: usa secret scopes (Key Vault‑backed) ou Managed Identity.  
# MAGIC - Partitioning: partitionar por colunas com alta seletividade para acelerar scans (ex.: year, month).  
# MAGIC - Small files: evitar muitos ficheiros pequenos — compacta ou use coalesce/repartition antes de write.  
# MAGIC - Metadata: manter tabelas Delta com `OPTIMIZE` quando necessário.
# MAGIC
# MAGIC
# MAGIC
# MAGIC -------------------------------------------------------
# MAGIC Criar tabelas (Managed vs External) a partir do Delta
# MAGIC -------------------------------------------------------
# MAGIC
# MAGIC 1) Criar Managed Table (metastore gere storage)
# MAGIC - Usa `saveAsTable` ou CTAS (requer CREATE no schema):
# MAGIC ```python
# MAGIC # ler delta e gravar como tabela gerida (Delta)
# MAGIC catalog = "databrickstheloop"   # exemplo
# MAGIC schema = "default"
# MAGIC table = "workshop_sales_managed"
# MAGIC full = f"{catalog}.{schema}.{table}"
# MAGIC
# MAGIC df = spark.read.format("delta").load(delta_path)
# MAGIC df.write.format("delta").mode("overwrite").saveAsTable(full)
# MAGIC ```
# MAGIC
# MAGIC 2) Criar External Table (registar location existente)
# MAGIC - Requer que o metastore UC tenha uma EXTERNAL LOCATION cobrindo o path (e Storage Credential).  
# MAGIC - SQL (após admin criar external location) ou se o teu workspace antigo/legacy metastore permitir:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE databrickstheloop.default.workshop_sales_external
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://<container>@<account>.dfs.core.windows.net/delta/sample_data';
# MAGIC ```
# MAGIC
# MAGIC Se receberes erro "NO PARENT EXTERNAL LOCATION FOR PATH", pede ao metastore admin para criar o `EXTERNAL LOCATION` cobrindo o container (veja Secção anterior).
# MAGIC
# MAGIC -------------------------------------------------------
# MAGIC Operações úteis sobre Delta (SQL e PySpark)
# MAGIC -------------------------------------------------------
# MAGIC - Mostrar detalhe:
# MAGIC ```sql
# MAGIC DESCRIBE DETAIL databrickstheloop.default.workshop_sales_managed;
# MAGIC DESCRIBE HISTORY databrickstheloop.default.workshop_sales_managed;
# MAGIC ```
# MAGIC
# MAGIC - Time travel (consultar versão anterior):
# MAGIC ```sql
# MAGIC SELECT * FROM databrickstheloop.default.workshop_sales_managed VERSION AS OF 0 LIMIT 10;
# MAGIC -- OU por timestamp:
# MAGIC SELECT * FROM databrickstheloop.default.workshop_sales_managed TIMESTAMP AS OF '2025-01-01 00:00:00';
# MAGIC ```
# MAGIC
# MAGIC - MERGE (exemplo de upsert):
# MAGIC ```sql
# MAGIC MERGE INTO databrickstheloop.default.workshop_sales_managed t
# MAGIC USING updates u
# MAGIC ON t.sale_id = u.sale_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC ```
# MAGIC
# MAGIC - OPTIMIZE / ZORDER (se runtime suportar — melhora leitura em colunas específicas):
# MAGIC ```sql
# MAGIC OPTIMIZE databrickstheloop.default.workshop_sales_managed
# MAGIC WHERE year = 2025
# MAGIC ZORDER BY (product_id);
# MAGIC ```
# MAGIC
# MAGIC - VACUUM (limpar arquivos antigos) — cuidado com time travel:
# MAGIC ```sql
# MAGIC VACUUM databrickstheloop.default.workshop_sales_managed RETAIN 168 HOURS;  -- 7 dias
# MAGIC ```
# MAGIC
# MAGIC -------------------------------------------------------
# MAGIC Dicas práticas de partição e layout
# MAGIC -------------------------------------------------------
# MAGIC - Partition by year/month/date para fact tables com grande volume.  
# MAGIC - Evita high‑cardinality partition columns (muitos pequenos ficheiros).  
# MAGIC - Use ZORDER em colunas frequentemente filtradas (por ex., product_id, customer_id).  
# MAGIC - Configure `spark.sql.shuffle.partitions` e `shuffle` conforme o teu cluster para evitar tasks muito pequenas.
# MAGIC

# COMMAND ----------

import builtins
import random
from datetime import datetime, timedelta

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import year, month, col

storage_account = ""
container = ""
account_key = "" 

parquet_folder = "parquet/sample_data"
parquet_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{parquet_folder}"

delta_folder = "delta/sample_data_delta"
delta_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{delta_folder}"
# --------------------------------------------------------

spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", account_key)

# Parâmetros de geração
random.seed(42)   # reprodutibilidade
num_rows = 1000
start = datetime(2025, 1, 1)

# Gerar linhas de exemplo
rows = []
for i in range(1, num_rows + 1):
    d = start + timedelta(days=random.randint(0, 364))
    sale_id = i
    product_id = random.randint(1, 50)
    customer_id = random.randint(1, 200)
    store_id = random.randint(1, 10)
    total_amount = builtins.round(random.uniform(5, 500), 2)  # usa builtins.round para evitar conflito com pyspark.round
    rows.append((sale_id, d.date(), product_id, customer_id, store_id, total_amount))

# Schema
schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("date", DateType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("store_id", IntegerType(), False),
    StructField("total_amount", DoubleType(), False)
])

# Criar DataFrame
df = spark.createDataFrame(rows, schema=schema)

# Reparticionar para evitar many small files (ajusta conforme o teu cluster)
df_repart = df.repartition(8)

# Escrever Parquet
df_repart.write.mode("overwrite").parquet(parquet_path)
print("Parquet escritos em:", parquet_path)
display(spark.read.parquet(parquet_path).limit(5))

# Preparar e escrever Delta (particionado por year/month para melhor layout)
df_for_delta = df_repart.withColumn("year", year(col("date"))).withColumn("month", month(col("date")))
df_for_delta.write.mode("overwrite").format("delta").partitionBy("year", "month").save(delta_path)
print("Delta escrito em:", delta_path)

# Verificação rápida
print("Contagem (sample):", spark.read.format("delta").load(delta_path).count())
display(spark.read.format("delta").load(delta_path).limit(5))

# COMMAND ----------

delta_path = "abfss://@.dfs.core.windows.net/delta/sample_data_delta"
df = spark.read.format("delta").load(delta_path)
display(df)

# COMMAND ----------

# Criar tabelas (Managed vs External) a partir do Delta
catalog = "databrickstheloop"  
schema = "databricksfundamentals"
table = "workshop_sales_managed"
full = f"{catalog}.{schema}.{table}"

df = spark.read.format("delta").load(delta_path)
df.write.format("delta").mode("overwrite").saveAsTable(full)
