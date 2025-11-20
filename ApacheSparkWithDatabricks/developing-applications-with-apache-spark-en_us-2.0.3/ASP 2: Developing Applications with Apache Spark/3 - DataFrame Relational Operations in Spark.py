# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 - DataFrame Relational Operations in Spark
# MAGIC
# MAGIC This demonstration shows how to effectively use joins and set operations with DataFrames, focusing on performance optimization and best practices.
# MAGIC
# MAGIC ### Objectives
# MAGIC - Understand different types of DataFrame joins
# MAGIC - Implement performance optimizations for joins
# MAGIC - Handle complex join scenarios
# MAGIC - Use set operations effectively
# MAGIC - Apply best practices for data skew

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC     - In the drop-down, select **More**.
# MAGIC
# MAGIC     - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 1. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Setup and Data Loading
# MAGIC
# MAGIC First, let's load our sample retail data tables and examine their structures.

# COMMAND ----------

from pyspark.sql.functions import *

# Read the data 
transactions_df = spark.read.table("samples.bakehouse.sales_transactions")
customers_df = spark.read.table("samples.bakehouse.sales_customers")
franchises_df = spark.read.table("samples.bakehouse.sales_franchises")
suppliers_df = spark.read.table("samples.bakehouse.sales_suppliers")

# COMMAND ----------

# Examine schemas
transactions_df.printSchema()

# COMMAND ----------

customers_df.printSchema()

# COMMAND ----------

franchises_df.printSchema()

# COMMAND ----------

suppliers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Basic Join Operations
# MAGIC
# MAGIC Let's start with simple join operations to combine our data.

# COMMAND ----------

# Inner join example to enrich the transactions with store information
enriched_transactions = franchises_df.join(
    transactions_df,
    on="franchiseID",
    how="inner"
)

display(enriched_transactions)

# COMMAND ----------

# The "on" clause can contain an expression
enriched_transactions = franchises_df.join(
    transactions_df,
    on= transactions_df.franchiseID == franchises_df.franchiseID,
    how="inner"
)

display(enriched_transactions)

# This is particularly useful if the join key is named differently in both entities

# COMMAND ----------

# Please note how all fields from both dataframes are present in the result, a better practice is to project the columns you need from each entity
# We will also alias some of the columns to disambiguate column names
enriched_transactions = franchises_df \
    .select(
        "franchiseID", 
        col("name").alias("store_name"), 
        col("city").alias("store_city"), 
        col("country").alias("store_country")
        ) \
    .join(
        transactions_df,
        on="franchiseID",
        how="inner"
    )
    
display(enriched_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Full Outer Join Operations
# MAGIC
# MAGIC Let's analyze the relationships between dataframes and identify missing data using outer joins

# COMMAND ----------

# Let's analyze the relationship between franchises and suppliers using a full outer join
full_join = franchises_df \
    .withColumnRenamed("name", "franchise_name") \
    .join(
        suppliers_df.select("supplierID", col("name").alias("supplier_name")),
        on="supplierID",
        how="full_outer" # Doing outer join
    )

# Find records that would NOT appear in an inner join
# These are records where either franchises or suppliers data is null
non_matching_records = full_join.filter(
        col("franchiseID").isNull() | 
        col("supplier_name").isNull()
    ) \
    .select("franchiseID", "franchise_name", col("supplierID").alias("orphaned_supplier_id"))

display(non_matching_records)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Spark SQL
# MAGIC
# MAGIC Let's do this using Spark SQL now....
# MAGIC

# COMMAND ----------

# Create temporary views
franchises_df.createOrReplaceTempView("franchises")
suppliers_df.createOrReplaceTempView("suppliers")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's do our outer join using SQL
# MAGIC SELECT 
# MAGIC     f.franchiseID,
# MAGIC     f.name as franchise_name,
# MAGIC     f.supplierID as orphaned_supplier_id
# MAGIC FROM franchises f
# MAGIC FULL OUTER JOIN suppliers s
# MAGIC ON f.supplierID = s.supplierID
# MAGIC WHERE f.franchiseID IS NULL OR s.name IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Set Operations
# MAGIC
# MAGIC Now let's explore relationships using set operations using the DataFrame API.

# COMMAND ----------

# Identify supplier IDs in each DataFrame
franchise_suppliers = franchises_df.select("supplierID").distinct()
all_suppliers = suppliers_df.select("supplierID").distinct()

# Find supplierIDs that are in franchises_df but not in suppliers_df
franchises_without_valid_suppliers = franchise_suppliers.subtract(all_suppliers)
display(franchises_without_valid_suppliers)

# COMMAND ----------

# Find the overlap - suppliers that exist in both tables
common_suppliers = franchise_suppliers.intersect(all_suppliers)
display(common_suppliers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Join Strategy**
# MAGIC    - Use inner joins where keys exist in all dataframes
# MAGIC    - Use outer joins where there is a possibility that keys don't exist in both dataframes
# MAGIC    - Handle column name conflicts
# MAGIC
# MAGIC 2. **Performance Optimization**
# MAGIC    - Filter before joining
# MAGIC    - Project only needed columns
# MAGIC    - Handle skewed keys appropriately
# MAGIC    - Reference the smaller dataframe first; or
# MAGIC    - Use broadcast joins for small tables

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
