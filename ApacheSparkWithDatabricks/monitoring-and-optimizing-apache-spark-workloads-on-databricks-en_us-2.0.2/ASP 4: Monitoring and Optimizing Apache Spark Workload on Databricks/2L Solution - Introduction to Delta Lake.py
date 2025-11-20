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
# MAGIC # 2L: Introduction to Delta Lake
# MAGIC
# MAGIC In this lab, you will gain hands-on experience with Delta Lake's key features:
# MAGIC
# MAGIC - Creating Delta tables
# MAGIC - Performing basic operations (INSERT, UPDATE, DELETE)
# MAGIC - Executing MERGE operations
# MAGIC - Using time travel capabilities
# MAGIC - Optimizing performance
# MAGIC
# MAGIC We'll be using the customer data from the TPC-H dataset to work through these concepts.

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
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course. It will also set your default catalog to **dbacademy** and the schema to your specific schema name shown below using the `USE` statements.
# MAGIC <br></br>
# MAGIC
# MAGIC ```
# MAGIC USE CATALOG dbacademy;
# MAGIC USE SCHEMA dbacademy.<your unique schema name>;
# MAGIC ```
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

# COMMAND ----------

# MAGIC %run  ./Includes/Classroom-Setup-Lab

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Creating Delta Tables
# MAGIC
# MAGIC In this section, you'll create a Delta table from the TPC-H customers dataset. You'll transform the data to use more intuitive column names and filter for a specific subset of customers.

# COMMAND ----------

from pyspark.sql.functions import *

spark.sql("DROP TABLE IF EXISTS delta_customers")

customers_df = (spark.table("samples.tpch.customer")
  .select(
    col("c_custkey").alias("customer_id"),
    col("c_name").alias("name"),
    col("c_address").alias("address"),
    col("c_nationkey").alias("nation_id"),
    col("c_phone").alias("phone"),
    col("c_acctbal").alias("account_balance"),
    col("c_mktsegment").alias("market_segment"),
    col("c_comment").alias("comment")
  )
  .filter(col("c_mktsegment").isin("BUILDING", "HOUSEHOLD"))
  .limit(5000)
)

## Write the DataFrame as a Delta table
customers_df.write.mode("overwrite").saveAsTable("delta_customers")

# COMMAND ----------

# Inspect the data in the Delta table
display(spark.table("delta_customers"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Exploring Table Metadata
# MAGIC
# MAGIC Let's examine the metadata of our newly created Delta table.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspecting the table metadata
# MAGIC DESCRIBE EXTENDED delta_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL delta_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta_customers

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Basic Delta Operations
# MAGIC
# MAGIC Now you'll learn how to perform basic operations on Delta tables, including an `UPDATE` operation.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Increasing their account_balance by 10%
# MAGIC UPDATE delta_customers
# MAGIC SET account_balance = account_balance * 1.1
# MAGIC WHERE market_segment = 'BUILDING';
# MAGIC
# MAGIC -- Deleting customer with a negative account balance
# MAGIC DELETE FROM delta_customers
# MAGIC WHERE account_balance < 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Time Travel Operations
# MAGIC
# MAGIC Now let's explore Delta Lake's time travel capabilities to view and query previous versions of our data.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_customers VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as record_count FROM delta_customers VERSION AS OF 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Restore Operations
# MAGIC
# MAGIC Let's learn how to recover data by restoring to a previous version.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Checking the current count for the table
# MAGIC SELECT COUNT(*) as current_count FROM delta_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE delta_customers TO VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Checking count again for the table
# MAGIC SELECT COUNT(*) as current_count FROM delta_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Using the MERGE Function
# MAGIC
# MAGIC Now let's explore Delta Lake's powerful `MERGE` capabilities for upsert operations.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Setup a temporary view to simulated incoming updated customer data
# MAGIC CREATE OR REPLACE TEMPORARY VIEW updated_customers AS
# MAGIC SELECT 999901 AS customer_id, 
# MAGIC        "Customer#000999901" AS name,
# MAGIC        "123 Updated Street" AS address, -- Updated address
# MAGIC        1 AS nation_id,
# MAGIC        "1-123-456-7890" AS phone,
# MAGIC        CAST(10000.00 AS DECIMAL(18,2)) AS account_balance, -- Updated balance
# MAGIC        "BUILDING" AS market_segment,
# MAGIC        "Updated customer record via MERGE" AS comment -- Updated comment
# MAGIC UNION ALL
# MAGIC -- Add a new record that doesn't exist in the table yet
# MAGIC SELECT 999999 AS customer_id,
# MAGIC        "Customer#000999999" AS name,
# MAGIC        "999 Merge Street" AS address,
# MAGIC        5 AS nation_id,
# MAGIC        "1-999-999-9999" AS phone,
# MAGIC        CAST(15000.00 AS DECIMAL(18,2)) AS account_balance,
# MAGIC        "HOUSEHOLD" AS market_segment,
# MAGIC        "New customer added via MERGE" AS comment;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Perform a MERGE operation to update existing customers address, account_balance and comment fields and insert new customers
# MAGIC MERGE INTO delta_customers AS target
# MAGIC USING updated_customers AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET 
# MAGIC     address = source.address,
# MAGIC     account_balance = source.account_balance,
# MAGIC     comment = source.comment
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (customer_id, name, address, nation_id, phone, account_balance, market_segment, comment)
# MAGIC   VALUES (source.customer_id, source.name, source.address, source.nation_id, 
# MAGIC           source.phone, source.account_balance, source.market_segment, source.comment);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the results
# MAGIC SELECT * FROM delta_customers WHERE customer_id IN (999901, 999999);

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
