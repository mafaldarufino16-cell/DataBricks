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
# MAGIC # 01: Introduction to Delta Lake
# MAGIC
# MAGIC This demonstration showcases Delta Lake's key features using the DataFrame API with Python:
# MAGIC
# MAGIC - Setting up Delta tables
# MAGIC - Basic operations (`INSERT`, `UPDATE`, `DELETE`)
# MAGIC - `MERGE` operations
# MAGIC - Schema evolution
# MAGIC - Time travel capabilities
# MAGIC - Performance optimization
# MAGIC
# MAGIC We'll be using data from the TPC-H dataset to demonstrate these concepts.
# MAGIC

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

# MAGIC %run  ./Includes/Classroom-Setup-Common

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Creating Delta Tables
# MAGIC
# MAGIC Delta tables can be created using standard DDL, e.g. `CREATE TABLE` statements (which can be used to create a new empty delta table), these can also be `CREATE TABLE AS SELECT` to create tables from the results of a query.  
# MAGIC
# MAGIC > **NOTE:** Delta tables support the `CREATE OR REPLACE TABLE ... AS SELECT` feature which allows a table to be redefined with the results of the new `SELECT` statement.
# MAGIC
# MAGIC Let's create our first Delta table using data from the TPC-H dataset. We'll start with the **orders** table.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col
order_df = spark.read.table("samples.tpch.orders")

# COMMAND ----------

# Displaying Orders_df
display(order_df)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS delta_orders")

# Transform and save the orders data using DataFrame API
orders_df = (
  order_df.select(
    col("o_orderkey").alias("order_id"),
    col("o_custkey").alias("customer_id"),
    col("o_orderstatus").alias("status"),
    col("o_totalprice").alias("total_price"),
    col("o_orderdate").alias("order_date"),
    col("o_orderpriority").alias("priority"),
    col("o_clerk").alias("clerk"),
    col("o_shippriority").alias("ship_priority"),
    col("o_comment").alias("comment")
  )
  .filter(col("o_orderdate") >= "1996-01-01")
  .limit(10000)
)

# Write the DataFrame as a Delta table
orders_df.write.mode("overwrite").saveAsTable("delta_orders")

# Display the Delta table
display(spark.table("delta_orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE:** Delta Lake is the default format for Databricks runtimes, otherwise you would need to use:
# MAGIC   `orders_df.write.format("delta").mode("overwrite").saveAsTable("delta_orders")`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The SQL equivalent of above spark query
# MAGIC DROP TABLE IF EXISTS delta_orders_sql;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE delta_orders_sql
# MAGIC AS
# MAGIC SELECT
# MAGIC   o_orderkey AS order_id,
# MAGIC   o_custkey AS customer_id,
# MAGIC   o_orderstatus AS status,
# MAGIC   o_totalprice AS total_price,
# MAGIC   o_orderdate AS order_date,
# MAGIC   o_orderpriority AS priority,
# MAGIC   o_clerk AS clerk,
# MAGIC   o_shippriority AS ship_priority,
# MAGIC   o_comment AS comment
# MAGIC FROM samples.tpch.orders
# MAGIC WHERE o_orderdate >= '1996-01-01'
# MAGIC LIMIT 10000;
# MAGIC
# MAGIC SELECT * FROM delta_orders_sql;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect table metadata
# MAGIC DESCRIBE EXTENDED delta_orders

# COMMAND ----------

# Inspecting using spark.sql
display(spark.sql("DESCRIBE EXTENDED delta_orders"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This is a delta lake specific command which returns detailed metadata about a Delta table including statistics
# MAGIC DESCRIBE DETAIL delta_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use this command to see the history of changes to the delta table, we will revisit this later..
# MAGIC DESCRIBE HISTORY delta_orders

# COMMAND ----------

# We can use the DeltaTable class to interact with the Delta table programmatically
from delta.tables import DeltaTable

# Get the Delta table
delta_table = DeltaTable.forName(spark, "delta_orders")

display(delta_table.history())

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Basic Delta Operations with DataFrame API
# MAGIC
# MAGIC Let's demonstrate basic DML operations with Delta tables using the DataFrame API.

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. Insert Operation

# COMMAND ----------

# Create a DataFrame with new records
from datetime import date
from decimal import Decimal

new_orders = spark.createDataFrame([
    (999997, 12345, 'P', Decimal('1234.56'), date(2023, 2, 15), "1-URGENT", "Clerk#000000123", 0, "New test order using Python"),
    (999996, 12346, 'O', Decimal('5678.90'), date(2023, 2, 16), "2-HIGH", "Clerk#000000456", 0, "Another test order using Python")
], schema=orders_df.schema)

# Append new records to the Delta table
new_orders.write.format("delta").mode("append").saveAsTable("delta_orders")

# Verify the new records
display(spark.table("delta_orders").filter(col("order_id").isin([999997, 999996])))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert two more records using SQL
# MAGIC INSERT INTO delta_orders
# MAGIC VALUES 
# MAGIC (999999, 12345, 'P', DECIMAL('1234.56'), DATE('2023-02-15'), '1-URGENT', 'Clerk#000000123', 0, 'New test order using SQL'),
# MAGIC (999998, 12346, 'O', DECIMAL('5678.90'), DATE('2023-02-16'), '2-HIGH', 'Clerk#000000456', 0, 'Another test order using SQL');
# MAGIC
# MAGIC -- Verify the insertion
# MAGIC SELECT * FROM delta_orders WHERE order_id IN (999999, 999998);
# MAGIC

# COMMAND ----------

# Check the history again now
display(delta_table.history())

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. DELETE Operation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Whoops!
# MAGIC DELETE FROM delta_orders

# COMMAND ----------

# Check the history again now
display(delta_table.history())

# COMMAND ----------

# Check the delta dataframe count
delta_table.toDF().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Time Travel Operations
# MAGIC
# MAGIC Explore Delta Lake's time travel capabilities for data auditing and recovery. Let's use this feature to query previous versions of our data.
# MAGIC **Time Travel can be performed using:**
# MAGIC - **Version numbers**: Integer values starting from 0 (e.g., `VERSION AS OF 2`)
# MAGIC - **Timestamps**: Formatted as ISO-8601 strings (e.g., `TIMESTAMP AS OF '2023-02-15T12:30:00.000Z'`)
# MAGIC

# COMMAND ----------

# Read a specific version of the Delta table
previous_version = spark.read.option("versionAsOf", 2).table("delta_orders")
display(previous_version.filter(col("order_id").isin([999999, 999998, 999997, 999996])))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The SQL equivalent of ☝️
# MAGIC SELECT * FROM delta_orders VERSION AS OF 2
# MAGIC WHERE order_id IN (999999, 999998, 999997, 999996);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta_orders

# COMMAND ----------

# MAGIC %md
# MAGIC Replace with an actual timestamp of version from your `DESCRIBE HISTORY` results. </br>
# MAGIC Format example: '2023-02-15T12:30:00.000Z' or '2023-02-15 12:30:00'

# COMMAND ----------

# We can also traverse historical versions using a timestamp
timestamp_to_restore_to = 'REPLACE THIS VALUE WITH A TIMESTAMP' 
display(
    spark.read.option("timestampAsOf", timestamp_to_restore_to).table("delta_orders")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE**: Use the command `describe history <table_name>` to get version info. 
# MAGIC
# MAGIC You will find the transaction time to use above.

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Restore Operations
# MAGIC
# MAGIC Learn how to recover data using Delta Lake's restore capabilities.

# COMMAND ----------

# Restore to the original version using SQL
spark.sql("RESTORE TABLE delta_orders TO VERSION AS OF 1")

# Verify the restore worked
display(spark.table("delta_orders"))

# COMMAND ----------

# Check the history again now
display(delta_table.history())

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Using MERGE Function
# MAGIC
# MAGIC Delta Lake provides powerful **MERGE** capabilities for upsert operations (update + insert) - as well as conditional `DELETE`s. Before jumping to MERGE lets look at Upsert functionality
# MAGIC
# MAGIC **What is an Upsert?**
# MAGIC - An "upsert" combines UPDATE and INSERT operations in a single atomic transaction
# MAGIC - If a matching record exists, it's updated; if not, a new record is inserted
# MAGIC - This is particularly useful for scenarios like:
# MAGIC   - Processing change data capture (CDC) feeds
# MAGIC   - Handling slowly changing dimensions (SCD)
# MAGIC   - Streaming data integration
# MAGIC   - Deduplication with latest values
# MAGIC
# MAGIC Let's see how Upsert can be used in MERGE operation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- First, create a temporary view with updated order data
# MAGIC CREATE OR REPLACE TEMPORARY VIEW updated_orders AS
# MAGIC SELECT 999996 AS order_id, 12346 AS customer_id, 'O' AS status, 
# MAGIC        CAST(6000.00 AS DECIMAL(18,2)) AS total_price, -- Updated price
# MAGIC        CAST('2023-02-16' AS DATE) AS order_date, 
# MAGIC        '1-URGENT' AS priority, -- Changed from 2-HIGH to 1-URGENT
# MAGIC        'Clerk#000000456' AS clerk, 
# MAGIC        0 AS ship_priority,
# MAGIC        'Updated order with higher priority' AS comment -- Updated comment
# MAGIC UNION ALL
# MAGIC -- Add a new record that doesn't exist in the table yet
# MAGIC SELECT 999995 AS order_id, 12347 AS customer_id, 'P' AS status, 
# MAGIC        CAST(2500.50 AS DECIMAL(18,2)) AS total_price,
# MAGIC        CAST('2023-02-17' AS DATE) AS order_date, 
# MAGIC        '3-MEDIUM' AS priority,
# MAGIC        'Clerk#000000789' AS clerk, 
# MAGIC        1 AS ship_priority,
# MAGIC        'New order via MERGE' AS comment;
# MAGIC
# MAGIC -- Now perform the MERGE operation
# MAGIC MERGE INTO delta_orders
# MAGIC USING updated_orders AS updates
# MAGIC ON delta_orders.order_id = updates.order_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET 
# MAGIC     total_price = updates.total_price,
# MAGIC     priority = updates.priority,
# MAGIC     comment = updates.comment
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (order_id, customer_id, status, total_price, order_date, priority, clerk, ship_priority, comment)
# MAGIC   VALUES (updates.order_id, updates.customer_id, updates.status, updates.total_price, 
# MAGIC           updates.order_date, updates.priority, updates.clerk, updates.ship_priority, updates.comment);
# MAGIC
# MAGIC -- Verify the results
# MAGIC SELECT * FROM delta_orders WHERE order_id IN (999995, 999996);

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE:** The upsert is performed by the MERGE statement, which updates existing rows and inserts new ones based on whether the order_id exists in the target table (delta_orders)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- See the MERGE operation in the table history
# MAGIC DESCRIBE HISTORY delta_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Optimizing File Sizes with OPTIMIZE
# MAGIC
# MAGIC Delta Lake tables can accumulate many small files over time, especially after numerous `UPDATE`, `DELETE`, or `MERGE` operations. The `OPTIMIZE` command helps maintain performance by compacting these small files into larger ones, reducing the metadata that needs to be processed during queries.
# MAGIC
# MAGIC > **Note** that Databricks automatically applies optimized writes to Delta tables by default in newer runtimes, which helps reduce small file issues proactively.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Note the value of the NumFiles column
# MAGIC DESCRIBE DETAIL delta_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize the delta_orders table to improve query performance
# MAGIC OPTIMIZE delta_orders
# MAGIC ZORDER BY (order_date, customer_id);
# MAGIC
# MAGIC -- Check optimization metrics
# MAGIC DESCRIBE HISTORY delta_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now look at the NumFiles column again
# MAGIC DESCRIBE DETAIL delta_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. Removing Older Versions using VACUUM
# MAGIC
# MAGIC Delta Lake maintains a history of changes, allowing time travel queries. However, this can consume storage space over time. The VACUUM command permanently removes files that are no longer referenced by the latest versions of the table.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- First, check the retention period (default is 7 days)
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC
# MAGIC -- Remove files no longer referenced by the Delta table and are older than the retention period
# MAGIC -- See what would be deleted without actually removing files (dry run)
# MAGIC VACUUM delta_orders RETAIN 24 HOURS DRY RUN;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now do it for real...
# MAGIC -- WARNING: This will permanently remove access to older versions
# MAGIC VACUUM delta_orders RETAIN 24 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Note that VACUUM operations appear in the log as well
# MAGIC DESCRIBE HISTORY delta_orders

# COMMAND ----------

# MAGIC %md
# MAGIC #####Databricks provides automatic VACUUM behavior:
# MAGIC - Default retention period is 7 days (168 hours)
# MAGIC - Delta tables are automatically vacuumed according to this retention period
# MAGIC - Safety checks prevent accidental removal of versions less than 7 days old
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE**: Vacuumed version may still be available after a `VACUUM` operation, this is due to **delta caching**, however don't rely on this, the version files could get evicted from delta cache at any stage (and will not longer be available after the cluster restarts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Versioning and Time Travel**
# MAGIC    - All changes create new versions
# MAGIC    - Access historical versions easily
# MAGIC    - Query data as of specific timestamps
# MAGIC
# MAGIC 2. **Data Recovery**
# MAGIC    - Restore to previous versions
# MAGIC    - Audit data changes
# MAGIC    - Track modifications
# MAGIC
# MAGIC 3. **Optimization**
# MAGIC    - Regular table optimization
# MAGIC    - Z-ORDER indexing
# MAGIC    - File compaction
# MAGIC
# MAGIC 4. **Maintenance**
# MAGIC    - Implement regular cleanup
# MAGIC    - Update statistics
# MAGIC    - Monitor table health
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
