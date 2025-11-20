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
# MAGIC # 03: Optimizing Apache Spark
# MAGIC
# MAGIC This notebook demonstrates key performance optimization techniques for Apache Spark applications using the TPC-H dataset:
# MAGIC
# MAGIC 1. Understanding Spark Performance - Resource utilization and data flow patterns
# MAGIC 2. Partitioning Strategies - DataFrame partitioning for better distribution
# MAGIC 3. Reducing Shuffle Operations - Minimizing expensive shuffle operations
# MAGIC 4. DataFrame Caching - Effective data persistence strategies
# MAGIC 5. Query Optimization - Understanding Catalyst optimizer and execution plans
# MAGIC
# MAGIC Throughout this notebook, we'll examine the Spark UI at each step to understand the impact of various optimizations.

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

# MAGIC %sql
# MAGIC USE SCHEMA mafalda_rufino;
# MAGIC
# MAGIC SELECT current_catalog(), current_schema();
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Data Setup
# MAGIC
# MAGIC First, let's copy some tables from the TPC-H sample dataset to our own catalog/schema for our optimization demonstrations and look at some configuration items.

# COMMAND ----------

# make copies of "lineitem", "orders" tables

for table in ["lineitem", "orders"]:
    print(f"Creating local copy of {table}...")
    
    # Create a local copy
    spark.table(f"samples.tpch.{table}").write.mode("overwrite").saveAsTable(table)

# COMMAND ----------

orders_df = spark.table("orders")
lineitems_df = spark.table("lineitem")

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, avg
import time

# Get cluster info for better partitioning
num_cores = sc.defaultParallelism
print(f"Default parallelism (cores): {num_cores}")

# Get shuffle partitions to 200 (Spark default)
shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
print(f"Default shuffle partitions: {shuffle_partitions}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Understanding Partitioning and Shuffling
# MAGIC
# MAGIC Let's deep dive into partitioning and understand how different operations affect the numbers or sizes of partitions and shuffling.

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. Default Partitioning
# MAGIC The default number of partitions in an input dataframe (read using the `DataFrameReader` from files or from a table) is equivalent to the number of files in the dataset

# COMMAND ----------

# Note that the default number of partitions in the dataframe is equivalent to the number of files in the dataset
print(f"Default partitions (lineitem): {lineitems_df.inputFiles().__len__()}")
display(spark.sql("DESCRIBE DETAIL lineitem").select("numFiles"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Narrow Transformations and Partition Counts
# MAGIC Narrow transformations (such as `filter`, `select`, `drop`, `withColumn`) will either retain the same number of partitions or reduce the number of partitions in the resultant dataframe, let's have a look.

# COMMAND ----------

print(f"Starting number of partitions: unsupported in shared clusters")
high_value_lineitems_df = lineitems_df.filter("l_extendedprice > 60000 AND l_linenumber < 2")
print("Number of partitions (after narrow transformation): not available in shared clusters")

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Narrow Transformations and Skew
# MAGIC Narrow transformations such as `filter` can create uneven partition sizes (as filtered records may not be distributed equally), to demonstrate this we will write out the results of our dataframe and look at the resultant file sizes.

# COMMAND ----------

def show_partition_sizes(input_dataframe):
    # Define the output directory
    output_path = f"databrickstheloop/mafalda_rufino/high_value_lineitems"

    # Remove the directory if it already exists
    dbutils.fs.rm(output_path, True)

    # Write the DataFrame as parquet files
    input_dataframe.write \
        .format("parquet") \
        .option("compression", "none") \
        .mode("overwrite") \
        .save(output_path)

    # List the files in the directory
    print("Files in the output directory:")
    files = dbutils.fs.ls(output_path)
    for i, file in enumerate([f for f in files if f.path.endswith(".parquet")]):
        print(f"Partition {i}: Size: {file.size} bytes")

show_partition_sizes(high_value_lineitems_df)

# COMMAND ----------

# To normalize skew, lets repartition
balanced_df = high_value_lineitems_df.repartition(10)
show_partition_sizes(balanced_df)

# Alternatively you could repartition by a specific column (or columns) for better data distribution
# balanced_df = high_value_lineitems_df.repartition(10, "l_orderkey")
# show_partition_sizes(balanced_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###4. Analyzing Shuffling
# MAGIC Look at the Spark UI for the job above☝️.  Notice that the above operation forced 90% of the dataset to shuffle (moving data between partitions).

# COMMAND ----------

# Use coalesce to reduce partitions without full shuffle, this may be better when you just need fewer partitions and don't mind some imbalance
# Note: Coalesce does not address skew directly however it will consolidate partitions
smaller_df = high_value_lineitems_df.coalesce(5)
show_partition_sizes(smaller_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Look at the Spark UI again now for the job above☝️.  Notice that the above operation did not result in shuffling.

# COMMAND ----------

# MAGIC %md
# MAGIC ###5. Wide Transformations and Partitioning
# MAGIC
# MAGIC Wide transformations (like `groupBy`, `join`, `repartition`) cause data to be shuffled across the network. They affect partitioning in significant ways:
# MAGIC
# MAGIC 1. They typically change the number of partitions (based on spark.sql.shuffle.partitions)
# MAGIC 2. They redistribute data across partitions based on keys
# MAGIC 3. They can create or resolve data skew depending on how they're used
# MAGIC

# COMMAND ----------

# Check partitions in original DataFrame
print(f"Original partitions in lineitems_df: {lineitems_df.rdd.getNumPartitions()}")
# Apply a groupBy (wide transformation)
grouped_lineitems = lineitems_df.groupBy("l_suppkey").count()
print(f"Partitions after groupBy: {grouped_lineitems.rdd.getNumPartitions()}")
# The resultant number of partitions is determined by AQE (adaptive query execution)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Understanding Shuffle Partitions and AQE
# MAGIC
# MAGIC Let's demonstrate the effects of the `spark.sql.shuffle.partitions` configuration setting and Adaptive Query Execution (AQE).

# COMMAND ----------

# Force disable AQE for a test
spark.conf.set("spark.sql.adaptive.enabled", "false")
grouped_lineitems_no_aqe = lineitems_df.groupBy("l_suppkey").count()
print(f"Partitions after groupBy with AQE disabled: {grouped_lineitems_no_aqe.rdd.getNumPartitions()}")

# Why 200? remember the default value of spark.sql.shuffle.partitions

# COMMAND ----------

# Let's set the number of shuffle partitions to the number of cores
spark.conf.set("spark.sql.shuffle.partitions", num_cores)
grouped_lineitems_no_aqe_updated_shuffle_parts = lineitems_df.groupBy("l_suppkey").count()
print(f"Partitions after groupBy with AQE disabled and shuffle partitions set: {grouped_lineitems_no_aqe_updated_shuffle_parts.rdd.getNumPartitions()}")

# COMMAND ----------

# Let's re-enable AQE now and re-set the number of shuffle partitions to the default
spark.conf.set("spark.sql.shuffle.partitions", 200)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Analyzing Explain Plans
# MAGIC
# MAGIC Spark's `explain()` function is a powerful tool for understanding query execution. It shows how Spark's Catalyst optimizer transforms your code into execution steps.
# MAGIC
# MAGIC Let's examine explain plans for different operations to better understand optimization opportunities:

# COMMAND ----------

# Simple query explain plan
simple_query = lineitems_df.filter("l_shipdate > '1995-01-01'").groupBy("l_shipmode").count()

print("SIMPLE QUERY EXPLAIN:")
simple_query.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC The following query has several inefficiencies as it is written, let's look at how the optimizer handles this.

# COMMAND ----------

inefficient_query = (
    lineitems_df
    # Filter applied late in the transformation
    .select("l_orderkey", "l_shipdate", "l_shipmode", "l_extendedprice", "l_discount")
    # Join before filtering (inefficient)
    .join(
        orders_df.select("o_orderkey", "o_orderdate", "o_orderpriority"),
        lineitems_df["l_orderkey"] == orders_df["o_orderkey"]
    )
    # Filters that could be pushed down before the join
    .filter(col("l_shipdate") > "1995-01-01")
    .filter(col("o_orderdate") > "1995-01-01")
    # Late filter on shipmode
    .filter(col("l_shipmode").isin("AIR", "MAIL"))
    # Calculate revenue
    .withColumn("revenue", col("l_extendedprice") * (1 - col("l_discount")))
    # Group and aggregate
    .groupBy("l_shipmode", "o_orderpriority")
    .agg(
        sum("revenue").alias("total_revenue"),
        count("*").alias("order_count")
    )
)

# COMMAND ----------

# Show the logical plan (what was written)
print("INEFFICIENT QUERY PLAN:")
inefficient_query.explain(mode="formatted")

# COMMAND ----------

# Show the optimized physical plan (what Spark will actually execute)
print("\nPHYSICAL PLAN (what Spark optimizes it to):")
inefficient_query.explain(mode="extended")

# COMMAND ----------

# MAGIC %md
# MAGIC Looking at these two execution plans, we can see how Spark optimized the inefficient query:
# MAGIC
# MAGIC 1. **Filter Pushdown**: In the optimized plan, notice how the filters were pushed down to the scan operations:
# MAGIC    ```
# MAGIC    +- PhotonScan parquet ...lineitem
# MAGIC       DictionaryFilters: [(l_shipdate#69 > 1995-01-01), l_shipmode#73 IN (AIR,MAIL)]
# MAGIC    ```
# MAGIC    Even though we placed these filters after the join in our query, Spark moved them to the earliest possible point (during the initial data read).
# MAGIC
# MAGIC 2. **Column Pruning**: The optimizer only reads the columns it actually needs:
# MAGIC    ```
# MAGIC    ReadSchema: struct<l_orderkey:bigint,l_extendedprice:decimal(18,2),l_discount:decimal(18,2),l_shipdate:date,l_shipmode:string>
# MAGIC    ```
# MAGIC    Even though we selected more columns in our initial query, Spark only reads what's necessary for the final result.
# MAGIC
# MAGIC 3. **Filter Combination**: In the optimized logical plan, you can see how multiple separate filters have been combined:
# MAGIC    ```
# MAGIC    +- Filter (((isnotnull(l_shipdate#69) AND isnotnull(l_orderkey#59L)) AND (l_shipdate#69 > 1995-01-01)) AND l_shipmode#73 IN (AIR,MAIL))
# MAGIC    ```
# MAGIC    All the filter conditions were combined into a single operation.
# MAGIC
# MAGIC 4. **Projection Optimization**: The execution only projects necessary columns at each step.
# MAGIC
# MAGIC ### The Bottom Line
# MAGIC
# MAGIC Spark's optimizer transformed this into a much more efficient plan that:
# MAGIC 1. Filters data as early as possible
# MAGIC 2. Reads only necessary columns
# MAGIC 3. Combines multiple filter conditions
# MAGIC 4. Uses a more efficient ordering of operations
# MAGIC
# MAGIC > **NOTE:** this doesn't mean you shouldn't write queries as you would expect them to be executed!

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. DataFrame Caching
# MAGIC
# MAGIC Caching can significantly improve performance for iterative operations on the same data. Let's demonstrate its impact.
# MAGIC
# MAGIC First, let's run a sequence of operations without caching:
# MAGIC
# MAGIC **👀 Spark UI Observation:** Take note of how each query has to read from the source tables repeatedly.
# MAGIC - Look at the "Input" metrics that show how much data is read
# MAGIC - Notice the full execution plan for each job
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import avg, max

# Uncached query
high_value_line_items_df = lineitems_df.filter("l_extendedprice > 100000").select(
    "l_orderkey", "l_shipdate", "l_shipmode", "l_extendedprice", "l_discount"
)
print(f"There are a total of {high_value_line_items_df.count()} high value line items")

avg_price_by_ship_mode = high_value_line_items_df.groupBy("l_shipmode").agg(
    avg("l_extendedprice").alias("avg_price")
)
print(f"Average price by ship mode (high_value_line_items_df is re-evaluated):")
display(avg_price_by_ship_mode)

max_price_by_ship_mode = high_value_line_items_df.groupBy("l_shipmode").agg(
    max("l_extendedprice").alias("max_price")
)
print(f"Max price by ship mode (high_value_line_items_df is re-evaluated again):")
display(max_price_by_ship_mode)

# COMMAND ----------

# Cached query
high_value_line_items_df =  lineitems_df.filter("l_extendedprice > 100000").select("l_orderkey", "l_shipdate", "l_shipmode", "l_extendedprice", "l_discount")
high_value_line_items_df.cache()
print(f"There are a total of {high_value_line_items_df.count()} high value line items")

avg_price_by_ship_mode = high_value_line_items_df.groupBy("l_shipmode").agg(avg("l_extendedprice").alias("avg_price"))
print(f"Average price by ship mode (high_value_line_items_df is NOT re-evaluated):")
avg_price_by_ship_mode.show()

max_price_by_ship_mode = high_value_line_items_df.groupBy("l_shipmode").agg(max("l_extendedprice").alias("max_price"))
print(f"Max price by ship mode (high_value_line_items_df is NOT re-evaluated):")
max_price_by_ship_mode.show()

# COMMAND ----------

# un-cache the dataframe
lineitems_df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up
# MAGIC
# MAGIC Let's clean up the resources we created for this demo.
# MAGIC

# COMMAND ----------

# Drop the tables we created
for table in ["lineitem", "orders"]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

print("Clean up completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Understanding Spark Performance**
# MAGIC    - Monitor resource utilization, data flow patterns, and bottlenecks using the Spark UI
# MAGIC    - Understand the impact of data size, formats, and distribution on performance
# MAGIC
# MAGIC 2. **Partitioning Strategies**
# MAGIC    - Choose high-cardinality columns for even distribution
# MAGIC    - Target 100-200MB per partition as a general guideline
# MAGIC    - Use `repartition()` and `coalesce()` to control partition count
# MAGIC
# MAGIC 3. **Reducing Shuffle Operations**
# MAGIC    - Filter early to reduce data volume
# MAGIC    - Use broadcast joins for small tables
# MAGIC    - Maintain consistent partitioning where possible
# MAGIC    - Monitor shuffle spill metrics (memory vs disk)
# MAGIC
# MAGIC 4. **DataFrame Caching**
# MAGIC    - Cache DataFrames that are reused in multiple operations
# MAGIC    - Use `cache()` or `persist()` explicitly
# MAGIC    - Remember to `unpersist()` when data is no longer needed
# MAGIC    
# MAGIC 5. **Query Optimization**
# MAGIC    - Understand Catalyst optimizer and execution plans with `explain()`
# MAGIC    - Leverage predicate pushdown and column pruning
# MAGIC    - Use the appropriate join strategy for your data
# MAGIC
# MAGIC 6. **Predictive Optimization with Delta Lake**
# MAGIC    - Delta Lake's **Predictive Optimization** automatically leverages your `OPTIMIZE` and Z-ORDER operations
# MAGIC    - When you run queries, the query optimizer automatically considers your Z-ORDER indexes
# MAGIC    - Benefits:
# MAGIC      - No need to explicitly hint which indexes to use in your queries
# MAGIC      - The system automatically prunes files based on Z-ORDER columns
# MAGIC      - Queries are automatically optimized for better performance
# MAGIC      - Reduces I/O by skipping files that don't contain relevant data
# MAGIC    - Example: If you've run `OPTIMIZE table ZORDER BY (date_col, region)`:
# MAGIC      - A query with `WHERE date_col = '2023-01-01' AND region = 'APAC'` automatically benefits
# MAGIC      - The optimizer uses Z-ORDER statistics to read only relevant files
# MAGIC      - This happens transparently without any special query modifications
# MAGIC
# MAGIC 7. **Best Practices**
# MAGIC    - Use the Spark UI to monitor performance
# MAGIC    - Filter early and select only needed columns
# MAGIC    - Optimize join operations and join order
# MAGIC    - Minimize UDFs in favor of built-in functions
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
