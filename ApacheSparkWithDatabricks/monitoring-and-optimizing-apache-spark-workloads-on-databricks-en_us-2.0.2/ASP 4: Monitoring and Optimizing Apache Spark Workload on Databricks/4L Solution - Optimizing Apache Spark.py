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
# MAGIC # 4L: Optimizing Apache Spark
# MAGIC
# MAGIC In this lab, you'll apply the Spark optimization techniques you've learned to a real-world dataset - the Airline Performance dataset.
# MAGIC
# MAGIC ### Objectives
# MAGIC - Analyze and understand dataset partitioning
# MAGIC - Implement proper partition strategies
# MAGIC - Use caching effectively to improve performance
# MAGIC - Analyze execution plans and measure performance improvements
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
# MAGIC ## A. Data Setup
# MAGIC
# MAGIC Let's begin by loading the data and understanding our environment.

# COMMAND ----------

## Import necessary libraries
from pyspark.sql.functions import col, count, avg, sum, max
import time

## Load the dataset
flights_df = spark.read.table("dbacademy_airline.v01.flights")

num_cores = sc.defaultParallelism
print(f"Default parallelism (cores): {num_cores}")

shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
print(f"Default shuffle partitions: {shuffle_partitions}")

# COMMAND ----------

flights_df.printSchema()
display(flights_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Understanding the Current Partitioning
# MAGIC
# MAGIC Before applying optimizations, it's important to understand the current state of our data.

# COMMAND ----------

current_partitions = flights_df.rdd.getNumPartitions()
print(f"Current number of partitions: {current_partitions}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Optimizing Partitions
# MAGIC
# MAGIC Given skewed data, create an optimally distributed dataset.

# COMMAND ----------

# The following narrow transformation will introduce skew in the dataset
filtered_flights_df = flights_df.filter(
    (col("ArrDelay") > 30) & 
    col("UniqueCarrier").isin(["UA", "AA", "DL"]) &
    (col("year") == 2008)
)

# COMMAND ----------

filtered_flights_df = filtered_flights_df.repartition(10)
current_partitions = filtered_flights_df.rdd.getNumPartitions()
print(f"Current number of partitions: {current_partitions}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Caching Strategies
# MAGIC
# MAGIC Apply caching techniques and measure performance impact.  Optimize the following query.

# COMMAND ----------

print(f"There are a total of {filtered_flights_df.count()} filtered flights")

display(
    filtered_flights_df.groupBy("UniqueCarrier").agg(count("UniqueCarrier").alias("count"))
)

display(
    filtered_flights_df.groupBy("UniqueCarrier").agg(avg("ArrDelay").alias("avg_delay"))
)

# COMMAND ----------

filtered_flights_df.cache()

print(f"There are a total of {filtered_flights_df.count()} filtered flights")

display(
    filtered_flights_df.groupBy("UniqueCarrier").agg(count("UniqueCarrier").alias("count"))
)

display(
    filtered_flights_df.groupBy("year").agg(count("year").alias("count"))
)

# COMMAND ----------

filtered_flights_df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
