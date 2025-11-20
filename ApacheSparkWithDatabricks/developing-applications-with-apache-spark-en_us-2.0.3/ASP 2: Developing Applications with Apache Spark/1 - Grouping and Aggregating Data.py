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
# MAGIC # 01 - Grouping and Aggregating Data
# MAGIC
# MAGIC This demonstration will show how to perform grouping and aggregation operations using NYC Taxi trip data. We'll explore basic grouping, multiple aggregations, and window functions.
# MAGIC
# MAGIC ### Objectives
# MAGIC - Understand basic grouping operations in Spark
# MAGIC - Perform time-based analysis using aggregations
# MAGIC - Implement complex aggregations with multiple metrics
# MAGIC - Use window functions for advanced analytics
# MAGIC - Optimize aggregation performance

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
# MAGIC ## A. Data Setup and Loading
# MAGIC
# MAGIC First, let's load our taxi trip data and examine its structure.

# COMMAND ----------

from pyspark.sql.functions import *

# Read and displaying the taxi data
trips_df = spark.read.table("samples.nyctaxi.trips")

display(trips_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Basic Grouping Operations
# MAGIC
# MAGIC Let's start with simple grouping operations to understand trip patterns by location.

# COMMAND ----------

# Count trips by pickup location, to show top 5 most popular pickup locations
location_counts = trips_df \
    .groupBy("pickup_zip") \
    .count() \
    .orderBy(desc("count"))

display(location_counts.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Combining Multiple Aggregations
# MAGIC
# MAGIC Let's perform multiple aggregations by location using the `agg()` method

# COMMAND ----------

# Perform multiple aggregations by location, order by most popular pickup locations
location_stats = trips_df \
    .groupBy("pickup_zip") \
    .agg(
        count("*").alias("total_trips"),
        round(avg("trip_distance"), 2).alias("avg_distance"),
        round(avg("fare_amount"), 2).alias("avg_fare"),
        round(sum("fare_amount"), 2).alias("total_fare_amt")
    ) \
    .orderBy(desc("total_trips"))

display(location_stats.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Window Functions
# MAGIC
# MAGIC Now let's use window functions for more advanced analytics.

# COMMAND ----------

from pyspark.sql.window import Window

# Create window specs for different ranking methods
window_by_trips = Window.orderBy(desc("total_trips"))
window_by_fare = Window.orderBy(desc("avg_fare"))

# Add different types of rankings
ranked_locations = location_stats \
    .withColumn("trips_rank", rank().over(window_by_trips)) \
    .withColumn("fare_rank", rank().over(window_by_fare)) \
    .withColumn("fare_quintile", ntile(5).over(window_by_fare))  # Divide into 5 groups by fare

# COMMAND ----------

ranked_locations.createOrReplaceTempView("ranked_locations")

# COMMAND ----------

# MAGIC %sql
# MAGIC select fare_quintile,min(avg_fare),max(avg_fare),count(*) as cnt_per_group from ranked_locations group by fare_quintile

# COMMAND ----------

# Displaying the results
display(ranked_locations.select(
    "pickup_zip", 
    "total_trips", 
    "avg_fare", 
    "avg_distance",
    "trips_rank",
    "fare_rank",
    "fare_quintile"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Basic Grouping**
# MAGIC    - Use `groupBy()` followed by aggregation method
# MAGIC    - Can group by multiple columns
# MAGIC    - Always check data distribution
# MAGIC
# MAGIC 2. **Window Functions**
# MAGIC    - Perfect for comparative analytics
# MAGIC    - Consider performance impact
# MAGIC    - Use appropriate window frame
# MAGIC
# MAGIC 3. **Best Practices**
# MAGIC    - Always alias aggregated columns
# MAGIC    - Handle null values appropriately
# MAGIC    - Consider data skew in grouping keys

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
