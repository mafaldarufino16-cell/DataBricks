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
# MAGIC # 03 - Window Aggregation in Spark Structured Streaming
# MAGIC
# MAGIC This notebook demonstrates advanced concepts of Structured Streaming including stateful operations, state management, streaming joins, and window operations.
# MAGIC
# MAGIC ### Objectives
# MAGIC - Understand stateful vs stateless operations
# MAGIC - Implement windowed operations
# MAGIC - Perform streaming joins
# MAGIC - Work with late arriving data using watermarks

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
# MAGIC ## A. Setup and Data Sources
# MAGIC
# MAGIC First, let's create two streaming DataFrames that we'll use throughout our demo:
# MAGIC 1. An **orders stream** containing customer orders
# MAGIC 2. A **status stream** containing order status updates
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Define schema for orders
orders_schema = StructType([
    StructField("customer_id", LongType(), True),
    StructField("notifications", StringType(), True),
    StructField("order_id", LongType(), True),
    StructField("order_timestamp", LongType(), True)
])

# Define schema for status updates
status_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("order_status", StringType(), True),
    StructField("status_timestamp", LongType(), True)
])

# Create orders streaming DataFrame
orders_stream = spark.readStream \
    .format("json") \
    .schema(orders_schema) \
    .option("maxFilesPerTrigger", 1) \
    .option("path", "/Volumes/dbacademy_retail/v01/retail-pipeline/orders/stream_json") \
    .load()

# Create status streaming DataFrame
status_stream = spark.readStream \
    .format("json") \
    .schema(status_schema) \
    .option("maxFilesPerTrigger", 1) \
    .option("path", "/Volumes/dbacademy_retail/v01/retail-pipeline/status/stream_json") \
    .load()

# Verify both are streaming DataFrames
print(f"orders_stream is streaming: {orders_stream.isStreaming}")
print(f"status_stream is streaming: {status_stream.isStreaming}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Stateless vs Stateful Operations
# MAGIC
# MAGIC Let's look at the difference between stateless and stateful operations:
# MAGIC
# MAGIC - **Stateless operations**: Process each record independently (e.g., `select`, `filter`)
# MAGIC - **Stateful operations**: Maintain information across batches (e.g., `groupBy`, `join`)

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. Stateless Operation Example
# MAGIC Let's apply some simple stateless transformations to our streams:
# MAGIC

# COMMAND ----------

display(orders_stream)

# COMMAND ----------

display(status_stream)

# COMMAND ----------

# Convert timestamps to a more usable format (stateless operation)
orders_transformed = orders_stream \
    .withColumn("order_time", from_unixtime(col("order_timestamp")).cast("timestamp")) \
    .withColumn("notification_enabled", col("notifications") == "Y")

# Display the transformed stream
display(orders_transformed)

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Stateful Operation Example
# MAGIC Now let's perform some stateful operations that maintain state across batches:

# COMMAND ----------

# Count orders by status (stateful aggregation)
status_counts = status_stream \
    .groupBy("order_status") \
    .count() \
    .orderBy(col("count").desc())

display(status_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Window Operations
# MAGIC
# MAGIC Window operations allow us to perform aggregations over time windows. We'll demonstrate:
# MAGIC - Tumbling Windows (fixed, non-overlapping)
# MAGIC - Sliding Windows (overlapping windows)

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. Tumbling Window Example
# MAGIC
# MAGIC Let's count orders per 1-minute tumbling window:

# COMMAND ----------

# First, make sure to clean up previous streams with same name
for query in spark.streams.active:
    if query.name == "tumbling_window_counts":
        query.stop()

# Prepare data by ensuring we have a proper timestamp column
status_events = status_stream \
    .withColumn("event_time", from_unixtime(col("status_timestamp")).cast("timestamp"))

# Group by status and 1-minute tumbling windows
tumbling_windows = status_events \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("order_status")
    ) \
    .count()

# Write to memory for visualization
tumbling_window_query = tumbling_windows.writeStream \
    .format("memory") \
    .outputMode("complete") \
    .queryName("tumbling_window_counts") \
    .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the tumbling window results
# MAGIC SELECT 
# MAGIC   window.start as window_start,
# MAGIC   window.end as window_end,
# MAGIC   order_status,
# MAGIC   count
# MAGIC FROM tumbling_window_counts
# MAGIC ORDER BY window_start, order_status

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Sliding Window Example
# MAGIC Now let's count orders per 2-minute window, sliding every 1 minute:
# MAGIC

# COMMAND ----------

# Stop any existing queries with the same name
for query in spark.streams.active:
    if query.name == "sliding_window_counts":
        query.stop()

# Group by status and sliding window
sliding_windows = status_events \
    .groupBy(
        window(col("event_time"), "2 minutes", "1 minute"),
        col("order_status")
    ) \
    .count()

# Write to memory for visualization
sliding_window_query = sliding_windows.writeStream \
    .format("memory") \
    .outputMode("complete") \
    .queryName("sliding_window_counts") \
    .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the sliding window results
# MAGIC SELECT 
# MAGIC   window.start as window_start,
# MAGIC   window.end as window_end,
# MAGIC   order_status,
# MAGIC   count
# MAGIC FROM sliding_window_counts
# MAGIC ORDER BY window_start, order_status

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Streaming Joins
# MAGIC
# MAGIC Let's demonstrate joining our streaming order data with status updates.

# COMMAND ----------

# Prepare our streaming DataFrames with proper timestamps
orders_with_time = orders_stream \
    .withColumn("order_time", from_unixtime(col("order_timestamp")).cast("timestamp"))

status_with_time = status_stream \
    .withColumn("status_time", from_unixtime(col("status_timestamp")).cast("timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. Stream-Static Join
# MAGIC First, let's create a static DataFrame for lookup purposes.
# MAGIC

# COMMAND ----------

# Create a static lookup table for order status descriptions
status_lookup = spark.createDataFrame([
    ("placed", "Order has been placed"),
    ("preparing", "Order is being prepared"),
    ("on the way", "Order is in transit"),
    ("delivered", "Order has been delivered"),
    ("cancelled", "Order has been cancelled")
], ["order_status", "status_description"])

# COMMAND ----------

# Join streaming status data with static status descriptions
enriched_status = status_with_time \
    .join(status_lookup, "order_status")

# Display the joined stream
display(enriched_status)

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Stream-Stream Join
# MAGIC Now let's join our two streaming DataFrames.

# COMMAND ----------

# Stop any existing queries
for query in spark.streams.active:
    if query.name == "order_status_join":
        query.stop()

# Join order stream with status stream on order_id
# Note: We need to limit state buildup for production use
order_status_join = orders_with_time \
    .join(
        status_with_time,
        "order_id"
    )

# Write to memory sink
order_status_join_query = order_status_join.writeStream \
    .format("memory") \
    .outputMode("append") \
    .queryName("order_status_join") \
    .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the joined data
# MAGIC SELECT 
# MAGIC   order_id, 
# MAGIC   customer_id, 
# MAGIC   order_status,
# MAGIC   notifications,
# MAGIC   order_time,
# MAGIC   status_time
# MAGIC FROM order_status_join
# MAGIC LIMIT 20
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Handling Late Data with Watermarks
# MAGIC Watermarks help us handle late-arriving data by defining how long to wait for late events.
# MAGIC

# COMMAND ----------

# Stop any existing queries
for query in spark.streams.active:
    if query.name == "windowed_with_watermark":
        query.stop()

# Add watermark to status events
status_with_watermark = status_events \
    .withWatermark("event_time", "10 minutes")

# Windows with watermark
watermarked_windows = status_with_watermark \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("order_status")
    ) \
    .count()

# Write to memory
query5 = watermarked_windows.writeStream \
    .format("memory") \
    .outputMode("complete") \
    .queryName("windowed_with_watermark") \
    .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the windowed data with watermark
# MAGIC SELECT 
# MAGIC   window.start as window_start,
# MAGIC   window.end as window_end,
# MAGIC   order_status,
# MAGIC   count
# MAGIC FROM windowed_with_watermark
# MAGIC ORDER BY window_start, order_status

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to stop the active streaming queries.

# COMMAND ----------

for query in spark.streams.active:
    query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
