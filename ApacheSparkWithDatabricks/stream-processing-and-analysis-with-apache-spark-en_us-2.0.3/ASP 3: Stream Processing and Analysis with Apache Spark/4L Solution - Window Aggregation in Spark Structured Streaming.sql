-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img
-- MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
-- MAGIC     alt="Databricks Learning"
-- MAGIC   >
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 4L - Window Aggregation in Spark Structured Streaming
-- MAGIC
-- MAGIC In this lab, you'll work with stateful operations, sliding windows, and watermarks in Spark Structured Streaming. You'll analyze streams of order and status data to derive meaningful insights.
-- MAGIC
-- MAGIC ### Objectives
-- MAGIC - Implement stateful aggregations and window operations
-- MAGIC - Handle late data and state management
-- MAGIC - Build real-time monitoring systems

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
-- MAGIC
-- MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
-- MAGIC
-- MAGIC Follow these steps to select the classic compute cluster:
-- MAGIC
-- MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
-- MAGIC
-- MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
-- MAGIC
-- MAGIC     - In the drop-down, select **More**.
-- MAGIC
-- MAGIC     - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
-- MAGIC
-- MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
-- MAGIC
-- MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
-- MAGIC
-- MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
-- MAGIC
-- MAGIC 1. Wait a few minutes for the cluster to start.
-- MAGIC
-- MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A. Setup and Data Sources
-- MAGIC
-- MAGIC First, let's set up our streaming environment with the necessary data sources.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC from pyspark.sql.window import Window
-- MAGIC
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC
-- MAGIC # Schemas are provided for you
-- MAGIC orders_schema = StructType([
-- MAGIC     StructField("customer_id", LongType(), True),
-- MAGIC     StructField("notifications", StringType(), True),
-- MAGIC     StructField("order_id", LongType(), True),
-- MAGIC     StructField("order_timestamp", LongType(), True)
-- MAGIC ])
-- MAGIC
-- MAGIC status_schema = StructType([
-- MAGIC     StructField("order_id", LongType(), True),
-- MAGIC     StructField("order_status", StringType(), True),
-- MAGIC     StructField("status_timestamp", LongType(), True)
-- MAGIC ])
-- MAGIC
-- MAGIC # Create status streaming DataFrame
-- MAGIC status_stream = spark.readStream \
-- MAGIC     .format("json") \
-- MAGIC     .schema(status_schema) \
-- MAGIC     .option("maxFilesPerTrigger", 1) \
-- MAGIC     .option("path", "/Volumes/dbacademy_retail/v01/retail-pipeline/status/stream_json") \
-- MAGIC     .load()
-- MAGIC
-- MAGIC # Create orders streaming DataFrame
-- MAGIC orders_stream = spark.readStream \
-- MAGIC     .format("json") \
-- MAGIC     .schema(orders_schema) \
-- MAGIC     .option("maxFilesPerTrigger", 1) \
-- MAGIC     .option("path", "/Volumes/dbacademy_retail/v01/retail-pipeline/orders/stream_json") \
-- MAGIC     .load()
-- MAGIC
-- MAGIC # Add event_time column to status stream
-- MAGIC status_events = status_stream \
-- MAGIC     .withColumn("event_time", from_unixtime(col("status_timestamp")).cast("timestamp"))
-- MAGIC
-- MAGIC # Verify streams are set up correctly
-- MAGIC print(f"orders_stream is streaming: {orders_stream.isStreaming}")
-- MAGIC print(f"status_stream is streaming: {status_stream.isStreaming}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Stateful Operations
-- MAGIC
-- MAGIC Let's explore stateful operations that maintain state across micro-batches.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # First, clean up any existing queries with the same names
-- MAGIC for query in spark.streams.active:
-- MAGIC     if query.name in ["status_counts", "customer_counts"]:
-- MAGIC         query.stop()
-- MAGIC
-- MAGIC # Count orders by status (stateful aggregation)
-- MAGIC status_counts = status_stream \
-- MAGIC     .groupBy("order_status") \
-- MAGIC     .count() \
-- MAGIC     .orderBy(col("count").desc())
-- MAGIC
-- MAGIC # Count orders by customer (stateful aggregation)
-- MAGIC customer_counts = orders_stream \
-- MAGIC     .groupBy("customer_id") \
-- MAGIC     .count() \
-- MAGIC     .orderBy(col("count").desc())
-- MAGIC
-- MAGIC # Write status counts to memory
-- MAGIC status_query = status_counts.writeStream \
-- MAGIC     .format("memory") \
-- MAGIC     .outputMode("complete") \
-- MAGIC     .queryName("status_counts") \
-- MAGIC     .start()
-- MAGIC
-- MAGIC # Write customer counts to memory
-- MAGIC customer_query = customer_counts.writeStream \
-- MAGIC     .format("memory") \
-- MAGIC     .outputMode("complete") \
-- MAGIC     .queryName("customer_counts") \
-- MAGIC     .start()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now you can query these tables to see the results:

-- COMMAND ----------

-- Query the in-memory table to see status counts
SELECT * FROM status_counts

-- COMMAND ----------

-- Query the in-memory table to see customer counts
select * from customer_counts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Sliding Window Operations
-- MAGIC In this section, you'll implement sliding window aggregations on the streaming data.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # First, clean up any existing queries with the same name
-- MAGIC for query in spark.streams.active:
-- MAGIC     if query.name == "sliding_windows":
-- MAGIC         query.stop()
-- MAGIC
-- MAGIC # Create sliding window aggregation
-- MAGIC sliding_window_counts = status_events \
-- MAGIC     .groupBy(
-- MAGIC         window(col("event_time"), "3 minutes", "1 minute"),
-- MAGIC         col("order_status")
-- MAGIC     ) \
-- MAGIC     .count()
-- MAGIC
-- MAGIC # Write sliding window counts to memory
-- MAGIC sliding_window_query = sliding_window_counts.writeStream \
-- MAGIC     .format("memory") \
-- MAGIC     .outputMode("complete") \
-- MAGIC     .queryName("sliding_windows") \
-- MAGIC     .start()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can query the sliding window results:

-- COMMAND ----------

-- Query the sliding window results
SELECT 
  window.start as window_start,
  window.end as window_end,
  order_status,
  count
FROM sliding_windows
ORDER BY window_start, order_status

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Late Data Handling with Watermarks
-- MAGIC Now, let's explore how to handle late-arriving data using watermarks.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # First, clean up any existing queries with the same names
-- MAGIC for query in spark.streams.active:
-- MAGIC     if query.name in ["windowed_with_watermark", "joined_with_watermark"]:
-- MAGIC         query.stop()
-- MAGIC
-- MAGIC # Add watermark to status events
-- MAGIC status_with_watermark = status_events \
-- MAGIC     .withWatermark("event_time", "5 minutes")
-- MAGIC
-- MAGIC # Windows with watermark
-- MAGIC watermarked_windows = status_with_watermark \
-- MAGIC     .groupBy(
-- MAGIC         window(col("event_time"), "3 minutes", "1 minute"),
-- MAGIC         col("order_status")
-- MAGIC     ) \
-- MAGIC     .count()
-- MAGIC
-- MAGIC # Write to memory
-- MAGIC watermark_query = watermarked_windows.writeStream \
-- MAGIC     .format("memory") \
-- MAGIC     .outputMode("complete") \
-- MAGIC     .queryName("windowed_with_watermark") \
-- MAGIC     .start()
-- MAGIC
-- MAGIC # Prepare orders stream with event_time for join
-- MAGIC orders_with_time = orders_stream \
-- MAGIC     .withColumn("order_time", from_unixtime(col("order_timestamp")).cast("timestamp")) \
-- MAGIC     .withWatermark("order_time", "5 minutes")
-- MAGIC
-- MAGIC # Join with watermarks to limit state
-- MAGIC watermarked_join = orders_with_time \
-- MAGIC     .join(
-- MAGIC         status_with_watermark,
-- MAGIC         "order_id",
-- MAGIC         "inner"
-- MAGIC     )
-- MAGIC
-- MAGIC # Write to memory
-- MAGIC join_query = watermarked_join.writeStream \
-- MAGIC     .format("memory") \
-- MAGIC     .outputMode("append") \
-- MAGIC     .queryName("joined_with_watermark") \
-- MAGIC     .start()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Query the results:

-- COMMAND ----------

SELECT 
  window.start as window_start,
  window.end as window_end,
  order_status,
  count
FROM windowed_with_watermark
ORDER BY window_start, order_status

-- COMMAND ----------

SELECT 
  order_id, 
  customer_id, 
  order_status,
  notifications,
  order_time,
  event_time as status_time
FROM joined_with_watermark
LIMIT 20

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the cell below to stop the active streaming queries.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for query in spark.streams.active:
-- MAGIC     query.stop()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
