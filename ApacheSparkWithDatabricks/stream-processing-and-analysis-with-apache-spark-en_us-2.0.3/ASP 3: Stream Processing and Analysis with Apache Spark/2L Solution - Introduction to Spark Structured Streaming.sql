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
-- MAGIC # 2L - Introduction to Spark Structured Streaming
-- MAGIC
-- MAGIC In this lab, you'll work with a streaming dataset containing order status updates. You'll learn how to create streaming DataFrames, perform basic transformations, and work with different streaming sinks.
-- MAGIC
-- MAGIC ### Objectives
-- MAGIC - Understand stream processing fundamentals
-- MAGIC - Implement basic streaming operations
-- MAGIC - Work with different streaming sources and sinks
-- MAGIC - Apply streaming transformations and watermarking
-- MAGIC - Handle late data and monitor streaming queries

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
-- MAGIC ## A. Stream Processing Setup
-- MAGIC
-- MAGIC First, let's set up our streaming infrastructure and define our data schema.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import *
-- MAGIC from pyspark.sql.functions import *
-- MAGIC # Define the schema
-- MAGIC schema = StructType([
-- MAGIC     StructField("order_id", LongType(), True),
-- MAGIC     StructField("order_status", StringType(), True),
-- MAGIC     StructField("status_timestamp", LongType(), True)
-- MAGIC ])
-- MAGIC
-- MAGIC # Create a streaming DataFrame using this schema
-- MAGIC status_stream = spark.readStream \
-- MAGIC     .format("json") \
-- MAGIC     .schema(schema) \
-- MAGIC     .option("maxFilesPerTrigger", 1) \
-- MAGIC     .option("path", "/Volumes/dbacademy_retail/v01/retail-pipeline/status/stream_json") \
-- MAGIC     .load()
-- MAGIC
-- MAGIC # Verify it's a streaming DataFrame
-- MAGIC print(f"isStreaming: {status_stream.isStreaming}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Streaming Queries
-- MAGIC
-- MAGIC Now we will create a basic streaming query, using the memory sink which we will subsequently query using SQL.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Write the results of your status_stream into a memory sink with a query name of "order_status_streaming_table", appending records to the output sink
-- MAGIC # Stop any existing queries with the same name
-- MAGIC
-- MAGIC for q in spark.streams.active:
-- MAGIC     if q.name == "order_status_streaming_table":
-- MAGIC         q.stop()
-- MAGIC
-- MAGIC # Write to memory sink for interactive querying
-- MAGIC memory_query = status_stream.writeStream \
-- MAGIC     .format("memory") \
-- MAGIC     .queryName("order_status_streaming_table") \
-- MAGIC     .outputMode("append") \
-- MAGIC     .start()

-- COMMAND ----------

SELECT order_status, count(*) as cnt 
FROM order_status_streaming_table
GROUP BY order_status

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Basic Transformations
-- MAGIC
-- MAGIC Now, you'll perform some basic transformations on the streaming data.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Perform basic transformations
-- MAGIC transformed_stream = status_stream \
-- MAGIC     .withColumn("event_time", from_unixtime(col("status_timestamp") / 1000).cast("timestamp")) \
-- MAGIC     .withColumn("status_description", concat(lit("Order status: "), col("order_status"))) \
-- MAGIC     .withColumn("is_completed", col("order_status").isin("delivered", "canceled"))
-- MAGIC
-- MAGIC # Display the transformed stream
-- MAGIC display(transformed_stream)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Controlling Processing with Triggers
-- MAGIC
-- MAGIC Finally, you'll use triggers to control how the stream processes data.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Stop any existing queries with the same name
-- MAGIC for q in spark.streams.active:
-- MAGIC     if q.name == "triggered_status_updates":
-- MAGIC         q.stop()
-- MAGIC         
-- MAGIC # Create a triggered streaming query
-- MAGIC triggered_query = status_stream \
-- MAGIC     .withColumn("processing_time", current_timestamp()) \
-- MAGIC     .writeStream \
-- MAGIC     .format("memory") \
-- MAGIC     .queryName("triggered_status_updates") \
-- MAGIC     .outputMode("append") \
-- MAGIC     .trigger(processingTime="15 seconds") \
-- MAGIC     .start()

-- COMMAND ----------


-- Check the processing batches
SELECT 
  processing_time,
  order_status,
  count(*) as record_count
FROM triggered_status_updates
GROUP BY processing_time, order_status
ORDER BY processing_time, order_status

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Key Takeaways
-- MAGIC
-- MAGIC 1. **Stream Processing Fundamentals**
-- MAGIC    - Structured Streaming provides a DataFrame-based streaming API
-- MAGIC    - Supports both batch and streaming processing models
-- MAGIC    - Handles data consistency and fault tolerance
-- MAGIC
-- MAGIC 2. **Sources and Sinks**
-- MAGIC    - Multiple input sources available (Rate, File, Kafka, etc.)
-- MAGIC    - Various output sinks for different use cases
-- MAGIC    - Memory sink useful for testing and debugging
-- MAGIC
-- MAGIC 3. **Data Processing**
-- MAGIC    - Supports standard DataFrame operations
-- MAGIC    - Windowing and watermarking for time-based processing
-- MAGIC    - Aggregations and streaming joins
-- MAGIC
-- MAGIC 4. **Monitoring and Management**
-- MAGIC    - Built-in query monitoring capabilities
-- MAGIC    - Progress tracking and metrics
-- MAGIC    - Late data handling strategies
-- MAGIC

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
