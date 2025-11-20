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
# MAGIC # 1 - Introduction to Spark Structured Streaming
# MAGIC
# MAGIC This notebook demonstrates key concepts of Structured Streaming using practical examples with IoT sensor data.
# MAGIC
# MAGIC ### Objectives
# MAGIC - Understand stream processing fundamentals
# MAGIC - Work with different streaming sources and sinks
# MAGIC - Implement streaming transformations
# MAGIC - Use watermarking and windowing
# MAGIC - Monitor streaming queries

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
# MAGIC ## A. Stream Processing Setup
# MAGIC
# MAGIC First, let's set up our streaming infrastructure and define our schema.

# COMMAND ----------

# Import necessary libraries if not already imported
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define the schema for the stream
schema = StructType([
    StructField("customer_id", LongType(), True),
    StructField("notifications", StringType(), True),
    StructField("order_id", LongType(), True),
    StructField("order_timestamp", LongType(), True)
])

# Now use this schema for your streaming DataFrame
stream_df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .option("path", "/Volumes/dbacademy_retail/v01/retail-pipeline/orders/stream_json") \
    .load()

# COMMAND ----------

# Confirm we have set up a streaming dataframe
print(f"isStreaming: {stream_df.isStreaming}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Running Basic Streaming Queries
# MAGIC
# MAGIC Now let's kick off some streaming queries.

# COMMAND ----------

# Display the stream for testing, display will start an implicit query (this is analogous to the console sink)
display(stream_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C: Basic Transformations on the Stream
# MAGIC Stateless streaming transformations are analogous to narrow transformations we would perform on normal DataFrames (`select`, `filter`, `withColumn`, etc).

# COMMAND ----------

# Simple transformations using standard DataFrame operations
transformed_stream = stream_df \
    .withColumn("notification_status", col("notifications").isNotNull()) \
    .withColumn("order_details", concat(lit("Order #"), col("order_id").cast("string")))

# Display the transformed stream
display(transformed_stream)

# COMMAND ----------

# Filter for only orders with notifications enabled
notifications_stream = stream_df \
    .filter(col("notifications") == "Y")

# Display filtered stream
display(notifications_stream)

# COMMAND ----------

# Stop any existing queries with the same name
for q in spark.streams.active:
    if q.name == "orders_streaming_table":
        q.stop()

# Write to memory sink for interactive querying
memory_query = stream_df.writeStream \
    .format("memory") \
    .queryName("orders_streaming_table") \
    .outputMode("append") \
    .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now you can query the in-memory table using SQL
# MAGIC SELECT notifications, count(*) as num_notifications 
# MAGIC FROM orders_streaming_table GROUP BY notifications

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Combining Multiple Streams using Union
# MAGIC
# MAGIC Different stream sources can be combined using relational operators like `union`.  Let's have a look.
# MAGIC
# MAGIC > **NOTE:** Stream-to-static DataFrame joins are fully supported and easy to implement (for example joining a stream with reference or static lookup data for enrichment). Stream-to-stream joins are supported as well, but require special handling for state management.

# COMMAND ----------

# Create a second stream with a subset of the data
filtered_stream1 = stream_df.filter(col("notifications") == "Y")
filtered_stream2 = stream_df.filter(col("notifications") == "N")

# Union the streams
combined_stream = filtered_stream1.union(filtered_stream2)

# Process the combined stream
display(combined_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Using Triggers to Control Processing
# MAGIC
# MAGIC Triggers control how long a batch window is, let's show an example.
# MAGIC

# COMMAND ----------

# Stop any existing queries with the same name
for q in spark.streams.active:
    if q.name == "triggered_query_table":
        q.stop()

# Process data in micro-batches every 10 seconds
triggered_query = stream_df \
    .withColumn("processing_ts", current_timestamp()) \
    .writeStream \
    .format("memory") \
    .queryName("triggered_query_table") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT processing_ts, count(*) as count 
# MAGIC FROM triggered_query_table 
# MAGIC GROUP BY processing_ts
# MAGIC ORDER BY processing_ts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Stream Processing Fundamentals**:
# MAGIC    - Continuous data processing
# MAGIC    - Schema definition
# MAGIC    - Basic transformations
# MAGIC
# MAGIC 2. **Sources and Sinks**:
# MAGIC    - Rate source for testing
# MAGIC    - Console sink for debugging
# MAGIC    - Memory sink for monitoring
# MAGIC
# MAGIC 4. **Monitoring and Management**:
# MAGIC    - Query monitoring
# MAGIC    - Progress tracking
# MAGIC    - Resource management
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to stop the active streaming queries.

# COMMAND ----------

for query in spark.streams.active:
    query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
