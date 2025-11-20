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
# MAGIC # 2L - Grouping and Aggregating E-Commerce Data
# MAGIC
# MAGIC In this lab, you'll practice working with grouping and aggregation in Spark using a dataset of e-commerce transactions. You'll perform various analyses to uncover patterns and insights in customer purchasing behavior.
# MAGIC
# MAGIC ### Objectives
# MAGIC - Use `groupBy` operations to summarize data
# MAGIC - Implement multiple aggregations
# MAGIC - Apply different ordering techniques
# MAGIC - (Bonus) Use window functions for advanced analytics

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
# MAGIC ## A. Initial Setup
# MAGIC
# MAGIC Load the retail transactions data and examine its structure.

# COMMAND ----------

from pyspark.sql.functions import *

## Read the e-commerce transactions data
transactions_df = spark.read.table("samples.bakehouse.sales_transactions")

## display a sample of the data
transactions_df.printSchema()

display(transactions_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Basic Grouping Operations
# MAGIC
# MAGIC Let's start with simple grouping operations to understand product sales patterns.

# COMMAND ----------

# 1. Group the data by products and count the number of sales
# 2. Order the results by the most popular products

# COMMAND ----------

## Count transactions by product
product_counts = transactions_df \
    .groupBy("product") \
    .count() \
    .orderBy(desc("count"))

display(product_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Multiple Aggregations
# MAGIC
# MAGIC Now let's perform multiple aggregations to get deeper insights.

# COMMAND ----------

# 1. Analyze sales by payment method
# 2. Calculate the total revenue, average transaction value, and count of transactions for each payment method
# 3. Order by total revenue (highest first)

# COMMAND ----------

## Analyze sales by payment method
payment_analysis = transactions_df \
    .groupBy("paymentMethod") \
    .agg(
        round(sum(col("totalPrice")), 2).alias("total_revenue"),
        round(avg(col("totalPrice")), 2).alias("avg_transaction_value"),
        count("*").alias("transaction_count")
    ) \
    .orderBy(desc("total_revenue"))

display(payment_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenge: Window Functions
# MAGIC
# MAGIC If you have time, try using window functions for advanced analytics.

# COMMAND ----------


## First, calculate total revenue by product and 
product_revenue_df = transactions_df \
    .groupBy("product") \
    .agg(
        round(sum(col("totalPrice")), 2).alias("total_revenue")
    )

## Usnig window functions to add rankings
## Ranking products by total revenue

from pyspark.sql.window import Window

## Create window spec for ranking categories
window_by_revenue = Window.orderBy(desc("total_revenue"))

## Add rankings
ranked_products_df = product_revenue_df \
    .withColumn("revenue_rank", rank().over(window_by_revenue))

## Display the rankings
display(ranked_products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
