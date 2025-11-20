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
# MAGIC ## Monitoring and Optimizing Apache Spark Workload on Databricks
# MAGIC
# MAGIC This course explores the Lakehouse architecture and Medallion design for scalable data workflows, focusing on Unity Catalog for
# MAGIC secure data governance, access control, and lineage tracking. The curriculum includes building reliable, ACID-compliant pipelines
# MAGIC with Delta Lake. 
# MAGIC
# MAGIC You'll examine Spark optimization techniques, such as partitioning, caching, and query tuning, and learn
# MAGIC performance monitoring, troubleshooting, and best practices for efficient data engineering and analytics to address real-world
# MAGIC challenges.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Prerequisites
# MAGIC You should meet the following prerequisites before starting this course:
# MAGIC
# MAGIC - Basic programming knowledge
# MAGIC - Familiarity with Python
# MAGIC - Basic understanding of SQL queries (`SELECT`, `JOIN`, `GROUP BY`)
# MAGIC - Familiarity with data processing concepts
# MAGIC - No prior Spark or Databricks experience required
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Course Agenda
# MAGIC The following modules are part of the **Data Engineer Learning** Path by Databricks Academy.
# MAGIC | # | Module Name |
# MAGIC | --- | --- |
# MAGIC | 1 | [Introduction to Delta Lake]($./1 - Introduction to Delta Lake) |
# MAGIC | 2L | [Introduction to Delta Lake]($./2L - Introduction to Delta Lake) |
# MAGIC | 3 | [Optimizing Apache Spark]($./3 - Optimizing Apache Spark) |
# MAGIC | 4L | [Optimizing Apache Spark]($./4L - Optimizing Apache Spark)|
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC
# MAGIC - Use Databricks Runtime version: **`16.4.x-scala2.12`** for running all demo and lab notebooks.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
