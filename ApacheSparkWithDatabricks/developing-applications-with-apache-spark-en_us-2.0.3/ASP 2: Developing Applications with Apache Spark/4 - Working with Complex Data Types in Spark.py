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
# MAGIC # 04 - Working with Complex Data Types in Spark
# MAGIC
# MAGIC This demonstration shows how to effectively work with nested data structures in Spark, including structs, arrays, and maps, using real e-commerce data examples.
# MAGIC
# MAGIC ### Objectives
# MAGIC - Convert JSON string data to Spark SQL native complex types
# MAGIC - Understand and manipulate complex data types (Struct, Array, Map)
# MAGIC - Process nested JSON-like data structures
# MAGIC - Use the pivot and explode functions to reshape datasets as required

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
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course. It will set your default catalog to **dbacademy** and the schema to your specific schema name shown below using the `USE` statements.
# MAGIC
# MAGIC Also, It will create a temp table for you named `raw_user_data`
# MAGIC <br></br>
# MAGIC
# MAGIC ```
# MAGIC USE CATALOG dbacademy;
# MAGIC USE SCHEMA dbacademy.<your unique schema name>;
# MAGIC ```
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04

# COMMAND ----------

# MAGIC %md
# MAGIC #### Querying the newly created table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_user_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Convert from JSON Strings to StructTypes
# MAGIC
# MAGIC Given raw data which includes nested JSON strings (arrays and/or objects), we will convert this data to native `StructTypes` in the DataFrame API.
# MAGIC
# MAGIC #### Why Convert JSON Strings to StructTypes?
# MAGIC
# MAGIC JSON strings in Spark DataFrames come with several inefficiencies:
# MAGIC
# MAGIC 1. **Parsing Overhead**: Every time you query JSON strings, Spark needs to parse them, adding computational overhead
# MAGIC 2. **Memory Inefficiency**: JSON strings store field names repeatedly for every row, wasting memory
# MAGIC 3. **No Type Safety**: JSON strings don't enforce data types, leading to potential errors
# MAGIC 4. **Poor Query Performance**: Spark can't optimize queries on JSON string content as effectively
# MAGIC 5. **Limited Predicate Pushdown**: Filter operations can't leverage columnar storage optimizations
# MAGIC
# MAGIC ### Steps to Convert JSON Strings to StructTypes
# MAGIC
# MAGIC 1. **Infer Schema**: Determine the structure of the JSON data (the `schema_of_json` function can be used for this)
# MAGIC 2. **Apply Schema**: Use `from_json()` to convert strings to structured data
# MAGIC 3. **Validate**: Ensure all data is correctly parsed and types are appropriate
# MAGIC 4. **Optimize**: Once converted, optimize storage/processing if needed
# MAGIC
# MAGIC ### Benefits of StructTypes
# MAGIC
# MAGIC 1. **Columnar Storage**: Efficient storage with Parquet/Delta
# MAGIC 2. **Type Safety**: Schema enforcement prevents data errors
# MAGIC 3. **Query Optimization**: Spark can optimize queries better with typed data
# MAGIC 4. **Predicate Pushdown**: Filters can be pushed down to storage layer
# MAGIC 5. **Better Performance**: Faster queries and reduced memory usage

# COMMAND ----------

from pyspark.sql.functions import *

# Load some data which includes JSON strings
raw_user_data_df = spark.read.table("raw_user_data")

# COMMAND ----------

# Inspect the Data
raw_user_data_df.printSchema()

# COMMAND ----------

# Displaying the Dataframe 
display(raw_user_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. View the data above and find columns **interests** and **recent_purchases**
# MAGIC 2. **interests** is an array column of String elements and **recent_purchases** is column containing objects

# COMMAND ----------

# Interests is an array of strings, using predefined schema
interests_schema = ArrayType(StringType())

# COMMAND ----------

# Let's get the schema for the "recent_purchases" and cast all of the columns from the raw data set into a confirmed structure

# Take a sample of one value of the "recent_purchases" column, bring this back to the Driver
recent_purchases_json = raw_user_data_df.select("recent_purchases").limit(1).collect()[0][0]
print("Raw JSON string:", recent_purchases_json)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Use the **schema_of_json** Function to generate a Schema based upon a sample row of data
# MAGIC
# MAGIC In many cases, especially with multiple nested complex structures, it is easiest to generate a schema based upon a sample of JSON data, we can do this using the schema_of_json Function.  
# MAGIC
# MAGIC From the above dataset, we can see that **interests** is an array column of string elements, **recent_purchases** is an array column which contains objects.

# COMMAND ----------

# Get the schema for the recent_purchases JSON
recent_purchases_schema = schema_of_json(lit(recent_purchases_json))

# COMMAND ----------

# Parse columns with the correct schemas
parsed_users_df = raw_user_data_df.select(
    col("user_id").cast("integer"),
    col("name"),
    col("active").cast("boolean"),
    col("signup_date").cast("date"),
    from_json(col("interests"), interests_schema).alias("interests"),
    from_json(col("recent_purchases"), recent_purchases_schema).alias("recent_purchases")
)

# Examine the schema
parsed_users_df.printSchema()

# COMMAND ----------

# Now look at the data again, You will notice order has been changed for recent_purchases
display(parsed_users_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Working with Arrays
# MAGIC
# MAGIC Let's explore different ways to access and manipulate arrays.

# COMMAND ----------

# Use the array_size method to see the lengths of the array columns in the dataframe
display(
parsed_users_df.select(
    "user_id",
    array_size("interests").alias("number_of_interests"),
    array_size("recent_purchases").alias("number_of_recent_purchases")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. The explode Method
# MAGIC
# MAGIC The `explode` method is used to unnest array elements into records

# COMMAND ----------

# Let's start by simplifying the data by selecting only the columns we need
user_101s_interests_df = parsed_users_df.select("user_id", "interests").filter(parsed_users_df.user_id == 101)
display(user_101s_interests_df)

# COMMAND ----------

# Let's demonstrate explode, note how there are three rows associated with "user_id" 101 (one for each "interests" array element)
display(
    user_101s_interests_df.select(
        "user_id", 
        explode("interests").alias("interest")
    )    
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. The collect_set and collect_list Methods
# MAGIC
# MAGIC The `collect_set` and `collect_list` methods are aggregate functions (which typically operate on grouped data) to create arrays from column values.  
# MAGIC
# MAGIC `collect_list` may include duplicate values, while `collect_set` removes duplicate array elements should they exist.

# COMMAND ----------

# Let's start by creating a new DataFrame with the "interests" column exploded
exploded_df = parsed_users_df.select("user_id", explode("interests").alias("interest"))
display(exploded_df)

# COMMAND ----------

# Let's use `collect_list` to collect all the "interests" values into a list for each "user_id"
user_interests_df = exploded_df.groupBy("user_id").agg(collect_list("interest").alias("interests"))
display(user_interests_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Referencing Struct Fields
# MAGIC
# MAGIC Let's explore how to access fields within a struct (an object with a predefined schema).

# COMMAND ----------

# First let's explode the "recent_purchases" column
exploded_purchases_df = parsed_users_df.select("user_id", explode("recent_purchases").alias("purchase"))
display(exploded_purchases_df)

# COMMAND ----------

# Use the dot notation to access struct fields
recent_purchases_df = exploded_purchases_df.select(
                        "user_id", 
                        col("purchase.date").alias("purchase_date"), 
                        col("purchase.product_id").alias("product_id"), 
                        col("purchase.name").alias("product_name"), 
                        col("purchase.price").alias("purchase_price")
                    )
display(recent_purchases_df)

# COMMAND ----------

# We can also use the getField() method to reference columns inside of structs, here's an example....

field_access_df = exploded_purchases_df.select(
    "user_id",
    col("purchase").getField("date").alias("purchase_date"),
    col("purchase").getField("product_id").alias("product_id"),
    col("purchase").getField("name").alias("product_name"),
    col("purchase").getField("price").alias("price")
)

display(field_access_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Using the pivot Method
# MAGIC
# MAGIC The `pivot` method in Spark allows you to transform row data into columnar format, creating a cross-tabulation. This is particularly useful for feature engineering when analyzing categorical data or when you need to reshape your data for reporting or visualization.

# COMMAND ----------

# Let's pivot the purchase data to show the count of each product purchased by each user
pivot_df = (recent_purchases_df
    .groupBy("user_id")
    .pivot("product_name")
    .agg(count("product_id").alias("quantity_purchased"))
)

# Display the result
display(pivot_df)

# COMMAND ----------

# Replace null values with zeros for better readability
pivot_df_no_nulls = (recent_purchases_df
    .groupBy("user_id")
    .pivot("product_name")
    .agg(count("product_id").alias("quantity_purchased"))
    .fillna(0)
)

# Display the result
display(pivot_df_no_nulls)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Struct Type Operations**:
# MAGIC    - Use dot notation for simple access
# MAGIC    - `getField()` for dynamic column access
# MAGIC    - Maintain schema clarity
# MAGIC
# MAGIC 2. **Array Operations**:
# MAGIC    - Use array functions for manipulation
# MAGIC    - Leverage explode for detailed analysis
# MAGIC    - Consider performance with large arrays
# MAGIC
# MAGIC 3. **Complex Aggregate Functions**:
# MAGIC    - Use the `collect_list` and `collect_set` methods to create arrays from grouped data
# MAGIC    - Use the `pivot` function to transform row values into columns for analysis and reporting
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
