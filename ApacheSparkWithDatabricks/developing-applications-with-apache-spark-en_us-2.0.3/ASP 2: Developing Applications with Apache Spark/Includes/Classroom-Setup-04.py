# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

# setattr(DA, 'paths.storage_location', f'{DA.paths.working_dir}/storage_location')

# COMMAND ----------

from pyspark.sql.functions import *
import matplotlib.pyplot as plt

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import json

# Define the CSV-like data with JSON strings as a list of tuples
data = [
    (101, "Alice Smith", "true", "2023-01-15",
     """["hiking", "machine learning", "photography"]""",
     """[{"product_id": "P123", "name": "Laptop", "price": 1299.99, "date": "2023-03-10"}, {"product_id": "P456", "name": "External Monitor", "price": 249.99, "date": "2023-03-15"}]"""),
    
    (102, "Bob Johnson", "true", "2023-02-20",
     """["coding", "gaming", "reading"]""",
     """[{"product_id": "P789", "name": "Mechanical Keyboard", "price": 149.99, "date": "2023-04-05"}]"""),
    
    (103, "Charlie Williams", "false", "2022-11-05",
     """["golf", "cooking", "traveling"]""",
     """[]"""),
    
    (104, "Diana Garcia", "true", "2023-03-10",
     """["drawing", "yoga", "music"]""",
     """[{"product_id": "P234", "name": "Graphics Tablet", "price": 199.99, "date": "2023-03-25"}, {"product_id": "P567", "name": "Stylus Pen", "price": 49.99, "date": "2023-03-25"}, {"product_id": "P890", "name": "Design Software", "price": 299.99, "date": "2023-04-02"}]"""),
    
    (105, "Ethan Davis", "true", "2022-09-15",
     """["basketball", "programming", "movies"]""",
     """[{"product_id": "P321", "name": "Textbook", "price": 89.99, "date": "2023-01-10"}, {"product_id": "P654", "name": "Backpack", "price": 59.99, "date": "2023-01-10"}]"""),
    
    (106, "Fiona Miller", "true", "2023-04-01",
     """["social media", "writing", "photography"]""",
     """[{"product_id": "P987", "name": "Camera", "price": 599.99, "date": "2023-04-15"}]"""),
    
    (107, "George Wilson", "false", "2022-12-10",
     """["finance", "cycling", "chess"]""",
     """[{"product_id": "P111", "name": "Financial Software", "price": 199.99, "date": "2023-01-05"}, {"product_id": "P222", "name": "Wireless Mouse", "price": 29.99, "date": "2023-02-15"}]"""),
    
    (108, "Hannah Brown", "true", "2023-02-28",
     """["education", "reading", "gardening"]""",
     """[{"product_id": "P333", "name": "Educational Subscription", "price": 14.99, "date": "2023-03-01"}, {"product_id": "P444", "name": "Notebook Set", "price": 24.99, "date": "2023-03-01"}]"""),
    
    (109, "Ian Taylor", "true", "2022-10-20",
     """["cooking", "food", "travel"]""",
     """[{"product_id": "P555", "name": "Cooking Knives", "price": 179.99, "date": "2023-01-25"}, {"product_id": "P666", "name": "Recipe Book", "price": 39.99, "date": "2023-02-10"}, {"product_id": "P777", "name": "Spice Set", "price": 49.99, "date": "2023-03-20"}]"""),
    
    (110, "Julia Martinez", "true", "2023-03-15",
     """["law", "politics", "hiking"]""",
     """[{"product_id": "P888", "name": "Legal Reference Book", "price": 129.99, "date": "2023-04-10"}]""")
]

# Define the schema for the raw data
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("active", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("interests", StringType(), True),
    StructField("recent_purchases", StringType(), True)
])

# Create DataFrame directly
df_raw = spark.createDataFrame(data, schema)

# Create Temp View
df_raw.createOrReplaceTempView("raw_user_data")

# COMMAND ----------


