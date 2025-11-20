# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

# setattr(DA, 'paths.storage_location', f'{DA.paths.working_dir}/storage_location')

# COMMAND ----------

from pyspark.sql.types import StructType


DA = DBAcademyHelper()
DA.init()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import json

# Define our sample e-commerce data with JSON strings
data = [
    (1001, "Jordan Smith", "jordan.smith@email.com", "2022-03-15",
     """["loyal", "premium", "tech-enthusiast"]""",
     """[
         {"order_id": "O8823", "date": "2023-01-05", "total": 799.99, "items": [
           {"product_id": "PHONE-256", "name": "Smartphone XS", "price": 699.99, "quantity": 1},
           {"product_id": "CASE-101", "name": "Phone Case", "price": 29.99, "quantity": 1},
           {"product_id": "CHGR-201", "name": "Fast Charger", "price": 49.99, "quantity": 1}
         ]},
         {"order_id": "O9012", "date": "2023-02-18", "total": 129.95, "items": [
           {"product_id": "HDPHN-110", "name": "Wireless Headphones", "price": 129.95, "quantity": 1}
         ]}
       ]""",
     """["smartphones", "accessories", "audio", "wearables"]"""
    ),
    
    (1002, "Alex Johnson", "alex.j@email.com", "2021-11-20",
     """["new", "standard", "home-office"]""",
     """[
         {"order_id": "O8901", "date": "2023-01-10", "total": 1299.99, "items": [
           {"product_id": "LAPTOP-15", "name": "Ultrabook Pro", "price": 1199.99, "quantity": 1},
           {"product_id": "MOUSE-202", "name": "Ergonomic Mouse", "price": 49.99, "quantity": 1},
           {"product_id": "KYBRD-303", "name": "Mechanical Keyboard", "price": 89.99, "quantity": 1}
         ]}
       ]""",
     """["laptops", "office-equipment", "monitors", "storage"]"""
    ),
    
    (1003, "Taylor Williams", "t.williams@email.com", "2022-08-05",
     """["standard", "gamer"]""",
     """[
         {"order_id": "O9188", "date": "2023-02-01", "total": 2099.97, "items": [
           {"product_id": "GPU-3080", "name": "Graphics Card RTX", "price": 899.99, "quantity": 1},
           {"product_id": "CPU-i9", "name": "Processor i9", "price": 499.99, "quantity": 1},
           {"product_id": "RAM-32GB", "name": "Gaming RAM 32GB", "price": 189.99, "quantity": 2},
           {"product_id": "MBOARD-Z", "name": "Gaming Motherboard", "price": 319.99, "quantity": 1}
         ]}
       ]""",
     """["gaming", "pc-components", "monitors", "accessories"]"""
    ),
    
    (1004, "Morgan Lee", "morgan.lee@email.com", "2022-06-10",
     """["standard", "photography"]""",
     """[
         {"order_id": "O9021", "date": "2023-01-15", "total": 3299.98, "items": [
           {"product_id": "CAM-DSLR", "name": "Professional Camera", "price": 2499.99, "quantity": 1},
           {"product_id": "LENS-50mm", "name": "Prime Lens", "price": 349.99, "quantity": 1},
           {"product_id": "TRIPOD-P", "name": "Premium Tripod", "price": 149.99, "quantity": 1},
           {"product_id": "SDCARD-128", "name": "Memory Card 128GB", "price": 79.99, "quantity": 3}
         ]},
         {"order_id": "O9254", "date": "2023-02-28", "total": 299.98, "items": [
           {"product_id": "BAG-CAM", "name": "Camera Bag", "price": 189.99, "quantity": 1},
           {"product_id": "CLEAN-KIT", "name": "Lens Cleaning Kit", "price": 29.99, "quantity": 1}
         ]}
       ]""",
     """["cameras", "photography", "lenses", "accessories"]"""
    ),
    
    (1005, "Casey Rivera", "casey.r@email.com", "2021-09-30",
     """["premium", "smart-home"]""",
     """[
         {"order_id": "O8765", "date": "2023-01-02", "total": 1029.95, "items": [
           {"product_id": "SMHUB-01", "name": "Smart Home Hub", "price": 249.99, "quantity": 1},
           {"product_id": "SMSPK-02", "name": "Smart Speaker", "price": 179.99, "quantity": 2},
           {"product_id": "SMBLB-03", "name": "Smart Bulbs Pack", "price": 119.99, "quantity": 3},
           {"product_id": "SMSENS-04", "name": "Motion Sensors", "price": 89.99, "quantity": 1}
         ]},
         {"order_id": "O9181", "date": "2023-02-15", "total": 349.98, "items": [
           {"product_id": "SMDLOCK-05", "name": "Smart Door Lock", "price": 249.99, "quantity": 1},
           {"product_id": "SMCAM-06", "name": "Indoor Camera", "price": 99.99, "quantity": 1}
         ]}
       ]""",
     """["smart-home", "security", "automation", "speakers"]"""
    )
]

# Define the schema for the raw data
schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("recent_orders", StringType(), True),
    StructField("browsing_history", StringType(), True)
])

# Create DataFrame
ecommerce_df = spark.createDataFrame(data, schema)

# Create temporary view
ecommerce_df.createOrReplaceTempView("ecommerce_raw")
