# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

# setattr(DA, 'paths.storage_location', f'{DA.paths.working_dir}/storage_location')

# COMMAND ----------

## The code below will create the DA object for every demo and lab and display the user's catalog + schema.
DA = DBAcademyHelper()
DA.init()


DA.display_config_values([
                        ('Course Catalog',DA.catalog_name),
                        ('Your Schema',DA.schema_name)
                    ])

# COMMAND ----------

@DBAcademyHelper.add_method
def insert_records():
  spark.sql('''INSERT INTO delta_orders
  VALUES 
  (999999, 12345, 'P', DECIMAL('1234.56'), DATE('2023-02-15'), '1-URGENT', 'Clerk#000000123', 0, 'New test order using SQL'),
  (999998, 12346, 'O', DECIMAL('5678.90'), DATE('2023-02-16'), '2-HIGH', 'Clerk#000000456', 0, 'Another test order using SQL');
  ''')
