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


