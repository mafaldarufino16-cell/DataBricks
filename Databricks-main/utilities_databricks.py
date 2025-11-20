# Import libraries
import re
import fnmatch
import glob
import os
from datetime import datetime, timezone
import json


# Spark Modules
from delta.tables import *
from pyspark.sql import Window, DataFrame, Row 
from pyspark.sql.types import *
from pyspark.sql.functions import * 
from databricks.sdk.runtime import *
from pyspark.sql.utils import AnalysisException

def read_parquet_with_metadata_filtered(path, pDateFrom, pDateTo):
    """
    Reads Parquet files from the specified path and selects all columns along with the file modification time and file name.
    Data only between the specified date range are considered.

    Parameters:
    path (str): The path to the Parquet files.

    Returns:
    DataFrame: A Spark DataFrame with all columns and the file modification time.
    """
    return (spark.read
            .option("ignoreCorruptFiles", "true")
            .format("parquet")
            .load(path)
            .select("*", "_metadata.file_modification_time", "_metadata.file_name")
            .filter(col('file_modification_time').between(pDateFrom, pDateTo)))