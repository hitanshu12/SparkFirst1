import sys
import os
import first

os.environ['JAVA_HOME'] = r'C:\Users\Hitanshu Verma\.jdks\corretto-1.8.0_452'
# Import Spark Context

from pyspark import SparkContext

# Import Spark Session
from pyspark.sql import SparkSession


# Create a spark context object
"""sc = SparkContext(appName="MySparkApplication")
sc"""

# To shut Down the current active spark context

# sc.stop()


# Create a spark session

"""spark = SparkSession.builder \
    .appName("MySparkApplication") \
    .getOrCreate() """

# Get the Spark context from the sparksession

# sc = spark.sparkContext


# Creating a spark session

sparks = SparkSession.builder \
    .appName("MySparkApplication") \
    .getOrCreate()

sparks
# Perform Various operation using spark session

