import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)


# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r'C:\Users\Hitanshu Verma\.jdks\corretto-1.8.0_452'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.config("spark.jars.packages",
                                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                                    "com.amazonaws:aws-java-sdk-bundle:1.12.262").getOrCreate()

spark._jsc.hadoopConfiguration().set(
    "fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
)

#spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

# Read Incremental Data from S3
#
# s3_df = spark.read.format('json').option("multiline","true").load("s3a://sales-analytics-hit-pipe/json/sales.json")\
#     .filter("order_date >= '2024-12-10'")
#
# s3_df.printSchema()
# s3_df.show(truncate=False)

# load data from Snowflake
# snowfalke configuration

snowfalke_options = {
    "sfURL": "https://ey81669.ap-southeast-1.snowflakecomputing.com",
    "sfDatabase": "SALES_ANALYTICS",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfUser": "hitaverm",
    "sfPassword": "hiTAV@cg12&1234",
    "sfRole": "ACCOUNTADMIN"
}

# read data
snow_df = (
    spark.read.format("snowflake")
    .options(**snowfalke_options)
    .option("query", """
                select * from SALES_ANALYTICS.PUBLIC.CUSTOMER_MASTER
                where UPDATED_AT > '2024-12-01'
             """)
    .load()
)

snow_df.printSchema()
snow_df.display()
