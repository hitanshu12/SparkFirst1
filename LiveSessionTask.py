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

spark = SparkSession.builder.getOrCreate()

#spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

data = [
    ("sai",  "chn", 40),
    ("zeyo", "hyd", 10),
    ("sai",  "hyd", 20),
    ("zeyo", "chn", 20),
    ("sai",  "chn", 10),
    ("zeyo", "hyd", 10),
]

df = spark.createDataFrame(data, ["name", "city", "amount"])

df.show()


aggDf = (
    df.groupby("name")
    .agg(
        sum("amount").alias("total"),
        count("amount").alias("count"),
        collect_list("amount").alias("amt_list"),
        collect_set("amount").alias("amt_set")
    )
)
aggDf.show()

# Multi Column group by

grpDF = (
    df.groupby("name","city")
    .agg(
        sum("amount").alias("total"),
        count("amount").alias("count"),
        collect_list("amount").alias("amt_list"),
        collect_set("amount").alias("amt_set")
    )
)
grpDF.show()


# scenario -2

data2 = [("India",), ("England",), ("Australia",)]

df2 = spark.createDataFrame(data=data2, schema=["country"])
df2.show()

copyDF = df2.selectExpr("country as cntry")
copyDF.show()

cross_join = (
    df2.crossJoin(copyDF)
    .filter(
        (col("country") != col("cntry")) &
        (col("country") < col("cntry"))
    )
    .withColumn("matches",
        concat_ws(" vs ", "country", "cntry")
    )
    # .drop("country", "cntry")
)
cross_join.show()


# Scenario 3

data = [('2020-05-30','Headphone'),('2020-06-01','Pencil'),('2020-06-02','Mask'),('2020-05-30','Basketball'),('2020-06-01','Book'),('2020-06-02','Mask'),('2020-05-30','T-Shirt')]
columns = ["sell_date",'product']

df = spark.createDataFrame(data,schema=columns)
df.show()


selldateDF = (

    df.groupby("sell_date")
    .agg(
        collect_set("product").alias("products"),
        count("product").alias("null_sell")
    )
    .where(col("null_sell") > 2)

)

selldateDF.show(truncate=False)


























