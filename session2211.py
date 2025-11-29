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

# file Processing
print("==== data ====")
print()
data = sc.textFile("dt.txt")
data.foreach(print)
print()

print("=== Filter the data that rows contains gymnastics")

flterRows = data.filter(lambda x: "Gymnastics" in x)
flterRows.foreach(print)
print()

"""
Iterate every row and print column contain gymnastics
"""
print("split the data with comma")
print()
splitRdd = data.map(lambda x: x.split(','))
splitRdd.foreach(print)
print()

print("define the column name")
print()
from collections import namedtuple
columns = namedtuple('columns', ['id','tdate','amount','category','product','mode'])
print()

print("assigned the column name to rdd")
print()

assignClmn = splitRdd.map(lambda x: columns(x[0], x[1], x[2], x[3], x[4], x[5]))
assignClmn.foreach(print)
print()


print("Filter the gym data row wise")
print()

finalRdd = assignClmn.filter(lambda x: "Gymnastics" in x.product)
finalRdd.foreach(print)
print()





