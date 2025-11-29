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

# Read the US date File

usdata = sc.textFile("usdata.csv")
print()
print("============= Raw Data ===============")
usdata.foreach(print)
print()

# Filter row Length greater than 200

print("====================lEN ===============")
lengthData = usdata.filter( lambda x : len(x) > 200 )
lengthData.foreach(print)
print()

#  Split with , delimiter

print("====================Split with comma Delimiter===============")
splitData = lengthData.flatMap( lambda x : x.split(",") )
splitData.foreach(print)
print()

# Once Flattened/Separate Remove Hyphens if we have any
print("====================Remove Hyphens===============")
rmHypenData = splitData.map( lambda x : x.replace("-","") )
rmHypenData.foreach(print)
print()


# Concatinate , Zeyo

print("====================Add Zeyo===============")
addZeyoData = rmHypenData.map( lambda x : x + ", Zeyo" )
addZeyoData.foreach(print)
print()

# Write data

addZeyoData.saveAsTextFile("file:///C:/Users/hit/rddsout")



