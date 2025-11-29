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
print("==== File Processing ====")
print()

rawlist = ["bigdata~spark~hadoop~hive"]
rddList = sc.parallelize(rawlist)
rddList.foreach(print)
print()

# FlatMAp

print("==== Flatmap ====")
flatRddList = rddList.flatMap(lambda x: x.split("~"))
flatRddList.foreach(print)
print()

print("==== Covert to Upper case ====")
# Uppercase
upperCaseRddList = flatRddList.map(lambda x: x.upper())
upperCaseRddList.foreach(print)
print()

print("==== Add Prefix and Suffix ====")
finalRdd = upperCaseRddList.map(lambda x: "Tech -> " + x + " TRAINER -> SAI")
finalRdd.foreach(print)
print("==== Usecase Done ====")
print()
# OR
print("Solve Usecase in one line code")
print()
finalRdd = rddList.flatMap(lambda x: x.split("~")).map(lambda x: x.upper()).map(lambda x: "Tech -> " + x + " TRAINER -> SAI").foreach(print)



"""
read dt.txt file
iterate each row and filter the fifth column contains gymnastics

sol : -
S1:- Read The file
S2:- Map Split with the delimiter
S3:- Define the column name
S4:- Assign the defined column to the each split
S5:- Apply column filter on required column


"""
print("=============Read DT Data=========")
dtDf = sc.textFile("dt.txt")
dtDf.foreach(print)
print()
print("=============Map Split with the delimiter=========")
splitDtDF = dtDf.map(lambda x: x.split(","))
splitDtDF.foreach(print)
print()
print("=============Define the column name=========")
columns = ["s.no","date","amount","category","membershipType","mode"]
print()
print("=============Assign the defined column to the each split=========")
newDtDF = splitDtDF.toDF(columns)
newDtDF.show()
print()
print("=============Apply column filter on required column=========")
from pyspark.sql.functions import col
fltrDF = newDtDF.filter(col("membershipType").contains("Gymnastics"))
fltrDF.show()

## or

from collections import namedtuple

columnsName = namedtuple('columnsName', ["id","date","amount","category","product","mode"])

print("namedtuple columns assigned")

assignedColDf = splitDtDF.map(lambda x: columnsName(x[0],x[1],x[2],x[3],x[4],x[5]))
assignedColDf.foreach(print)
print()
print("=============Apply column filter on required column=========")
finalProdDF = assignedColDf.filter(lambda x: "Gymnastics" in x.product)
finalProdDF.foreach(print)
print()

# covert rdd to dataframe
print("======covert rdd to dataframe======")
gymDF = finalProdDF.toDF()
gymDF.show()
print()

# write Data into parque file format
# print("======write Data into parque file format======")
# gymDF.write.parquet("file:///C:/zeyobron/parqueFile")
# print()
# print("======write Data into parque file format Done======")
