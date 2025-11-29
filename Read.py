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


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

# print("======================================== READ FILE Start==============================================")
#
# sc.textFile("file:///C:/Users/Hitanshu Verma/Documents/zeyobron/42/Pyspark/Data/customers-50.txt",1).foreach(print)
#
# spark.read.csv("file:///C:/Users/Hitanshu Verma/Documents/zeyobron/42/Pyspark/Data/customers-50.csv").show()
#
# print("======================================== READ FILE END==============================================")
#
#
# rdd = sc.textFile(r"file:///C:/Users/Hitanshu Verma/Documents/zeyobron/42/Pyspark/Data/customers-50.txt", 1)
# for line in rdd.take(5):
#     print(line)


csvdf = (

    spark
    .read
    .format("csv")
    .option("header","true")
    .load("usdata.csv")

)

csvdf.show()

csvdf.createOrReplaceTempView("rishab")


spark.sql("select * from rishab where state='LA'").show()

print()
print("Process through DSL")
print()


laData = csvdf.filter("state = 'LA'")
laData.show()





























