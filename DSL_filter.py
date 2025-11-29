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

print("===================Read SQl Table==================")
print()
# readSql = (spark.read
#            .format("jdbc")
#            .option("url", "jdbc:mysql://localhost:3306;databaseName=NamasteSQL")
#            .option("driver", "com.mysql.cj.jdbc.Driver")
#            .option("dbtable", "dbo.employees")
#            .option("user", "root")
#            .option("password", "")
#            .load()
#            )
#
# readSql.show()


data = [
    ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00000005", "02-14-2011", 200, "Gymnastics", None , "cash")
]

# Using toDF() to create DataFrame
df = spark.createDataFrame(data).toDF(
    "txnno", "txndate", "amount", "category", "subcategory", "spendmode"
)

df.show()

### SELECT columns
print("===================Select==================")
print()
selDF = df.select("txndate", "amount")

selDF.show()

### DROP columns
print("===================Drop==================")
print()
dropDf = df.drop("txndate", "amount")

dropDf.show()

print("===================Filter: category = 'Exercise'==================")
print()

filCol = df.filter("category = 'Exercise'")
filCol.show()

print("===================Filter: category = 'Exercise' and spendmode = 'cash'==================")
print()

filMultpleCol = df.filter("category = 'Exercise' and spendmode = 'cash'")
filMultpleCol.show()

print("===================Filter: category = 'Exercise' or spendmode = 'cash'==================")
print()

filMultpleorCol = df.filter("category = 'Exercise' or spendmode = 'cash'")
filMultpleorCol.show()

print("===================Filter: category in 'Exercise' and 'Gymnastics'==================")
print()

inFilDf = df.filter("category in ('Exercise','Gymnastics')")
inFilDf.show()

print("===================Filter: category not in 'Exercise' and 'Gymnastics'==================")
print()

NotinFilDf = df.filter("category not in ('Exercise','Gymnastics')")
NotinFilDf.show()

print("===================Sub Category Contains Gymnastics==================")
print()

likeFil = df.filter("subcategory like '%Gymnastics%'")
likeFil.show()

print("===================Sub Category is null ==================")
print()

isNullDF = df.filter("subcategory is null")
isNullDF.show()

print("===================Sub Category is not null ==================")
print()

isNotNullDF = df.filter("subcategory is not null")
isNotNullDF.show()

print("===================Filter: category not equal to 'Exercise'==================")
print()

notFil = df.filter("category != 'Exercise'")
notFil.show()






