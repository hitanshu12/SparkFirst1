import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt":os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe":os.path.join(hadoop_home, "bin", "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll":os.path.join(hadoop_home, "bin", "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://raw.githubusercontent.com/saiadityaus1/test1/refs/heads/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/refs/heads/master/rm.json": "rm.json"}.items()]

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

print("=========== Zeyo Start=========")

a = 2
print(a)

b = 3
print(b)

c = a + 2
print(c)

d = "zeyobron"
print(d)

e = d + " analytics"
print(e)


# list iteration

lis = [1,2,3,4,5,6]
print(lis)
# Process the list using pySpark
# First convert the list into RDD or Dataframe

print("=========== RDD List=========")
rddlis = sc.parallelize(lis)  # convert List to RDD
print(rddlis.collect())

print("=========== Add Rdd List =========")
addRdd = rddlis.map(lambda x : x + 2)
print(addRdd.collect())

print("=========== Multiply RDD List =========")
mulrdd = rddlis.map(lambda x : x * 10)
print(mulrdd.collect())


print("=========== Divide RDD List =========")
divRdd = rddlis.map(lambda x : x / 5)
print(divRdd.collect())


print("=========== Filter Example =========")

print()

filin  = rddlis.filter( lambda x : x > 2 )
print(filin.collect())






print("====Raw String=====")

lisstr  = ["zeyobron", "zeyo", "tera"]
print(lisstr)

# Rdd String
print("====rdd String=====")
rddStr = sc.parallelize(lisstr)
print(rddStr.collect())

# iterate analytics to every element

print("====add rdd String=====")
addStr = rddStr.map( lambda x : x + " analytics")
print(addStr.collect())

# remove zeyo from the rdd
print("====remove zeyo =====")
rmStr = rddStr.map( lambda x : x.replace("zeyo", "") )
print(rmStr.collect())

# filter every element contains zeyo
print("====filter zeyo String=====")
filZeyo = rddStr.filter(lambda x : "zeyo" in x)
print(filZeyo.collect())



print("==== Raw Str ====")
rawlist = [ "A~B" , "C~D" ]
print(rawlist)
print()

print("==== Rdd Str ====")
rddList = sc.parallelize(rawlist)
print(rddList.collect())
print()
# iterate every element of the  list and flatten/ separate every with "~" delimiter
print("==== flatmap Str ====")
flatRdds = rddList.flatMap(lambda x : x.split("~"))
print(flatRdds.collect())

################################################################### Usecase Pyspark Start ####################################################################################

rawList = ["State -> TN ~ City -> Chennai" , "State -> Kerala ~ City -> Trivendum"]

# requirement need different DF for State and City

print("==== Raw Str ====")
print(rawList)
print()

print("==== Rdd Str ====")
rddList = sc.parallelize(rawList)
print(rddList.collect())
print()


# Flat map

print("==== Flat Rdd Str ====")
fmrdd = rddList.flatMap(lambda x : x.split("~"))
print(fmrdd.collect())
print()

# Filter State

print("==== filter Rdd Str ====")
filState = fmrdd.filter(lambda x : "State" in x)
print(filState.collect())
print()


# Filter City

print("==== filter city Rdd Str ====")
filcity = fmrdd.filter(lambda x : "City" in x)
print(filcity.collect())
print()

# replace state ->
print("==== rplc state Str ====")
rplcRdd = filState.map(lambda x : x.replace("State -> ",""))
print(rplcRdd.collect())
print()

# replace City ->
print("==== rplc city Str ====")
cityRdd = filcity.map(lambda x : x.replace("City -> ",""))
print(cityRdd.collect())
print()


#################################################################### Usecase Pyspark End ################################################################################


rawList = ["State -> TN ~ City -> Chennai" , "State -> Kerala ~ City -> Trivendum"]

# requirement need different DF for State and City

print("==== Raw Str ====")
print(rawList)
print()

print("==== Rdd Str ====")
rddList = sc.parallelize(rawList)
rddList.foreach(print)
print()


# Flat map

print("==== Flat Rdd Str ====")
fmrdd = rddList.flatMap(lambda x : x.split("~"))
fmrdd.foreach(print)
print()

# Filter State

print("==== filter Rdd Str ====")
filState = fmrdd.filter(lambda x : "State" in x)
filState.foreach(print)
print()


# Filter City

print("==== filter city Rdd Str ====")
filcity = fmrdd.filter(lambda x : "City" in x)
filcity.foreach(print)
print()

# replace state ->
print("==== rplc state Str ====")
rplcRdd = filState.map(lambda x : x.replace("State -> ",""))
rplcRdd.foreach(print)
print()

# replace City ->
print("==== rplc city Str ====")
cityRdd = filcity.map(lambda x : x.replace("City -> ",""))
cityRdd.foreach(print)
print()





# file Processing
print("==== File Processing ====")
print()
stateFile = sc.textFile("state.txt")
stateFile.foreach(print)



