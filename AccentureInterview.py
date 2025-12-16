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


# Write a sql query to return only unique route destination

from pyspark.sql.functions import *

data = [
    ("Delhi", "Pune", 1400),
    ("Pune", "Delhi", 1400),
    ("Bangalore", "Chennai", 350),
    ("Mumbai", "Ahmedabad", 500),
    ("Chennai", "Bangalore", 350),
    ("Patna", "Ranchi", 300)
]

columns = ["start_location", "end_location", "distance"]

df = spark.createDataFrame(data, columns)
df.show()

df.createOrReplaceTempView('table')

print("======================================Without CTE============================================")

result2 = spark.sql(
    """
        select 
            case when start_location < end_location then start_location else end_location end as start_point,
            case when start_location < end_location then end_location else start_location end as end_point,
            max(distance) as distance
        from table
        group by
        case when start_location < end_location then start_location else end_location end,
        case when start_location < end_location then end_location else start_location end
    """
)

result2.show()

print("========================================With CTE=================================================")

result = spark.sql(
    """
    with travel_distance as(
        select *, 
            case when start_location < end_location then start_location else end_location end as start_point,
            case when start_location < end_location then end_location else start_location end as end_point
        from table
    )
    select distinct start_point as start_location, end_point as end_location, distance
    from travel_distance
    """
)

result.show()

print("========================================pysaprk version============================================")

result3 = (
    df.withColumn('start_point', when(col('start_location') < col('end_location'), col('start_location') ).otherwise(col('end_location')) )
    .withColumn('end_point', when(col('start_location') < col('end_location'), col('end_location') ).otherwise(col('start_location')) )
)

result3.show()


result3 = (
    result3.groupby(col('start_point'), col('end_point'))
    .agg(
        max('distance').alias('distance')
    )
)

result3.show()



print("===========================Python Code===============================")

Source = [[1,2,(3,4),5,[6,7,8],9]]

ls = []

for i in Source:
    if isinstance(i,  (list, tuple)):
        ls.extend(i)
    else:
        ls.append(i)

print(ls)

# Count the occurance

from collections import Counter

# data = ['a', 'b', 'c', 'a', 'b', 'a']
data = "qasssadec"
count = Counter(data)

print(count)


# remove duplicate

print("remove duplicates")

arr = [1, 2, 2, 3, 3, 4]
def remove_duplicate(n):
    ls = []
    for i in n:
        if i not in ls:
            ls.append(i)
        else:
            pass

    return ls

print(remove_duplicate(arr))

# Read data from 5 line

df = spark.createDataFrame([("A",), ("B",), ("C",)], ["letter"])

df.show()

# Convert to RDD, zip, map to (index, letter), create new DataFrame
rdd = spark.sparkContext.textFile(r"C:/Users/Hitanshu Verma/IdeaProjects/SparkFirst1/data/movies.csv")
rdd_from_5th  = (
    rdd.zipWithIndex()
    .filter(lambda x: x[1] >= 4)
    .map(lambda x: x[0])
)
new_df = spark.read.csv(rdd_from_5th, header=False, inferSchema=True)
new_df.show()






