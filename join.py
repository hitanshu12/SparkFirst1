
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

data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]

cust = spark.createDataFrame(data4, ["id", "name"])
cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])
prod.show()

secondidlist = prod.select("id").rdd.flatMap(lambda x : x).collect()
print(secondidlist)

finaldf = cust.filter(~col("id").isin(secondidlist))
finaldf.show()

antijoin = cust.join(prod, ["id"],"leftanti")
antijoin.show()

crossJoin =  cust.crossJoin(prod)
crossJoin.show()


# Scenario
data = [
    (1, "A"),
    (2, "B"),
    (3, "C"),
    (4, "D")
]

input1 = spark.createDataFrame(data, ["id", "name"])
input1.show()

data1 = [
    (1, "A"),
    (2, "B"),
    (4, "X"),
    (5, "F")
]

input2 = spark.createDataFrame(data1, ["id1", "name1"])
input2.show()

# not common column

cols_df1 = set(input1.columns)
cols_df2 = set(input2.columns)

common_col = list(cols_df1-cols_df2)
print(common_col)

from pyspark.sql.functions import col

# step 1:full Join : only not common columns
print("full Join")
fullJoinDF = (
    input1.alias("a").join(input2.alias("b"), col("a.id") == col("b.id1"), 'full')
    .filter(col("a.name").isNull() | col("b.name1").isNull() | (col("a.name") != col("b.name1")))
)
fullJoinDF.show()

print("My Solution")

fullJoinDF = (
    fullJoinDF
    .withColumn(
        "id",
        # expr("case when id is null then id1 else id end")
        when(col("id").isNull(), col("id1")).otherwise(col("id"))
    )
    .withColumn("comment",
        when(col("name1").isNull(),'new in source')
        .when(col("name").isNull(),'new in target')
        .otherwise("mismatch")
        # expr("""
        #     case
        #         when name1 is null then 'new in source'
        #         when name  is null then  'new  in target'
        #         else 'mismatch'
        #     end
        # """)
    )
    .drop("name", "name1", "id1")
)

fullJoinDF.show()
# =============================  Solution 2 =============================

# Convert RDDs to DataFrames using toDF()
print("Sai Solution")
source_rdd = spark.sparkContext.parallelize([

    (1, "A"),
    (2, "B"),
    (3, "C"),
    (4, "D")

],1)

target_rdd = spark.sparkContext.parallelize([

    (1, "A"),
    (2, "B"),
    (4, "X"),
    (5, "F")

],2)

df1 = source_rdd.toDF(["id", "name"])
df2 = target_rdd.toDF(["id", "name1"])
df1.show()
df2.show()

joindf = df1.join(df2, ["id"] , "full")
joindf.show()

commdf = joindf.withColumn("comment" ,expr("case when name = name1 then 1  else 0 end"))
commdf.show()

fildf = commdf.filter("comment !=1 ")
fildf.show()

procdf = fildf.withColumn( "comment" , expr("""

                                                    case
                                                    when name1 is null then 'new in source'                                
                                                    when name  is null then  'new  in target'
                                                    else
                                                    'mismatch'
                                                    end
                                                
                                                """))


procdf.show()

finaldf = procdf.drop("name","name1")
finaldf.show()

