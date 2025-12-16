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


# Write a sql query to print the student details who scored > 60% in each subject
# and overall 70% marks
# nate:- maximum possible marks per subject is 50

from pyspark.sql.functions import *

data = [
    ("A1", "Maths", 31),
    ("A1", "Physics", 39),
    ("A1", "Chemistry", 28),
    ("B1", "Maths", 32),
    ("B1", "Physics", 34),
    ("B1", "Chemistry", 29),
    ("C1", "Maths", 31),
    ("C1", "Physics", 36),
    ("C1", "Chemistry", 32),
    ("D1", "Maths", 35),
    ("D1", "Physics", 38),
    ("D1", "Chemistry", 33)
]

columns = ["StudentName", "Subject", "Marks"]

df = spark.createDataFrame(data, columns)
df.show()

df.createOrReplaceTempView('student')

print("======================================With CTE============================================")

result2 = spark.sql(
    """
    with marks as(
        select 
            *,
            50 AS max_marks,
            round((marks/50)*100,0) as percentage
        from student
    ),
    marks1 as (
    select *,
        count(*)over(partition by StudentName order by StudentName) as cnt
    from marks
    where percentage > 60 
    )
    select StudentName, (sum(Marks)/150.0)*100 as overall_percentage
    from marks1 where cnt > 2
    group by StudentName
    having (sum(Marks)/150.0)*100 > 70
    """
)

result2.show()


print("======================================With CTE 2============================================")


result3 = spark.sql(
    """
    with marks3 as(
        select 
            StudentName,
            Subject,
            Marks,
            50 as max_marks,
            round((Marks/50)*100,0) as percentage,
        from student
    ), 
    marks4 as (
        select *,
            count(*)over(partition by StudentName order by StudentName) as cnt
            from marks3
        
    )
    select * from marks4
    """
)

result3.show()





# print("========================================pysaprk version============================================")
#
# result3 = (
#     df.withColumn('start_point', when(col('start_location') < col('end_location'), col('start_location') ).otherwise(col('end_location')) )
#     .withColumn('end_point', when(col('start_location') < col('end_location'), col('end_location') ).otherwise(col('start_location')) )
# )
#
# result3.show()
#
#
# result3 = (
#     result3.groupby(col('start_point'), col('end_point'))
#     .agg(
#         max('distance').alias('distance')
#     )
# )
#
# result3.show()
#




