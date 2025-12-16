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


# TCS Scenario


data3 = [
    ("a", 25),
    ("b", 20),
    ("c", 30),
    ("d", 27),
    ("e", 35),
]

schema = ["name", "age"]

tcsDF = spark.createDataFrame(data3, schema)
tcsDF.show()

new_age_df = (
    tcsDF
    .filter("age > 25")
    .withColumn(
        "new_age", col("age")
    )
    .drop("age")
)
new_age_df.show()

# Employee second-highest salary without window function

print("Employee second-highest salary without window function")

data = [
    (1, "Ankit", 100, 25000, 4, 39, "1986-09-19"),
    (2, "Mohit", 100, 15000, 5, 48, "1977-09-19"),
    (3, "Vikas", 100, 10000, 4, 37, "1988-09-19"),
    (4, "Rohit", 100, 5000, 2, 16, "2009-09-19"),
    (5, "Mudit", 200, 12000, 6, 55, "1970-09-19"),
    (6, "Agam", 200, 12000, 2, 14, "2011-09-19"),
    (7, "Sanjay", 200, 9000, 2, 13, "2012-09-19"),
    (8, "Ashish", 200, 5000, 2, 12, "2013-09-19"),
    (9, "Mukesh", 300, 6000, 6, 51, "1974-09-19"),
    (10, "Rakesh", 500, 7000, 6, 50, "1975-09-19")
]

schema = ["id", "name", "dept_id", "salary", "experience", "age", "dob"]

df = spark.createDataFrame(data, schema) \
    .withColumn("dob", to_date("dob", "yyyy-MM-dd"))

df.show(truncate=False)
df.printSchema()

print("inner query")
# inner_query = (
#     df.select("salary")
#     .groupby("salary")
#     .agg(
#         count("salary").alias("cnt")
#     )
#     .filter("cnt == 1")
#     .drop("cnt")
#     .orderBy(df.salary.desc())
#     .limit(5)
# )
# Step 1: Get top 5 distinct salaries
inner_query = (
    df.select("salary")
    .distinct()
    .orderBy(df.salary.desc())
    .limit(5)
)

inner_query.show()

# Step 2: Filter employees with those salaries
filtered_df = df.join(inner_query, on="salary", how="inner")
filtered_df.show()

# Step 3: Order by salary ascending and pick top 1
result = filtered_df.orderBy("salary").limit(1)
result.show()

# Window function
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import Window

data = [

    ("DEPT1", 1000),
    ("DEPT1", 500),
    ("DEPT1", 700),
    ("DEPT2", 400),
    ("DEPT2", 200),
    ("DEPT3", 200),
    ("DEPT3", 500)
]

columns = ["department", "salary"]
df = spark.createDataFrame(data, columns)
df.show()

# first - create window - later apply in dataframe
deptwindow = Window.partitionBy(  "department"  ).orderBy(  col("salary").desc()  )

denserankdf  = df.withColumn("rnk" , dense_rank().over(deptwindow))
denserankdf.show()

filrnk = denserankdf.filter(" rnk  = 2 ")
filrnk.show()

finaldf = filrnk.drop("rnk")
finaldf.show()

print("=================SubQuery=======================")

# subquery
dept_salaries = df.select("department", "salary").distinct()
dept_salaries.show()

# Step 2: Order salaries descending per department
dept_salaries_desc = dept_salaries.orderBy("department", F.desc("salary"))
dept_salaries_desc.show()

# Step 3: Collect salaries per department as a list
dept_salary_list = dept_salaries_desc.groupBy("department") \
    .agg(F.collect_list("salary").alias("salary_list"))
dept_salary_list.show()


# Step 4: Get the second highest salary from the list
second_highest_dept_salary = dept_salary_list.select(
    "department",
    F.expr("salary_list[1]").alias("second_highest_salary")  # index 1 → second element
)

second_highest_dept_salary.show()

print("=================SubQuery End=======================")
print()
print("=================SubQuery: Solution 2=======================")

# get the max salary
max_salary = (
     df.groupby("department")
     .agg(
         max("salary").alias("max_salary")
     )
 )
max_salary.show()

# join department wise and filter with max_salary

filter_df = (
    df.join(max_salary, ["department"], "inner")
    .filter(col("salary") < col("max_salary"))
    .drop("max_salary")
)

filter_df.show()

# now take the max salary
second_highest_salary = (
    filter_df.groupby("department")
    .agg(
        max("salary").alias("second_highest_salary")
    )
)

second_highest_salary.show()


















