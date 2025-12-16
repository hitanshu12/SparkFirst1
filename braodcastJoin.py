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

# broadcast Join

# always broadcast  the small table

broadcastJoin = (
    cust.join(broadcast(prod), ["id"], "full")
)
broadcastJoin.show()

data = ['A','b','g','d','&','@','₹','%','1','2','3','4']
df = spark.createDataFrame(data, "string").toDF("char")


df.show()

finaldf = df.withColumn(
    "category",
    expr("""
        case
            when char rlike '^[0-9]$' THEN 'number'
            when char rlike '^[A-Za-z]$' THEN 'alphabet'
            else 'Special Character'
        end
    """)
)

finaldf.show()

# if you want count
finaldf = df.withColumn(
    "category",
    expr("""
        case
            when char rlike '^[0-9]$' THEN 'number'
            when char rlike '^[A-Za-z]$' THEN 'alphabet'
            else 'Special Character'
        end
    """)
).groupby("category").count()

finaldf.show()

# Evenk Scenario

data = [
    (1, "Ram-Kumar_Das"),
    (2, "Raj-Kumar_Sahu"),
    (3, "Hari-Hara_Mishra")
]

df = spark.createDataFrame(data, ["Sl_No", "Full_Name"])

df.show()

# middle name
middleDF = df.withColumn(
    "Middle_Name",
    split(split(col("Full_Name"),"-")[1],"_")[0]
).drop("Full_Name")

middleDF.show()


# Scenario 2

data = [
    (1, "Ram", "01-01-1998"),
    (2, "Raj", "01-01-1998"),
    (3, "Hari", "01-01-1998"),
    (1, "Ram", "01-01-2026")
]

columns = ["Sl_No", "First_Name", "Change_Date"]

df2 = spark.createDataFrame(data, columns)

df2.show()

df2 = (
    df2.groupby("First_Name").agg(
        max("Change_Date").alias("Change_Date")
    )
)

df2.show()


# Write a spark program to drop only those columns which contains more than 80% of null values from a dataframe.

from pyspark.sql import functions as F

# Sample DataFrame
df = spark.createDataFrame(
    [
        (1, None, None, 10),
        (2, None, None, 20),
        (3, None, None, None),
        (4, None, None, 40),
        (5, "A", None, 50)
    ],
    ["id", "col1", "col2", "col3"]
)

# Threshold
THRESHOLD = 0.8

# Total row count
total_rows = df.count()

# Identify columns to drop
cols_to_drop = []

for col in df.columns:
    null_count = df.filter(F.col(col).isNull()).count()
    if null_count / total_rows > THRESHOLD:
        cols_to_drop.append(col)

# Drop columns
df_cleaned = df.drop(*cols_to_drop)

df_cleaned.show()
