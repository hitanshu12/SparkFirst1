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
selDF = df.select(
    "txnno",
    "txndate",
    "amount",
    "category",
    "subcategory",
    "spendmode"
)

selDF.show()

print("===================Select Expression==================")
print()
selExprDF = df.selectExpr(
    "cast(txnno as int) as txnno",
    "txndate",
    "split(txndate,'-')[2] as year",
    "amount + 1000 as amount",
    "upper(category) as category", # processing
    "concat(subcategory,'~zeyo') as subcategory",
    "spendmode",
    "case when spendmode = 'cash' then 1 else 0 end as status",
    "case when spendmode = 'cash' then 1 when spendmode = 'credit' then 2 else 0 end as multiCondition"
)

selExprDF.show()



selExprDF1 = df.selectExpr(
    "cast(txnno as int) as txnno",
    "to_date(txndate, 'dd-mm-yyyy') as txndate",
    "year(to_date(txndate, 'dd-mm-yyyy')) as year",
    "amount + 1000 as amount",
    "upper(category) as category", # processing
    "concat(subcategory,'~zeyo') as subcategory",
    "spendmode",
    "case when spendmode = 'cash' then 1 else 0 end as status",
    "case when spendmode = 'cash' then 1 when spendmode = 'paytm' then 2 else 0 end as multiStatus"
)

selExprDF1.show()
selExprDF1.printSchema()

print("===================With Columns Dataframe==================")
withColDF = (
    df
    .withColumn(
        "category",
        expr("upper('category')")
    )
    .withColumn(
        "amount",
        expr("amount + 1000")
    )
    .withColumn(
        "subcategory",
        expr("concat(subcategory, '~zeyo')")
    )
    .withColumn(
        "txnno",
        expr("cast(txnno as int)")
    )
    .withColumn(
        "txndate",
        expr("to_date(txndate, 'dd-mm-yyyy')")
    )
    .withColumn(
        "status",
        expr("case when spendmode = 'cash' then 1 else 0 end")
    )
    # Rename the txndate
    .withColumnRenamed("txndate","year")
)
withColDF.show()























