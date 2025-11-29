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






# SQL GET READY CODE


data = [
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
    (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()

data2 = [
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "02-14-2011", 200.0, "Winter", None, "cash"),
    (7, "02-14-2011", 200.0, "Winter", None, "cash")
]

df1 = spark.createDataFrame(data2, ["id", "tdate", "amount", "category", "product", "spendby"])
df1.show()

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



###################################### create a tempview for sql operation in pyspark ##########################



df.createOrReplaceTempView("df")
df1.createOrReplaceTempView("df1")
cust.createOrReplaceTempView("customer")
prod.createOrReplaceTempView("product")

print("Select two Columns")
spark.sql("select id, tdate from df").show()
print()

print("filter df where category = Exercise")
spark.sql('select * from df where category= "Exercise"').show()
print()

print("filter id, tdate, category, spendby from df where category = Exercise and spendby = cash")
spark.sql('select id, tdate, category, spendby from df where category= "Exercise" and spendby = "cash" ').show()
print()

print("Multi Value Filter:- category = Exercise and Gymnastics")
spark.sql('select * from df where category IN ("Exercise", "Gymnastics") ').show()
print()

print("Product contains gymnastics - Like Filter")
spark.sql('select * from df where product like "%Gymnastics%"').show()
print()

print("Not Filter:- category not equal to exercise")
spark.sql('select * from df where category != "Exercise"').show()
print()

print("Not Filter:- category not equal to exercise and gymnastics")
spark.sql('select * from df where category Not in ("Exercise", "Gymnastics")').show()
print()

print("Filter:- Product having null values")
spark.sql('select * from df where product is null').show()
print()

print("Filter:- Product having not null values")
spark.sql('select * from df where product is not null').show()
print()

print("Max of ID")
spark.sql('select max(id) as max_id from df').show()
print()

print("Mix of ID")
spark.sql('select min(id) as min_id from df').show()
print()

print("Count of rows")
spark.sql('select count(*) as number_of_rows from df').show()
print()


print("add status column in df")
spark.sql('select *, (case when spendby = "cash" then 1 else 0 end) as status from df').show()
print()


print("concat two columns")
spark.sql('select id, category, concat(cast(id as string),"-",category) as cancat_column from df').show()
print()


print("concat multiple columns")
spark.sql('select id, category, product, concat_ws("-",cast(id as string),category,product) as cancat_column from df').show()
print()

print("Lower case:- category")
spark.sql('select category, lower(category) as lower_category from df').show()
print()

print("Upper case:- category")
spark.sql('select category, upper(category) as upper_category from df').show()
print()

print("CEIL round to upper value")
spark.sql('select amount, ceil(amount) as ceil from df').show()
print()

print("Round:- nearest value")
spark.sql('select amount, round(amount, 0) as round from df').show()
print()

print("Replace Null")
spark.sql('select product, coalesce(product, "NA") as product1 from df').show()
print()


print("trim space")
spark.sql('select product, trim(product) as productTrim from df').show()
print()


print("distinct : Unique")
spark.sql('select distinct category from df').show()
print()

print("distinct multiple columns")
spark.sql('select distinct category, spendby from df').show()
print()


print("Substring")
spark.sql('select product, substring(product,1,10) as subStr from df').show()
print()


print("Split")
spark.sql('select product, split(product," ")[0] as split from df').show()
print()

print("Union")
spark.sql('select * from df Union select * from df1').show()
print()

print("Union All")
spark.sql('select * from df Union All select * from df1').show()
print()


print("Aggregation Sum")
spark.sql('select category, Sum(amount) as total_amount from df group by category').show()
print()

print("Aggregation Sum:- category and spendby")
spark.sql('select category, spendby, Sum(amount) as total_amount from df group by category, spendby').show()
print()


print("Aggregation Sum:- category and spendby and Count:- of category and spendby")
spark.sql('select category, spendby, Sum(amount) as total_amount, count(amount) as count from df group by category, spendby').show()
print()

print("Max amount of every category ")
spark.sql('select category, max(amount) as max_amount from df group by category order by category').show()
print()

print("Min amount of every category ")
spark.sql('select category, min(amount) as min_amount from df group by category order by category').show()
print()

print("Window Function: row_number")
spark.sql('select category,amount, row_number()over(partition by category order by amount desc) as row_number from df').show()
print()

print("Window Function: Rank")
spark.sql('select category,amount, rank()over(partition by category order by amount desc) as rank from df').show()
print()

print("Window Function: Dense_Rank")
spark.sql('select category,amount, dense_rank()over(partition by category order by amount desc) as dense_rank from df').show()
print()

print("Window Function: Lead")
spark.sql('select category,amount, lead(amount)over(partition by category order by amount desc) as lead_column from df').show()
print()

print("Window Function: Lag")
spark.sql('select category,amount, lag(amount)over(partition by category order by amount desc) as lag_column from df').show()
print()


print("Having: count the category got repeated")
spark.sql('select category,count(category) as category_count from df group by category having count(category) > 1').show()
print()


print("Inner Join")
spark.sql('select c.*, p.product from customer c inner join product p on c.id = p.id').show()
print()


print("Left Join")
spark.sql('select c.*, p.product from customer c Left join product p on c.id = p.id').show()
print()

print("Right Join")
spark.sql('select c.*, p.product from customer c right join product p on c.id = p.id').show()
print()


print("Full outer Join")
spark.sql('select c.*, p.product from customer c full join product p on c.id = p.id').show()
print()

print("Left Anti Join")
spark.sql('select c.* from customer c left anti join product p on c.id = p.id').show()
print()


print("Left Semi Join")
spark.sql('select c.* from customer c left semi join product p on c.id = p.id').show()
print()



















