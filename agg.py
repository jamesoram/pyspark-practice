from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

spark = SparkSession.builder.appName("agg").getOrCreate()
df = spark.read.option("header", "true").csv("employees.csv")

# show the average salary per department
#avg_salary = df.groupBy("department").avg("salary").show()
df.groupBy("department").agg(
    avg("salary").alias("avg_salary"),
    count("name").alias("count")
).show()

spark.stop()
