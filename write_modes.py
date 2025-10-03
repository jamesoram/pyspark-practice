from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, avg

spark = SparkSession.builder.appName("WorkingJSON").getOrCreate()
df = spark.read.option("header", "true").csv("employees.csv")

# save as Parquet
df.write.mode("overwrite").parquet("output/employees.parquet")

# csv
df.write.mode("overwrite").option("header", "true").csv("output/employees.csv")

# json
df.write.mode("overwrite").json("output/employees.json")

spark.stop()
