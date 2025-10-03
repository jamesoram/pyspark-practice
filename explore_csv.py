from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ExploreCSV").getOrCreate()

df = spark.read.option("header", "true").csv("employees.csv")

df.show()
df.printSchema()

print("Total rows: ", df.count())

# Show first 3 rows
print("First 3 rows:", df.head(3))

spark.stop()
