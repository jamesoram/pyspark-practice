from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("JoinQueries").getOrCreate()

employees = spark.read.option("header", "true").csv("employees.csv")
departments = spark.read.option("header", "true").csv("departments.csv")

# Assume department name is matching and join on it
joined_df = employees.join(departments, on="department", how="inner")
joined_df.show()

# show only name, salary and location
joined_df.select("name", "salary", "location").show()

spark.stop()
